package control

import (
	"context"
	"crypto/subtle"
	"embed"
	"encoding/json"
	"errors"
	"github.com/Polign/vector-transfer/accountauth"
	"github.com/Polign/vector-transfer/connector"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

//go:embed web/*
var assets embed.FS

type AccountService interface {
	Authenticate(*http.Request) (accountauth.Principal, error)
	Mount(*http.ServeMux)
	Realm() string
}
type ownerContext struct{}

func Handler(e *Engine, token string) http.Handler { return handler(e, token, nil) }

// AccountHandler uses verified Polign identity exclusively; there is no operator-token fallback.
// Configure this handler before starting workers.
func AccountHandler(e *Engine, accounts AccountService) http.Handler {
	e.AccountRealm = accounts.Realm()
	return handler(e, "", accounts)
}
func requestOwner(r *http.Request) *Owner {
	o, _ := r.Context().Value(ownerContext{}).(*Owner)
	return o
}
func publicJob(j Job) Job {
	j.Cursor = ""
	j.LeaseID = ""
	j.ReportSequence = 0
	j.Owner = nil
	j.IdempotencyKey = ""
	j.SourceFingerprint = ""
	j.SinkFingerprint = ""
	j.SinkResource = ""
	return j
}
func ownedJob(e *Engine, r *http.Request) (Job, error) {
	j, err := e.Store.Get(r.PathValue("id"))
	if err != nil {
		return Job{}, err
	}
	if !sameOwner(j.Owner, requestOwner(r)) {
		return Job{}, ErrNotFound
	}
	return j, nil
}
func handler(e *Engine, token string, accounts AccountService) http.Handler {
	api := http.NewServeMux()
	mountWorkerManagement(api, e)
	api.HandleFunc("GET /v1/connections", func(w http.ResponseWriter, r *http.Request) {
		list := make([]connector.Binding, 0)
		for _, b := range e.connectionList() {
			if o := requestOwner(r); o != nil {
				b.CanRead = b.AllowsRead(o.Subject)
				b.CanWrite = b.AllowsWrite(o.Subject)
			}
			if b.CanRead || b.CanWrite {
				b.Fingerprint = ""
				b.Resource = ""
				list = append(list, b)
			}
		}
		respond(w, 200, map[string]any{"connections": list, "personal_connections_enabled": accounts != nil && e.Connections != nil && !e.HostedDisabled, "hosted_execution_enabled": !e.HostedDisabled})
	})
	api.HandleFunc("POST /v1/connections", func(w http.ResponseWriter, r *http.Request) {
		owner := requestOwner(r)
		if owner == nil || e.Connections == nil {
			apiError(w, ErrForbidden)
			return
		}
		var definition ConnectionDefinition
		if err := decode(w, r, &definition); err != nil {
			apiError(w, err)
			return
		}
		binding, created, err := e.Connections.Create(*owner, definition, r.Header.Get("Idempotency-Key"))
		if err != nil {
			apiError(w, err)
			return
		}
		binding.Fingerprint = ""
		binding.Resource = ""
		status := 200
		if created {
			status = 201
		}
		respond(w, status, binding)
	})
	api.HandleFunc("PUT /v1/connections/{id}/credentials", func(w http.ResponseWriter, r *http.Request) {
		owner := requestOwner(r)
		if owner == nil || e.Connections == nil {
			apiError(w, ErrForbidden)
			return
		}
		var credentials connector.PersonalCredentials
		if err := decode(w, r, &credentials); err != nil {
			apiError(w, err)
			return
		}
		binding, err := e.Connections.Rotate(*owner, r.PathValue("id"), credentials)
		if err != nil {
			apiError(w, err)
			return
		}
		binding.Fingerprint = ""
		binding.Resource = ""
		respond(w, 200, binding)
	})
	api.HandleFunc("DELETE /v1/connections/{id}", func(w http.ResponseWriter, r *http.Request) {
		owner := requestOwner(r)
		if owner == nil || e.Connections == nil {
			apiError(w, ErrForbidden)
			return
		}
		id := r.PathValue("id")
		for _, job := range e.Store.List() {
			if sameOwner(job.Owner, owner) && (job.Spec.Source == id || job.Spec.Sink == id) && (job.State == "queued" || job.State == "running" || job.State == "retry_wait" || job.State == "cancel_requested") {
				apiError(w, errors.New("cancel active jobs before removing this connection"))
				return
			}
		}
		if err := e.Connections.Delete(*owner, id); err != nil {
			apiError(w, err)
			return
		}
		respond(w, 200, map[string]bool{"deleted": true})
	})
	api.HandleFunc("GET /v1/jobs", func(w http.ResponseWriter, r *http.Request) {
		limit, err := queryInt(r, "limit", 100, 1, 1000)
		if err != nil {
			apiError(w, err)
			return
		}
		offset, err := queryInt(r, "offset", 0, 0, 100000000)
		if err != nil {
			apiError(w, err)
			return
		}
		jobs := e.Store.List()
		filtered := make([]Job, 0)
		for _, j := range jobs {
			if sameOwner(j.Owner, requestOwner(r)) && (r.URL.Query().Get("state") == "" || j.State == r.URL.Query().Get("state")) {
				filtered = append(filtered, publicJob(j))
			}
		}
		start := min(offset, len(filtered))
		end := min(start+limit, len(filtered))
		respond(w, 200, map[string]any{"jobs": filtered[start:end], "total": len(filtered)})
	})
	api.HandleFunc("POST /v1/jobs", func(w http.ResponseWriter, r *http.Request) {
		var spec Spec
		if err := decode(w, r, &spec); err != nil {
			apiError(w, err)
			return
		}
		actor := principal(token)
		var owners []Owner
		if o := requestOwner(r); o != nil {
			owners = append(owners, *o)
			actor = "account:" + o.Subject
		}
		j, created, err := e.Submit(spec, r.Header.Get("Idempotency-Key"), actor, owners...)
		if err != nil {
			if e.Store.Health() != nil {
				respond(w, 503, map[string]string{"error": "job store unavailable"})
			} else {
				apiError(w, err)
			}
			return
		}
		status := 200
		if created {
			status = 202
		}
		w.Header().Set("Location", "/v1/jobs/"+j.ID)
		respond(w, status, publicJob(j))
	})
	api.HandleFunc("GET /v1/jobs/{id}", func(w http.ResponseWriter, r *http.Request) {
		j, err := ownedJob(e, r)
		if err != nil {
			apiError(w, err)
			return
		}
		respond(w, 200, publicJob(j))
	})
	api.HandleFunc("GET /v1/jobs/{id}/events", func(w http.ResponseWriter, r *http.Request) {
		if _, err := ownedJob(e, r); err != nil {
			apiError(w, err)
			return
		}
		after, err := strconv.ParseInt(r.URL.Query().Get("after"), 10, 64)
		if r.URL.Query().Get("after") == "" {
			after = 0
			err = nil
		}
		if err != nil || after < 0 {
			apiError(w, errors.New("invalid after cursor"))
			return
		}
		limit, err := queryInt(r, "limit", 100, 1, 1000)
		if err != nil {
			apiError(w, err)
			return
		}
		events, err := e.Store.Events(r.PathValue("id"), after, limit)
		if err != nil {
			apiError(w, err)
			return
		}
		next := after
		if len(events) > 0 {
			next = events[len(events)-1].Sequence
		}
		respond(w, 200, map[string]any{"events": events, "next_after": next})
	})
	api.HandleFunc("POST /v1/jobs/{id}/{action}", func(w http.ResponseWriter, r *http.Request) {
		var body struct{}
		if err := decode(w, r, &body); err != nil {
			apiError(w, err)
			return
		}
		current, err := ownedJob(e, r)
		if err != nil {
			apiError(w, err)
			return
		}
		if r.PathValue("action") == "resume" && !e.authorized(current) {
			apiError(w, ErrForbidden)
			return
		}
		actor := principal(token)
		if o := requestOwner(r); o != nil {
			actor = "account:" + o.Subject
		}
		j, err := e.Action(r.PathValue("id"), r.PathValue("action"), actor)
		if err != nil {
			apiError(w, err)
			return
		}
		respond(w, 200, publicJob(j))
	})
	mux := http.NewServeMux()
	mountWorkerProtocol(mux, e)
	if accounts != nil {
		accounts.Mount(mux)
	} else {
		mux.HandleFunc("GET /auth/config", func(w http.ResponseWriter, r *http.Request) { respond(w, 200, map[string]string{"mode": "operator"}) })
	}
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		if e.Store.Health() != nil || e.Connections.Health() != nil || e.Workers.Health() != nil {
			respond(w, 503, map[string]string{"status": "unavailable"})
			return
		}
		respond(w, 200, map[string]string{"status": "ok"})
	})
	mux.Handle("/v1/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		if accounts != nil {
			p, err := accounts.Authenticate(r)
			if err != nil {
				status := 401
				if errors.Is(err, accountauth.ErrUnavailable) {
					status = 503
				}
				respond(w, status, map[string]string{"error": "Polign sign-in unavailable or expired"})
				return
			}
			if p.Subject == "" || p.OwnerID == "" {
				respond(w, 401, map[string]string{"error": "Polign sign-in required"})
				return
			}
			owner := &Owner{ID: p.OwnerID, Subject: p.Subject, Realm: accounts.Realm()}
			api.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ownerContext{}, owner)))
			return
		}
		if token != "" && subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+token)) != 1 {
			respond(w, 401, map[string]string{"error": "valid bearer token required"})
			return
		}
		if token == "" {
			host := r.Host
			if h, _, err := net.SplitHostPort(host); err == nil {
				host = h
			}
			ip := net.ParseIP(host)
			if host != "localhost" && (ip == nil || !ip.IsLoopback()) {
				respond(w, 403, map[string]string{"error": "local access required"})
				return
			}
		}
		if origin := r.Header.Get("Origin"); origin != "" {
			u, err := url.Parse(origin)
			if err != nil || u.Host != r.Host || (u.Scheme != "http" && u.Scheme != "https") {
				respond(w, 403, map[string]string{"error": "cross-origin requests are disabled"})
				return
			}
		}
		api.ServeHTTP(w, r)
	}))
	mux.HandleFunc("GET /worker-guide.html", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/connections/new", http.StatusFound)
	})
	for name, contentType := range map[string]string{
		"docs.html": "text/html; charset=utf-8", "landing.js": "text/javascript; charset=utf-8", "connection-setup.html": "text/html; charset=utf-8", "worker-reference.html": "text/html; charset=utf-8", "connection-setup.js": "text/javascript; charset=utf-8", "connection-fields.js": "text/javascript; charset=utf-8", "worker-script.js": "text/javascript; charset=utf-8", "index.html": "text/html; charset=utf-8", "app.js": "text/javascript; charset=utf-8", "style.css": "text/css; charset=utf-8",
		"bloom.jpg": "image/jpeg", "favicon.svg": "image/svg+xml", "polign-sans.woff2": "font/woff2", "FONT-LICENSE.txt": "text/plain; charset=utf-8",
	} {
		path := "/" + name
		if name == "connection-setup.html" {
			path = "/connections/new"
		}
		if name == "index.html" {
			path = "/{$}"
		}
		mux.HandleFunc("GET "+path, func(w http.ResponseWriter, r *http.Request) {
			data, err := assets.ReadFile("web/" + name)
			if err != nil {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Type", contentType)
			_, _ = w.Write(data)
		})
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; style-src 'self'; script-src 'self'; object-src 'none'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'")
		mux.ServeHTTP(w, r)
	})
}

func principal(token string) string {
	if token == "" {
		return "local"
	}
	return "operator-token"
}

func decode(w http.ResponseWriter, r *http.Request, out any) error {
	t, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || t != "application/json" {
		return errors.New("Content-Type must be application/json")
	}
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10))
	d.DisallowUnknownFields()
	if err := d.Decode(out); err != nil {
		return errors.New("invalid JSON request or unknown fields")
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return errors.New("request must contain one JSON object")
	}
	return nil
}

func queryInt(r *http.Request, key string, fallback, low, high int) (int, error) {
	s := r.URL.Query().Get(key)
	if s == "" {
		return fallback, nil
	}
	v, err := strconv.Atoi(s)
	if err != nil || v < low || v > high {
		return 0, errors.New("invalid " + key)
	}
	return v, nil
}

func apiError(w http.ResponseWriter, err error) {
	status := 400
	if errors.Is(err, ErrForbidden) {
		status = 403
	}
	if errors.Is(err, ErrNotFound) {
		status = 404
	}
	if errors.Is(err, ErrConflict) {
		status = 409
	}
	message := err.Error()
	if errors.Is(err, ErrWorkersUnavailable) {
		status = 503
		message = "worker registry unavailable"
	}
	if errors.Is(err, ErrVaultUnavailable) {
		status = 503
		message = "connection credential store unavailable"
	}
	if strings.Contains(message, "journal") {
		status = 503
		message = "job store unavailable"
	}
	respond(w, status, map[string]string{"error": message})
}

func respond(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
