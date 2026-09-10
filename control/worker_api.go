package control

import (
	"net/http"
	"path/filepath"
	"strings"
	"time"
)

func mountWorkerManagement(api *http.ServeMux, e *Engine) {
	api.HandleFunc("GET /v1/workers", func(w http.ResponseWriter, r *http.Request) {
		if e.Workers == nil {
			respond(w, 200, map[string]any{"enabled": false, "workers": []WorkerView{}})
			return
		}
		if e.Workers.Health() != nil {
			apiError(w, ErrWorkersUnavailable)
			return
		}
		respond(w, 200, map[string]any{"enabled": true, "workers": e.Workers.List(requestOwner(r))})
	})
	api.HandleFunc("POST /v1/workers", func(w http.ResponseWriter, r *http.Request) {
		if e.Workers == nil {
			apiError(w, ErrForbidden)
			return
		}
		var body struct {
			Name string `json:"name"`
		}
		if err := decode(w, r, &body); err != nil {
			apiError(w, err)
			return
		}
		view, token, err := e.Workers.Create(requestOwner(r), body.Name)
		if err != nil {
			apiError(w, err)
			return
		}
		respond(w, 201, map[string]any{"worker": view, "enrollment_token": token, "expires_in": 600})
	})
	api.HandleFunc("POST /v1/workers/{id}/revoke", func(w http.ResponseWriter, r *http.Request) {
		if e.Workers == nil {
			apiError(w, ErrForbidden)
			return
		}
		var body struct{}
		if err := decode(w, r, &body); err != nil {
			apiError(w, err)
			return
		}
		if err := e.Workers.Revoke(r.PathValue("id"), requestOwner(r)); err != nil {
			apiError(w, err)
			return
		}
		respond(w, 200, map[string]bool{"revoked": true})
	})
}
func mountWorkerProtocol(mux *http.ServeMux, e *Engine) {
	if e.WorkerDownloads != "" {
		for _, name := range []string{"vtransfer-linux-amd64", "vtransfer-linux-arm64", "vtransfer-darwin-arm64", "SHA256SUMS"} {
			mux.HandleFunc("GET /downloads/"+name, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Disposition", "attachment; filename="+name)
				http.ServeFile(w, r, filepath.Join(e.WorkerDownloads, name))
			})
		}
	}
	if e.Workers == nil {
		return
	}
	mux.HandleFunc("POST /worker/v1/enroll", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			ID        string `json:"id"`
			Token     string `json:"token"`
			PublicKey string `json:"public_key"`
		}
		if err := decode(w, r, &body); err != nil {
			apiError(w, err)
			return
		}
		if err := e.Workers.Enroll(body.ID, body.Token, body.PublicKey); err != nil {
			apiError(w, err)
			return
		}
		respond(w, 200, map[string]bool{"enrolled": true})
	})
	mux.HandleFunc("POST /worker/v1/challenge", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			ID string `json:"id"`
		}
		if err := decode(w, r, &body); err != nil {
			apiError(w, err)
			return
		}
		challenge, err := e.Workers.Challenge(body.ID)
		if err != nil {
			apiError(w, err)
			return
		}
		respond(w, 200, map[string]string{"challenge": challenge})
	})
	mux.HandleFunc("POST /worker/v1/session", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			ID        string `json:"id"`
			Challenge string `json:"challenge"`
			Signature string `json:"signature"`
		}
		if err := decode(w, r, &body); err != nil {
			apiError(w, err)
			return
		}
		token, err := e.Workers.Session(body.ID, body.Challenge, body.Signature)
		if err != nil {
			apiError(w, err)
			return
		}
		respond(w, 200, map[string]any{"token": token, "expires_in": 600})
	})
	auth := func(fn func(http.ResponseWriter, *http.Request, Worker)) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			worker, err := e.Workers.Authenticate(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
			if err != nil {
				respond(w, 401, map[string]string{"error": "worker authentication required"})
				return
			}
			if (e.AccountRealm != "" && (worker.Owner == nil || worker.Owner.Realm != e.AccountRealm)) || (e.AccountRealm == "" && worker.Owner != nil) {
				apiError(w, ErrForbidden)
				return
			}
			fn(w, r, worker)
		}
	}
	mux.HandleFunc("POST /worker/v1/manifest", auth(func(w http.ResponseWriter, r *http.Request, worker Worker) {
		var manifest WorkerManifest
		if err := decode(w, r, &manifest); err != nil {
			apiError(w, err)
			return
		}
		if err := e.Workers.Publish(worker.ID, manifest); err != nil {
			apiError(w, err)
			return
		}
		respond(w, 200, map[string]bool{"accepted": true})
	}))
	mux.HandleFunc("POST /worker/v1/claim", auth(func(w http.ResponseWriter, r *http.Request, worker Worker) {
		var body struct{}
		if err := decode(w, r, &body); err != nil {
			apiError(w, err)
			return
		}
		if err := e.Workers.Touch(worker.ID); err != nil {
			apiError(w, err)
			return
		}
		deadline := time.NewTimer(15 * time.Second)
		defer deadline.Stop()
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		for {
			// Re-check revocation and current manifest while a long poll is outstanding.
			current, err := e.Workers.Authenticate(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
			if err != nil {
				respond(w, 401, map[string]string{"error": "worker authentication required"})
				return
			}
			a, err := e.Store.ClaimWorker(current)
			if err != nil {
				apiError(w, err)
				return
			}
			if a != nil {
				respond(w, 200, map[string]any{"job": a})
				return
			}
			select {
			case <-r.Context().Done():
				return
			case <-deadline.C:
				respond(w, 200, map[string]any{"job": nil})
				return
			case <-tick.C:
			}
		}
	}))
	mux.HandleFunc("POST /worker/v1/jobs/{id}/progress", auth(func(w http.ResponseWriter, r *http.Request, worker Worker) {
		var report WorkerReport
		if err := decode(w, r, &report); err != nil {
			apiError(w, err)
			return
		}
		j, err := e.Store.ReportWorker(worker, r.PathValue("id"), report)
		if err != nil {
			apiError(w, err)
			return
		}
		if err := e.Workers.Touch(worker.ID); err != nil {
			apiError(w, err)
			return
		}
		respond(w, 200, map[string]any{"cancel_requested": j.State == "cancel_requested", "lease_until": j.LeaseUntil})
	}))
}
