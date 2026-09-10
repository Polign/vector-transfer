// Package accountauth connects browser sessions to the existing Polign account
// API. OAuth credentials stay in server memory; browsers receive opaque cookies.
package accountauth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

var ErrUnauthorized = errors.New("Polign sign-in required")
var ErrUnavailable = errors.New("Polign account service unavailable")

const sessionCookie = "__Host-vtransfer-session"
const loginCookie = "__Host-vtransfer-login"
const accountScope = "https://account.polign.com/account"

type Config struct {
	PublicURL  string `json:"public_url"`
	AccountURL string `json:"account_url"`
	APIURL     string `json:"api_url"`
	CognitoURL string `json:"cognito_url"`
	ClientID   string `json:"client_id"`
}

// Principal comes only from the authenticated account API, never request fields.
type Principal struct {
	Subject       string `json:"subject"`
	Email         string `json:"email"`
	EmailVerified bool   `json:"email_verified"`
	OwnerID       string `json:"-"`
}

type transaction struct {
	verifier, state string
	expires         time.Time
}
type session struct {
	mu                    sync.Mutex
	access, refresh, csrf string
	expires, deadline     time.Time
	subject               string
	closed                bool
}

type Service struct {
	config       Config
	client       *http.Client
	mu           sync.Mutex
	transactions map[string]transaction
	sessions     map[string]*session
}

func Load(path string) (*Service, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	d := json.NewDecoder(io.LimitReader(f, 64<<10))
	d.DisallowUnknownFields()
	var cfg Config
	if err := d.Decode(&cfg); err != nil {
		return nil, errors.New("invalid account authentication configuration")
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return nil, errors.New("account configuration must contain one JSON object")
	}
	return New(cfg)
}

func New(cfg Config) (*Service, error) {
	for _, value := range []string{cfg.PublicURL, cfg.AccountURL, cfg.APIURL, cfg.CognitoURL} {
		u, err := url.Parse(value)
		if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
			return nil, errors.New("account authentication requires HTTPS origins without credentials, paths, or query strings")
		}
	}
	if strings.TrimSpace(cfg.ClientID) == "" {
		return nil, errors.New("account client_id is required")
	}
	cfg.PublicURL = strings.TrimRight(cfg.PublicURL, "/")
	cfg.AccountURL = strings.TrimRight(cfg.AccountURL, "/")
	cfg.APIURL = strings.TrimRight(cfg.APIURL, "/")
	cfg.CognitoURL = strings.TrimRight(cfg.CognitoURL, "/")
	return &Service{config: cfg, client: &http.Client{Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, transactions: map[string]transaction{}, sessions: map[string]*session{}}, nil
}

func random() string {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return base64.RawURLEncoding.EncodeToString(b[:])
}
func digest(s string) string                   { b := sha256.Sum256([]byte(s)); return hex.EncodeToString(b[:]) }
func (s *Service) Realm() string               { return digest(s.config.APIURL) }
func (s *Service) Owner(subject string) string { return digest(s.config.APIURL + "\x00" + subject) }

func (s *Service) sameOrigin(r *http.Request) bool {
	u, _ := url.Parse(s.config.PublicURL)
	return r.Host == u.Host && (r.Header.Get("Origin") == "" || r.Header.Get("Origin") == s.config.PublicURL)
}

// SecureOrigin requires HTTPS at the browser-facing proxy. The backend must be
// private, and the proxy must overwrite X-Forwarded-Proto from the actual scheme.
func (s *Service) SecureOrigin(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil && r.Header.Get("X-Forwarded-Proto") != "https" {
			w.Header().Set("Cache-Control", "no-store")
			if r.Method != "GET" && r.Method != "HEAD" {
				write(w, http.StatusForbidden, map[string]string{"error": "HTTPS required"})
				return
			}
			http.Redirect(w, r, s.config.PublicURL+r.URL.RequestURI(), http.StatusPermanentRedirect)
			return
		}
		w.Header().Set("Strict-Transport-Security", "max-age=31536000")
		next.ServeHTTP(w, r)
	})
}

func (s *Service) Mount(mux *http.ServeMux) {
	mux.HandleFunc("GET /auth/config", func(w http.ResponseWriter, r *http.Request) {
		write(w, 200, map[string]any{"mode": "polign", "account_url": s.config.AccountURL, "login_url": "/auth/login"})
	})
	mux.HandleFunc("GET /auth/login", s.login)
	mux.HandleFunc("GET /auth/callback", s.callback)
	mux.HandleFunc("GET /auth/session", func(w http.ResponseWriter, r *http.Request) {
		p, err := s.Authenticate(r)
		if err != nil {
			authError(w, err)
			return
		}
		csrf := ""
		if session := s.lookup(r); session != nil && r.Header.Get("Authorization") == "" {
			session.mu.Lock()
			csrf = session.csrf
			session.mu.Unlock()
		}
		write(w, 200, map[string]any{"subject": p.Subject, "email": p.Email, "csrf_token": csrf})
	})
	mux.HandleFunc("POST /auth/logout", s.logout)
}

func write(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Referrer-Policy", "no-referrer")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
func authError(w http.ResponseWriter, err error) {
	status := 401
	if errors.Is(err, ErrUnavailable) {
		status = 503
	}
	write(w, status, map[string]string{"error": err.Error()})
}
func cookie(w http.ResponseWriter, name, value string, age int) {
	http.SetCookie(w, &http.Cookie{Name: name, Value: value, Path: "/", Secure: true, HttpOnly: true, SameSite: http.SameSiteLaxMode, MaxAge: age})
}

func (s *Service) pruneLocked(now time.Time) {
	for key, t := range s.transactions {
		if !now.Before(t.expires) {
			delete(s.transactions, key)
		}
	}
	for key, session := range s.sessions {
		if !now.Before(session.deadline) {
			delete(s.sessions, key)
		}
	}
}

func (s *Service) login(w http.ResponseWriter, r *http.Request) {
	if !s.sameOrigin(r) {
		write(w, 403, map[string]string{"error": "unexpected transfer origin"})
		return
	}
	state, browser, verifier := random(), random(), random()
	s.mu.Lock()
	s.pruneLocked(time.Now())
	if len(s.transactions) >= 10000 || len(s.sessions) >= 10000 {
		s.mu.Unlock()
		write(w, 503, map[string]string{"error": "sign-in capacity reached"})
		return
	}
	s.transactions[digest(browser)] = transaction{verifier: verifier, state: state, expires: time.Now().Add(5 * time.Minute)}
	s.mu.Unlock()
	cookie(w, loginCookie, browser, 300)
	hash := sha256.Sum256([]byte(verifier))
	q := url.Values{"response_type": {"code"}, "client_id": {s.config.ClientID}, "redirect_uri": {s.config.PublicURL + "/auth/callback"}, "scope": {"openid email profile " + accountScope}, "state": {state}, "code_challenge_method": {"S256"}, "code_challenge": {base64.RawURLEncoding.EncodeToString(hash[:])}}
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Referrer-Policy", "no-referrer")
	http.Redirect(w, r, s.config.CognitoURL+"/oauth2/authorize?"+q.Encode(), http.StatusFound)
}

type tokens struct {
	Access  string `json:"access_token"`
	Refresh string `json:"refresh_token"`
	Expires int    `json:"expires_in"`
	Type    string `json:"token_type"`
}

func (s *Service) exchange(ctx context.Context, form url.Values) (tokens, error) {
	form.Set("client_id", s.config.ClientID)
	req, _ := http.NewRequestWithContext(ctx, "POST", s.config.CognitoURL+"/oauth2/token", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := s.client.Do(req)
	if err != nil {
		return tokens{}, ErrUnavailable
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 500 {
		return tokens{}, ErrUnavailable
	}
	if resp.StatusCode != 200 {
		return tokens{}, ErrUnauthorized
	}
	var t tokens
	if json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&t) != nil || t.Access == "" || t.Expires < 1 || t.Expires > 86400 || !strings.EqualFold(t.Type, "Bearer") {
		return tokens{}, ErrUnauthorized
	}
	return t, nil
}

func (s *Service) profile(ctx context.Context, access string) (Principal, error) {
	req, _ := http.NewRequestWithContext(ctx, "GET", s.config.APIURL+"/v1/me", nil)
	req.Header.Set("Authorization", "Bearer "+access)
	resp, err := s.client.Do(req)
	if err != nil {
		return Principal{}, ErrUnavailable
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 500 || resp.StatusCode == 429 {
		return Principal{}, ErrUnavailable
	}
	if resp.StatusCode != 200 {
		return Principal{}, ErrUnauthorized
	}
	var p Principal
	if json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&p) != nil || p.Subject == "" || len(p.Subject) > 256 || (p.Email != "" && !p.EmailVerified) {
		return Principal{}, ErrUnauthorized
	}
	p.OwnerID = s.Owner(p.Subject)
	return p, nil
}

func (s *Service) callback(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Referrer-Policy", "no-referrer")
	if !s.sameOrigin(r) {
		authError(w, ErrUnauthorized)
		return
	}
	bound, err := r.Cookie(loginCookie)
	if err != nil {
		authError(w, ErrUnauthorized)
		return
	}
	s.mu.Lock()
	t, ok := s.transactions[digest(bound.Value)]
	delete(s.transactions, digest(bound.Value))
	s.mu.Unlock()
	cookie(w, loginCookie, "", -1)
	if !ok || time.Now().After(t.expires) || subtle.ConstantTimeCompare([]byte(t.state), []byte(r.URL.Query().Get("state"))) != 1 || r.URL.Query().Get("code") == "" || r.URL.Query().Get("error") != "" {
		authError(w, ErrUnauthorized)
		return
	}
	tok, err := s.exchange(r.Context(), url.Values{"grant_type": {"authorization_code"}, "code": {r.URL.Query().Get("code")}, "code_verifier": {t.verifier}, "redirect_uri": {s.config.PublicURL + "/auth/callback"}})
	if err != nil {
		authError(w, err)
		return
	}
	p, err := s.profile(r.Context(), tok.Access)
	if err != nil {
		authError(w, err)
		return
	}
	id := random()
	now := time.Now()
	s.mu.Lock()
	s.pruneLocked(now)
	if len(s.sessions) >= 10000 {
		s.mu.Unlock()
		authError(w, ErrUnavailable)
		return
	}
	if old, e := r.Cookie(sessionCookie); e == nil {
		delete(s.sessions, digest(old.Value))
	}
	s.sessions[digest(id)] = &session{access: tok.Access, refresh: tok.Refresh, csrf: random(), expires: now.Add(time.Duration(tok.Expires) * time.Second), deadline: now.Add(8 * time.Hour), subject: p.Subject}
	s.mu.Unlock()
	cookie(w, sessionCookie, id, 8*3600)
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (s *Service) lookup(r *http.Request) *session {
	c, err := r.Cookie(sessionCookie)
	if err != nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	ss := s.sessions[digest(c.Value)]
	if ss != nil && !time.Now().Before(ss.deadline) {
		delete(s.sessions, digest(c.Value))
		return nil
	}
	return ss
}

// Authenticate validates identity with polign_account on every request. Cookie
// mutations additionally require a session-bound CSRF token and exact Origin.
// Explicit bearer requests support CLI clients without using browser cookies.
func (s *Service) Authenticate(r *http.Request) (Principal, error) {
	if !s.sameOrigin(r) {
		return Principal{}, ErrUnauthorized
	}
	if header := r.Header.Get("Authorization"); header != "" {
		if !strings.HasPrefix(header, "Bearer ") || len(header) > 16384 {
			return Principal{}, ErrUnauthorized
		}
		return s.profile(r.Context(), strings.TrimPrefix(header, "Bearer "))
	}
	ss := s.lookup(r)
	if ss == nil {
		return Principal{}, ErrUnauthorized
	}
	ss.mu.Lock()
	defer ss.mu.Unlock()
	if ss.closed {
		return Principal{}, ErrUnauthorized
	}
	if r.Method != "GET" && r.Method != "HEAD" {
		if r.Header.Get("Origin") != s.config.PublicURL || subtle.ConstantTimeCompare([]byte(r.Header.Get("X-CSRF-Token")), []byte(ss.csrf)) != 1 {
			return Principal{}, ErrUnauthorized
		}
	}
	if !time.Now().Add(30 * time.Second).Before(ss.expires) {
		if ss.refresh == "" {
			return Principal{}, ErrUnauthorized
		}
		tok, err := s.exchange(r.Context(), url.Values{"grant_type": {"refresh_token"}, "refresh_token": {ss.refresh}})
		if err != nil {
			return Principal{}, err
		}
		ss.access = tok.Access
		if tok.Refresh != "" {
			ss.refresh = tok.Refresh
		}
		ss.expires = time.Now().Add(time.Duration(tok.Expires) * time.Second)
	}
	p, err := s.profile(r.Context(), ss.access)
	if err == nil && p.Subject != ss.subject {
		ss.closed = true
		return Principal{}, ErrUnauthorized
	}
	return p, err
}

func (s *Service) logout(w http.ResponseWriter, r *http.Request) {
	// Local logout remains possible when the account API is unavailable or tokens expired.
	ss := s.lookup(r)
	if !s.sameOrigin(r) || r.Header.Get("Origin") != s.config.PublicURL || ss == nil || r.Header.Get("Authorization") != "" {
		authError(w, ErrUnauthorized)
		return
	}
	ss.mu.Lock()
	if subtle.ConstantTimeCompare([]byte(r.Header.Get("X-CSRF-Token")), []byte(ss.csrf)) != 1 {
		ss.mu.Unlock()
		authError(w, ErrUnauthorized)
		return
	}
	ss.closed = true
	refresh := ss.refresh
	ss.access = ""
	ss.refresh = ""
	ss.mu.Unlock()
	if refresh != "" {
		form := url.Values{"token": {refresh}, "client_id": {s.config.ClientID}}
		req, _ := http.NewRequestWithContext(r.Context(), "POST", s.config.CognitoURL+"/oauth2/revoke", strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		if resp, err := s.client.Do(req); err == nil {
			resp.Body.Close()
		}
	}
	if c, err := r.Cookie(sessionCookie); err == nil {
		s.mu.Lock()
		delete(s.sessions, digest(c.Value))
		s.mu.Unlock()
	}
	cookie(w, sessionCookie, "", -1)
	q := url.Values{"client_id": {s.config.ClientID}, "logout_uri": {s.config.PublicURL + "/"}}
	write(w, 200, map[string]string{"logout_url": s.config.CognitoURL + "/logout?" + q.Encode()})
}
