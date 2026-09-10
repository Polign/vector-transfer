package accountauth

import (
	"crypto/sha256"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

type transportFunc func(*http.Request) (*http.Response, error)

func (f transportFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func fixture(t *testing.T) (*Service, http.Handler, *string, *int) {
	t.Helper()
	s, err := New(Config{PublicURL: "https://transfer.test", AccountURL: "https://account.test", APIURL: "https://api.test", CognitoURL: "https://login.test", ClientID: "public-client"})
	if err != nil {
		t.Fatal(err)
	}
	challenge, subject, status := "", "alice", 200
	s.client.Transport = transportFunc(func(r *http.Request) (*http.Response, error) {
		body, code := "", 200
		switch r.URL.String() {
		case "https://login.test/oauth2/token":
			if err := r.ParseForm(); err != nil {
				t.Fatal(err)
			}
			if r.Form.Get("client_id") != "public-client" {
				t.Fatal("wrong client")
			}
			if r.Form.Get("grant_type") == "authorization_code" {
				h := sha256.Sum256([]byte(r.Form.Get("code_verifier")))
				if base64.RawURLEncoding.EncodeToString(h[:]) != challenge || r.Form.Get("redirect_uri") != "https://transfer.test/auth/callback" {
					t.Fatal("PKCE or callback mismatch")
				}
			} else if r.Form.Get("refresh_token") != "secret-refresh" {
				t.Fatal("wrong refresh token")
			}
			body = `{"access_token":"secret-access","refresh_token":"secret-refresh","expires_in":3600,"token_type":"Bearer"}`
		case "https://api.test/v1/me":
			if r.Header.Get("Authorization") != "Bearer secret-access" {
				code = 401
			}
			if status != 200 {
				code = status
			}
			body = `{"subject":"` + subject + `"}`
		case "https://login.test/oauth2/revoke":
			body = `{}`
		default:
			t.Fatalf("unexpected credential destination: %s", r.URL)
		}
		return &http.Response{StatusCode: code, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
	})
	mux := http.NewServeMux()
	s.Mount(mux)
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		recorder := httptest.NewRecorder()
		mux.ServeHTTP(recorder, r)
		if r.URL.Path == "/auth/login" {
			u, _ := url.Parse(recorder.Header().Get("Location"))
			challenge = u.Query().Get("code_challenge")
			if u.Query().Get("code_challenge_method") != "S256" {
				t.Fatal("PKCE missing")
			}
		}
		for k, v := range recorder.Header() {
			w.Header()[k] = v
		}
		w.WriteHeader(recorder.Code)
		_, _ = w.Write(recorder.Body.Bytes())
	})
	return s, h, &subject, &status
}

func request(h http.Handler, method, path string, c *http.Cookie, csrf string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, "https://transfer.test"+path, nil)
	if c != nil {
		r.AddCookie(c)
	}
	if csrf != "" {
		r.Header.Set("Origin", "https://transfer.test")
		r.Header.Set("X-CSRF-Token", csrf)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}
func loggedIn(t *testing.T, h http.Handler) *http.Cookie {
	t.Helper()
	login := request(h, "GET", "/auth/login", nil, "")
	u, _ := url.Parse(login.Header().Get("Location"))
	callback := request(h, "GET", "/auth/callback?code=one-use-code&state="+url.QueryEscape(u.Query().Get("state")), login.Result().Cookies()[0], "")
	if callback.Code != 303 {
		t.Fatalf("callback: %d %s", callback.Code, callback.Body.String())
	}
	for _, c := range callback.Result().Cookies() {
		if c.Name == sessionCookie {
			if !c.Secure || !c.HttpOnly || c.SameSite != http.SameSiteLaxMode || c.Path != "/" || c.Domain != "" {
				t.Fatal("unsafe session cookie")
			}
			return c
		}
	}
	t.Fatal("missing session")
	return nil
}

func TestBrowserSessionPKCECSRFRefreshAndLogout(t *testing.T) {
	s, h, _, status := fixture(t)
	c := loggedIn(t, h)
	w := request(h, "GET", "/auth/session", c, "")
	if w.Code != 200 || strings.Contains(w.Body.String(), "secret-") || !strings.Contains(w.Body.String(), "alice") {
		t.Fatalf("session: %s", w.Body.String())
	}
	r := httptest.NewRequest("POST", "https://transfer.test/v1/jobs", nil)
	r.AddCookie(c)
	if _, err := s.Authenticate(r); err != ErrUnauthorized {
		t.Fatal("missing CSRF accepted")
	}
	ss := s.lookup(r)
	r.Header.Set("Origin", "https://evil.test")
	r.Header.Set("X-CSRF-Token", ss.csrf)
	if _, err := s.Authenticate(r); err != ErrUnauthorized {
		t.Fatal("cross-origin accepted")
	}
	r.Header.Set("Origin", "https://transfer.test")
	ss.expires = time.Now().Add(-time.Minute)
	p, err := s.Authenticate(r)
	if err != nil || p.Subject != "alice" || p.OwnerID != s.Owner("alice") {
		t.Fatalf("refresh: %+v %v", p, err)
	}
	*status = 503
	if _, err := s.Authenticate(r); err != ErrUnavailable {
		t.Fatal("account outage did not fail closed")
	}
	if request(h, "POST", "/auth/logout", c, "").Code != 401 {
		t.Fatal("logout missing CSRF accepted")
	}
	if request(h, "POST", "/auth/logout", c, ss.csrf).Code != 200 {
		t.Fatal("local logout failed during outage")
	}
	if request(h, "GET", "/auth/session", c, "").Code != 401 {
		t.Fatal("logged out session accepted")
	}
}

func TestLoginBindingAndIdentityCannotChange(t *testing.T) {
	s, h, subject, _ := fixture(t)
	login := request(h, "GET", "/auth/login", nil, "")
	u, _ := url.Parse(login.Header().Get("Location"))
	path := "/auth/callback?code=code&state=" + u.Query().Get("state")
	if request(h, "GET", path, nil, "").Code != 401 {
		t.Fatal("unbound login accepted")
	}
	if request(h, "GET", path, login.Result().Cookies()[0], "").Code != 303 {
		t.Fatal("bound callback failed")
	}
	if request(h, "GET", path, login.Result().Cookies()[0], "").Code != 401 {
		t.Fatal("callback replay accepted")
	}
	login = request(h, "GET", "/auth/login", nil, "")
	if request(h, "GET", "/auth/callback?code=code&state=wrong", login.Result().Cookies()[0], "").Code != 401 {
		t.Fatal("wrong state accepted")
	}
	c := loggedIn(t, h)
	*subject = "mallory"
	if request(h, "GET", "/auth/session", c, "").Code != 401 {
		t.Fatal("changed identity accepted")
	}
	r := httptest.NewRequest("GET", "https://transfer.test/v1/jobs", nil)
	r.Header.Set("Authorization", "Bearer secret-access")
	p, err := s.Authenticate(r)
	if err != nil || p.Subject != "mallory" {
		t.Fatal("explicit bearer failed")
	}
	r.Host = "evil.test"
	if _, err := s.Authenticate(r); err != ErrUnauthorized {
		t.Fatal("wrong host accepted")
	}
}

func TestAccountConfigRequiresHTTPSOrigins(t *testing.T) {
	for _, origin := range []string{"http://account.test", "https://user:pass@account.test", "https://account.test/path", "https://account.test?secret=x"} {
		_, err := New(Config{PublicURL: origin, AccountURL: "https://account.test", APIURL: "https://api.test", CognitoURL: "https://login.test", ClientID: "client"})
		if err == nil {
			t.Fatalf("accepted %s", origin)
		}
	}
}

func TestSecureOriginRedirectsReadsAndRejectsPlaintextMutations(t *testing.T) {
	s, _, _, _ := fixture(t)
	h := s.SecureOrigin(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }))
	for _, tc := range []struct {
		method, target, forwarded string
		want                      int
	}{
		{"GET", "http://untrusted.test/auth/login", "", 308},
		{"POST", "http://transfer.test/v1/jobs", "", 403},
		{"GET", "http://transfer.test/", "https", 204},
		{"GET", "https://transfer.test/", "", 204},
	} {
		r := httptest.NewRequest(tc.method, tc.target, nil)
		r.Header.Set("X-Forwarded-Proto", tc.forwarded)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != tc.want {
			t.Fatalf("%+v: %d", tc, w.Code)
		}
		if w.Code == 308 && w.Header().Get("Location") != "https://transfer.test/auth/login" {
			t.Fatal("redirect trusted attacker host")
		}
		if w.Code == 204 && w.Header().Get("Strict-Transport-Security") == "" {
			t.Fatal("missing HSTS")
		}
	}
}
