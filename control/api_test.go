package control

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/Polign/vector-transfer/connector"
)

func TestAPIAuthenticationIdempotencyAndAuditPagination(t *testing.T) {
	e := testEngine(t, sourceFunc(func(context.Context, string, int) (connector.Page, error) { return connector.Page{Done: true}, nil }), sinkFunc(func(context.Context, []connector.Record) error { return nil }))
	h := Handler(e, "test-token")
	body, _ := json.Marshal(spec())
	request := func(method, path, token, key, payload string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, "http://127.0.0.1"+path, strings.NewReader(payload))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Authorization", "Bearer "+token)
		r.Header.Set("Idempotency-Key", key)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	if w := request("POST", "/v1/jobs", "wrong", "", string(body)); w.Code != 401 {
		t.Fatal("unauthenticated submission accepted")
	}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w := request("POST", "/v1/jobs", "test-token", "one-request", string(body))
			if w.Code != 200 && w.Code != 202 {
				t.Errorf("submit status: %d %s", w.Code, w.Body)
			}
		}()
	}
	wg.Wait()
	jobs := e.Store.List()
	if len(jobs) != 1 {
		t.Fatalf("duplicate jobs: %d", len(jobs))
	}
	j := jobs[0]
	if j.CreatedBy != "operator-token" {
		t.Fatal("untrusted actor recorded")
	}
	changed := strings.Replace(string(body), `"test"`, `"other"`, 1)
	if w := request("POST", "/v1/jobs", "test-token", "one-request", changed); w.Code != 409 {
		t.Fatal("idempotency conflict not detected")
	}
	if w := request("POST", "/v1/jobs/"+j.ID+"/cancel", "test-token", "", "{}"); w.Code != 200 {
		t.Fatal("cancel failed")
	}
	if w := request("POST", "/v1/jobs/"+j.ID+"/resume", "test-token", "", "{}"); w.Code != 200 {
		t.Fatal("resume failed")
	}
	w := request("GET", "/v1/jobs/"+j.ID+"/events?limit=1", "test-token", "", "")
	var events struct {
		Events []Event `json:"events"`
		Next   int64   `json:"next_after"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &events); err != nil {
		t.Fatal(err)
	}
	if len(events.Events) != 1 || events.Next != 1 {
		t.Fatalf("bad audit pagination: %s", w.Body)
	}
	if w := request("GET", "/v1/jobs/"+j.ID+"/events?after=1", "test-token", "", ""); !strings.Contains(w.Body.String(), `"resume"`) {
		t.Fatal("audit continuation missing events")
	}
	if w := request("GET", "/v1/jobs/missing", "test-token", "", ""); w.Code != 404 {
		t.Fatal("missing job status")
	}
}

func TestLocalAPIRejectsCrossOriginAndUnknownFields(t *testing.T) {
	e := testEngine(t, nil, nil)
	h := Handler(e, "")
	for _, tc := range []struct {
		host, origin, body string
		want               int
	}{
		{"attacker.example", "", `{}`, 403},
		{"127.0.0.1", "https://attacker.example", `{}`, 403},
		{"127.0.0.1", "", `{"api_key":"secret"}`, 400},
		{"127.0.0.1", "", `{} {}`, 400},
	} {
		r := httptest.NewRequest("POST", "http://"+tc.host+"/v1/jobs", strings.NewReader(tc.body))
		r.Header.Set("Origin", tc.origin)
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != tc.want {
			t.Errorf("%+v: status %d", tc, w.Code)
		}
	}
	r := httptest.NewRequest("GET", "http://127.0.0.1/", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "<title>Vector Transfer · Polign</title>") || w.Header().Get("Content-Security-Policy") == "" {
		t.Fatal("console not served correctly")
	}
}
