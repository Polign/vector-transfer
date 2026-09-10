package control

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/anuptalwalkar/vector-transfer/accountauth"
	"github.com/anuptalwalkar/vector-transfer/connector"
)

type testAccounts struct{}

func (testAccounts) Realm() string        { return "polign-test" }
func (testAccounts) Mount(*http.ServeMux) {}
func (testAccounts) Authenticate(r *http.Request) (accountauth.Principal, error) {
	name := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if name != "alice" && name != "bob" {
		return accountauth.Principal{}, accountauth.ErrUnauthorized
	}
	return accountauth.Principal{Subject: name, OwnerID: "owner-" + name}, nil
}

func TestAccountJobsArePrivateAndPermissionsAreDirectional(t *testing.T) {
	e := testEngine(t, sourceFunc(func(context.Context, string, int) (connector.Page, error) { return connector.Page{Done: true}, nil }), sinkFunc(func(context.Context, []connector.Record) error { return nil }))
	for name, b := range e.Registry {
		b.CanRead = b.Source != nil
		b.CanWrite = b.Sink != nil
		b.ReadSubjects = []string{"alice", "bob"}
		b.WriteSubjects = []string{"alice", "bob"}
		e.Registry[name] = b
	}
	e.Registry["private"] = connector.Binding{Name: "private", Source: e.Registry["source"].Source, CanRead: true, ReadSubjects: []string{"alice"}}
	h := AccountHandler(e, testAccounts{})
	request := func(method, path, user, payload string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, "https://transfer.test"+path, strings.NewReader(payload))
		r.Header.Set("Authorization", "Bearer "+user)
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Idempotency-Key", "same-key")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	body, _ := json.Marshal(spec())
	if w := request("POST", "/v1/jobs", "operator-token", string(body)); w.Code != 401 {
		t.Fatal("operator fallback accepted")
	}
	var alice, bob Job
	for name, out := range map[string]*Job{"alice": &alice, "bob": &bob} {
		w := request("POST", "/v1/jobs", name, string(body))
		if w.Code != 202 {
			t.Fatalf("submit: %d %s", w.Code, w.Body)
		}
		if err := json.Unmarshal(w.Body.Bytes(), out); err != nil {
			t.Fatal(err)
		}
		if out.Owner != nil {
			t.Fatal("internal owner leaked")
		}
	}
	if alice.ID == bob.ID {
		t.Fatal("idempotency key crossed accounts")
	}
	if request("POST", "/v1/jobs", "alice", string(body)).Code != 200 {
		t.Fatal("same-owner idempotency failed")
	}
	for _, suffix := range []string{"", "/events", "/cancel", "/resume"} {
		method := "GET"
		if suffix == "/cancel" || suffix == "/resume" {
			method = "POST"
		}
		if w := request(method, "/v1/jobs/"+alice.ID+suffix, "bob", "{}"); w.Code != 404 {
			t.Fatalf("cross-owner %s: %d", suffix, w.Code)
		}
	}
	if w := request("GET", "/v1/jobs", "bob", ""); strings.Contains(w.Body.String(), alice.ID) || !strings.Contains(w.Body.String(), bob.ID) {
		t.Fatal("job list isolation failed")
	}
	if w := request("GET", "/v1/connections", "bob", ""); strings.Contains(w.Body.String(), "private") || strings.Contains(w.Body.String(), "alice") {
		t.Fatal("connection permissions leaked")
	}
	_, err := e.Store.Update(alice.ID, "checkpoint", "worker", "Saved batch", "", func(j *Job) error { j.Cursor = "sensitive-provider-cursor"; return nil })
	if err != nil {
		t.Fatal(err)
	}
	if w := request("GET", "/v1/jobs/"+alice.ID, "alice", ""); strings.Contains(w.Body.String(), "sensitive-provider-cursor") {
		t.Fatal("provider cursor leaked")
	}
	for _, direction := range []string{"source", "sink"} {
		original := e.Registry[direction]
		denied := original
		if direction == "source" {
			denied.ReadSubjects = []string{"bob"}
		} else {
			denied.WriteSubjects = []string{"bob"}
		}
		e.Registry[direction] = denied
		if w := request("POST", "/v1/jobs", "alice", string(body)); w.Code != 403 {
			t.Fatalf("revoked %s accepted", direction)
		}
		e.Registry[direction] = original
	}
	for _, field := range []string{`"owner":{"id":"owner-bob"}`, `"api_key":"secret"`, `"source_token":"secret"`} {
		payload := strings.TrimSuffix(string(body), "}") + "," + field + "}"
		if request("POST", "/v1/jobs", "alice", payload).Code != 400 {
			t.Fatal("untrusted ownership or credentials accepted")
		}
	}
	if request("POST", "/v1/jobs/"+alice.ID+"/cancel", "alice", "{}").Code != 200 {
		t.Fatal("own cancel failed")
	}
	b := e.Registry["sink"]
	b.WriteSubjects = nil
	e.Registry["sink"] = b
	if request("POST", "/v1/jobs/"+alice.ID+"/resume", "alice", "{}").Code != 403 {
		t.Fatal("resume bypassed revoked grant")
	}
}

func TestRevokedGrantAndChangedRealmStopWorker(t *testing.T) {
	for _, changedRealm := range []bool{false, true} {
		t.Run(map[bool]string{false: "revoked", true: "realm"}[changedRealm], func(t *testing.T) {
			reads, writes := 0, 0
			e := testEngine(t, sourceFunc(func(context.Context, string, int) (connector.Page, error) {
				reads++
				return connector.Page{Records: []connector.Record{record("a")}, Done: true}, nil
			}), sinkFunc(func(context.Context, []connector.Record) error { writes++; return nil }))
			e.AccountRealm = "polign-test"
			for name, b := range e.Registry {
				b.ReadSubjects = []string{"alice"}
				b.WriteSubjects = []string{"alice"}
				e.Registry[name] = b
			}
			_, _, err := e.Submit(spec(), "", "account:alice", Owner{ID: "owner-alice", Subject: "alice", Realm: e.AccountRealm})
			if err != nil {
				t.Fatal(err)
			}
			j, ok, err := e.Store.Claim()
			if err != nil || !ok {
				t.Fatal("claim failed")
			}
			if changedRealm {
				e.AccountRealm = "another-account-service"
			} else {
				b := e.Registry["sink"]
				b.WriteSubjects = nil
				e.Registry["sink"] = b
			}
			if err := e.execute(context.Background(), j); err != nil {
				t.Fatal(err)
			}
			got, _ := e.Store.Get(j.ID)
			if got.State != "failed" || reads != 0 || writes != 0 {
				t.Fatalf("unauthorized execution: %+v", got)
			}
		})
	}
}

func TestScheduledRestartSurvivesReopenAndPreservesCheckpoint(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	fail := true
	var cursors []string
	e := &Engine{Store: store, Registry: connector.Registry{
		"source": {Fingerprint: "s", Source: sourceFunc(func(_ context.Context, cursor string, _ int) (connector.Page, error) {
			cursors = append(cursors, cursor)
			if cursor == "" {
				return connector.Page{Records: []connector.Record{record("a")}, Next: "second-page"}, nil
			}
			if fail {
				return connector.Page{}, &connector.Transient{Err: errors.New("credential-super-secret")}
			}
			return connector.Page{Records: []connector.Record{record("b")}, Done: true}, nil
		})},
		"sink": {Fingerprint: "d", Sink: sinkFunc(func(context.Context, []connector.Record) error { return nil })},
	}}
	spec := spec()
	spec.MaxAttempts = 1
	spec.MaxRestarts = 1
	job, _, err := e.Submit(spec, "", "tester")
	if err != nil {
		t.Fatal(err)
	}
	j, _, _ := store.Claim()
	if err := e.execute(context.Background(), j); err != nil {
		t.Fatal(err)
	}
	j, _ = store.Get(job.ID)
	if j.State != "retry_wait" || j.Records != 1 || j.Cursor != "second-page" || j.Restarts != 1 || j.NextRunAt == nil || j.LastCheckpointAt == nil {
		t.Fatalf("restart: %+v", j)
	}
	if _, ok, _ := store.Claim(); ok {
		t.Fatal("claimed before retry time")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	journal, err := os.ReadFile(filepath.Join(dir, "journal.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(journal), "credential-super-secret") {
		t.Fatal("provider error leaked into journal")
	}
	store, err = OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	e.Store = store
	if err := store.Recover(); err != nil {
		t.Fatal(err)
	}
	j, _ = store.Get(job.ID)
	if j.State != "retry_wait" || j.NextRunAt == nil {
		t.Fatal("retry schedule lost after restart")
	}
	_, err = store.Update(job.ID, "test_clock", "test", "Make scheduled retry eligible", "", func(j *Job) error { past := time.Now().Add(-time.Second); j.NextRunAt = &past; return nil })
	if err != nil {
		t.Fatal(err)
	}
	fail = false
	j, ok, err := store.Claim()
	if err != nil || !ok {
		t.Fatal("due retry not claimed")
	}
	if err := e.execute(context.Background(), j); err != nil {
		t.Fatal(err)
	}
	j, _ = store.Get(job.ID)
	if j.State != "succeeded" || j.Records != 2 || j.Runs != 2 || len(cursors) != 3 || cursors[2] != "second-page" {
		t.Fatalf("bad recovery: %+v %v", j, cursors)
	}
}

func TestRestartBudgetAndCancelScheduledJob(t *testing.T) {
	e := testEngine(t, sourceFunc(func(context.Context, string, int) (connector.Page, error) {
		return connector.Page{}, &connector.Transient{Err: errors.New("temporary")}
	}), sinkFunc(func(context.Context, []connector.Record) error { return nil }))
	spec := spec()
	spec.MaxAttempts = 1
	spec.MaxRestarts = 1
	j, _, err := e.Submit(spec, "", "tester")
	if err != nil {
		t.Fatal(err)
	}
	claimed, _, _ := e.Store.Claim()
	if err := e.execute(context.Background(), claimed); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Action(j.ID, "cancel", "tester"); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := e.Store.Claim(); ok {
		t.Fatal("canceled retry executed")
	}
	if _, err := e.Action(j.ID, "resume", "tester"); err != nil {
		t.Fatal(err)
	}
	claimed, _, _ = e.Store.Claim()
	if err := e.execute(context.Background(), claimed); err != nil {
		t.Fatal(err)
	}
	_, err = e.Store.Update(j.ID, "test_clock", "test", "Advance retry", "", func(j *Job) error { past := time.Now().Add(-time.Second); j.NextRunAt = &past; return nil })
	if err != nil {
		t.Fatal(err)
	}
	claimed, _, _ = e.Store.Claim()
	if err := e.execute(context.Background(), claimed); err != nil {
		t.Fatal(err)
	}
	got, _ := e.Store.Get(j.ID)
	if got.State != "failed" || got.Restarts != 1 {
		t.Fatalf("restart budget not enforced: %+v", got)
	}
}

func TestPreAccountJournalRemainsReadable(t *testing.T) {
	// This golden entry was written by the previous release, before ownership,
	// phases, checkpoint timestamps, and automatic restart fields existed.
	b, err := os.ReadFile("testdata/legacy-journal.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "journal.jsonl"), b, 0600); err != nil {
		t.Fatal(err)
	}
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	jobs := s.List()
	if len(jobs) != 1 || jobs[0].Owner != nil || jobs[0].Spec.MaxRestarts != 0 {
		t.Fatal("legacy semantics changed")
	}
	if _, ok, err := s.Claim(); err != nil || !ok {
		t.Fatal("legacy queued job cannot run")
	}
}
