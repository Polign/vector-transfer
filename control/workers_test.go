package control

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func workerFixture(t *testing.T) (*Workers, Worker, string) {
	t.Helper()
	ws, err := OpenWorkers(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ws.Close() })
	o := Owner{ID: "tenant-a", Subject: "alice", Realm: "realm"}
	v, token, err := ws.Create(&o, "production")
	if err != nil {
		t.Fatal(err)
	}
	w, err := ws.Get(v.ID, &o)
	if err != nil {
		t.Fatal(err)
	}
	return ws, w, token
}
func testManifest() WorkerManifest {
	return WorkerManifest{Connections: []WorkerConnection{{Name: "source", Kind: "polign", CanRead: true, Fingerprint: strings.Repeat("a", 64), Resource: strings.Repeat("c", 64)}, {Name: "sink", Kind: "polign", CanWrite: true, Fingerprint: strings.Repeat("b", 64), Resource: strings.Repeat("d", 64)}}, Pairs: []WorkerPair{{Source: "source", Sink: "sink", Dimension: 2}}}
}
func TestWorkerIdentityEnrollmentRenewalAndRevocation(t *testing.T) {
	ws, w, enrollment := workerFixture(t)
	_, token, _ := strings.Cut(enrollment, ".")
	pub, key, _ := ed25519.GenerateKey(rand.Reader)
	encoded := base64.StdEncoding.EncodeToString(pub)
	if ws.Enroll(w.ID, "wrong", encoded) == nil {
		t.Fatal("accepted wrong enrollment")
	}
	if err := ws.Enroll(w.ID, token, encoded); err != nil {
		t.Fatal(err)
	}
	if err := ws.Enroll(w.ID, token, encoded); err != nil {
		t.Fatal("lost enrollment response must be retryable", err)
	}
	other, _, _ := ed25519.GenerateKey(rand.Reader)
	if ws.Enroll(w.ID, token, base64.StdEncoding.EncodeToString(other)) == nil {
		t.Fatal("rebound enrolled key")
	}
	if _, err := ws.Get(w.ID, &Owner{ID: "tenant-b", Subject: "bob", Realm: "realm"}); err != ErrNotFound {
		t.Fatal("cross-account worker visible")
	}
	challenge, err := ws.Challenge(w.ID)
	if err != nil {
		t.Fatal(err)
	}
	signature := base64.StdEncoding.EncodeToString(ed25519.Sign(key, SessionProof(w.ID, challenge)))
	session, err := ws.Session(w.ID, challenge, signature)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = ws.Session(w.ID, challenge, signature); err == nil {
		t.Fatal("challenge replay accepted")
	}
	if _, err = ws.Authenticate(session); err != nil {
		t.Fatal(err)
	}
	next, _ := ws.Challenge(w.ID)
	replacement, err := ws.Session(w.ID, next, base64.StdEncoding.EncodeToString(ed25519.Sign(key, SessionProof(w.ID, next))))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = ws.Authenticate(session); err == nil {
		t.Fatal("old worker session survived renewal")
	}
	session = replacement
	data, err := os.ReadFile(ws.path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), token) || strings.Contains(string(data), session) || strings.Contains(string(data), base64.StdEncoding.EncodeToString(key.Seed())) {
		t.Fatal("secret persisted on control plane")
	}
	if err = ws.Revoke(w.ID, w.Owner); err != nil {
		t.Fatal(err)
	}
	if _, err = ws.Authenticate(session); err == nil {
		t.Fatal("revoked session accepted")
	}
	if _, err = ws.Challenge(w.ID); err == nil {
		t.Fatal("revoked key renewed")
	}
}
func TestWorkerLeasesIsolationRecoveryAndProgress(t *testing.T) {
	ws, w, _ := workerFixture(t)
	if err := ws.Publish(w.ID, testManifest()); err != nil {
		t.Fatal(err)
	}
	w, _ = ws.Get(w.ID, w.Owner)
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	engine := &Engine{Store: store, Workers: ws, AccountRealm: "realm"}
	spec := Spec{WorkerID: w.ID, Name: "private transfer", Source: "source", Sink: "sink", Dimension: 2, BatchSize: 2}
	j, _, err := engine.Submit(spec, "first", "alice", *w.Owner)
	if err != nil {
		t.Fatal(err)
	}
	bad := spec
	bad.Sink = "unapproved"
	if _, _, err = engine.Submit(bad, "", "alice", *w.Owner); err == nil {
		t.Fatal("unapproved destination accepted")
	}
	if _, ok, err := store.Claim(); err != nil || ok {
		t.Fatal("hosted engine claimed remote job")
	}
	a, err := store.ClaimWorker(w)
	if err != nil || a == nil {
		t.Fatal(err)
	}
	if next, err := store.ClaimWorker(w); err != nil || next != nil {
		t.Fatal("concurrent worker lease")
	}
	if err = store.Recover(); err != nil {
		t.Fatal(err)
	}
	current, _ := store.Get(j.ID)
	if current.State != "running" {
		t.Fatal("host recovery modified remote lease")
	}
	r := WorkerReport{LeaseID: a.LeaseID, Sequence: 1, State: "running", Phase: "checkpointed", Records: 2, Batches: 1, Checkpoint: true}
	outsider := w
	outsider.ID = "other"
	if _, err = store.ReportWorker(outsider, j.ID, r); err != ErrNotFound {
		t.Fatal("cross worker progress", err)
	}
	if _, err = store.ReportWorker(w, j.ID, r); err != nil {
		t.Fatal(err)
	}
	if _, err = store.ReportWorker(w, j.ID, r); err != ErrConflict {
		t.Fatal("replayed report accepted")
	}
	r.Sequence++
	r.Records = 1
	if _, err = store.ReportWorker(w, j.ID, r); err != ErrConflict {
		t.Fatal("checkpoint moved backwards")
	}
	r.Records = 2
	_, err = store.Update(j.ID, "fixture", "test", "", "", func(j *Job) error { past := time.Now().Add(-time.Minute); j.LeaseUntil = &past; return nil })
	if err != nil {
		t.Fatal(err)
	}
	b, err := store.ClaimWorker(w)
	if err != nil || b == nil || a.LeaseID == b.LeaseID {
		t.Fatal("expired lease not fenced", err)
	}
	if _, err = store.ReportWorker(w, j.ID, r); err != ErrConflict {
		t.Fatal("stale worker report accepted")
	}
	if _, err = engine.Action(j.ID, "cancel", "alice"); err != nil {
		t.Fatal(err)
	}
	r.LeaseID = b.LeaseID
	r.Sequence = 1
	r.State = "running"
	current, err = store.ReportWorker(w, j.ID, r)
	if err != nil || current.State != "cancel_requested" {
		t.Fatal("cancellation overwritten", err)
	}
	r.Sequence++
	r.State = "canceled"
	current, err = store.ReportWorker(w, j.ID, r)
	if err != nil || current.State != "canceled" || current.LeaseID != "" {
		t.Fatal("cancellation not acknowledged", err)
	}
	current, err = engine.Action(j.ID, "resume", "alice")
	if err != nil || current.Generation != 1 {
		t.Fatal("resume generation missing", err)
	}
	public, _ := json.Marshal(publicJob(current))
	if strings.Contains(string(public), a.LeaseID) || strings.Contains(string(public), strings.Repeat("a", 64)) {
		t.Fatal("lease or config fingerprint exposed")
	}
}
func TestWorkerRegistryDurableEnrollmentAndExpiry(t *testing.T) {
	dir := t.TempDir()
	ws, err := OpenWorkers(dir)
	if err != nil {
		t.Fatal(err)
	}
	v, _, err := ws.Create(nil, "local")
	if err != nil {
		t.Fatal(err)
	}
	e := ws.entries[v.ID]
	e.EnrollmentUntil = time.Now().Add(-time.Minute)
	ws.entries[v.ID] = e
	if err = ws.save(); err != nil {
		t.Fatal(err)
	}
	ws.Close()
	ws, err = OpenWorkers(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()
	if ws.List(nil)[0].Status != "enrollment expired" {
		t.Fatal("expiry not preserved")
	}
	pub, _, _ := ed25519.GenerateKey(rand.Reader)
	if ws.Enroll(v.ID, "expired", base64.StdEncoding.EncodeToString(pub)) == nil {
		t.Fatal("expired enrollment accepted")
	}
	if err = os.WriteFile(filepath.Join(dir, "workers.json"), []byte("broken"), 0600); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerAPIOwnerIsolationAndCredentialSeparation(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ws, err := OpenWorkers(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()
	engine := &Engine{Store: store, Workers: ws}
	h := AccountHandler(engine, testAccounts{})
	request := func(method, path, token, body string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, "https://transfer.test"+path, strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Authorization", "Bearer "+token)
		out := httptest.NewRecorder()
		h.ServeHTTP(out, r)
		return out
	}
	if request("POST", "/v1/workers", "", `{"name":"production"}`).Code != 401 {
		t.Fatal("anonymous enrollment creation")
	}
	response := request("POST", "/v1/workers", "alice", `{"name":"production"}`)
	if response.Code != 201 {
		t.Fatal(response.Body)
	}
	var created struct {
		Worker WorkerView `json:"worker"`
		Token  string     `json:"enrollment_token"`
	}
	if err = json.Unmarshal(response.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(request("GET", "/v1/workers", "bob", "").Body.String(), created.Worker.ID) {
		t.Fatal("worker leaked across owners")
	}
	if request("POST", "/v1/workers/"+created.Worker.ID+"/revoke", "bob", "{}").Code != 404 {
		t.Fatal("cross-owner revocation")
	}
	if request("POST", "/worker/v1/manifest", "alice", `{}`).Code != 401 {
		t.Fatal("account token accepted as worker identity")
	}
	_, token, _ := strings.Cut(created.Token, ".")
	pub, key, _ := ed25519.GenerateKey(rand.Reader)
	if err = ws.Enroll(created.Worker.ID, token, base64.StdEncoding.EncodeToString(pub)); err != nil {
		t.Fatal(err)
	}
	challenge, _ := ws.Challenge(created.Worker.ID)
	session, err := ws.Session(created.Worker.ID, challenge, base64.StdEncoding.EncodeToString(ed25519.Sign(key, SessionProof(created.Worker.ID, challenge))))
	if err != nil {
		t.Fatal(err)
	}
	if request("GET", "/v1/jobs", session, "").Code != 401 {
		t.Fatal("worker token accepted for account API")
	}
	if request("POST", "/worker/v1/manifest", session, `{"connections":[],"pairs":[],"credentials":"must-not-be-accepted"}`).Code != 400 {
		t.Fatal("unknown credential field accepted")
	}
	if request("POST", "/v1/workers/"+created.Worker.ID+"/revoke", "alice", "{}").Code != 200 {
		t.Fatal("owner could not revoke")
	}
	if request("POST", "/worker/v1/manifest", session, `{}`).Code != 401 {
		t.Fatal("revoked worker API session accepted")
	}
}
