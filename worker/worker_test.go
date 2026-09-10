package worker

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anuptalwalkar/vector-transfer/connector"
	"github.com/anuptalwalkar/vector-transfer/control"
)

type fixtureSource struct {
	pause   atomic.Bool
	reads   atomic.Int32
	stopped atomic.Bool
}

func (s *fixtureSource) Read(ctx context.Context, cursor string, limit int) (connector.Page, error) {
	s.reads.Add(1)
	if cursor != "" && s.pause.Load() {
		<-ctx.Done()
		s.stopped.Store(true)
		return connector.Page{}, ctx.Err()
	}
	if cursor == "" {
		return connector.Page{Records: []connector.Record{{ID: "private-id-one", Values: []float32{1, 2}, Metadata: map[string]json.RawMessage{"private": json.RawMessage(`"customer-only"`)}}}, Next: "private-source-cursor"}, nil
	}
	if cursor != "private-source-cursor" {
		return connector.Page{}, errors.New("unexpected cursor")
	}
	return connector.Page{Records: []connector.Record{{ID: "private-id-two", Values: []float32{3, 4}}}, Done: true}, nil
}

type fixtureSink struct {
	mu      sync.Mutex
	records map[string]connector.Record
	writes  int
}

func (s *fixtureSink) Upsert(ctx context.Context, rs []connector.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range rs {
		s.records[r.ID] = r
	}
	s.writes++
	return nil
}
func eventually(t *testing.T, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(12 * time.Second)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("condition not reached")
}
func TestCustomerWorkerEndToEndRestartAndPrivacy(t *testing.T) {
	cpdir := t.TempDir()
	store, err := control.OpenStore(cpdir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ws, err := control.OpenWorkers(filepath.Join(cpdir, "workers"))
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()
	engine := &control.Engine{Store: store, Workers: ws}
	handler := control.Handler(engine, "")
	var failProgress atomic.Bool
	var intercepted atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/progress") && failProgress.Load() {
			intercepted.Store(true)
			w.WriteHeader(503)
			return
		}
		handler.ServeHTTP(w, r)
	}))
	defer server.Close()
	v, enrollment, err := ws.Create(nil, "customer-network")
	if err != nil {
		t.Fatal(err)
	}
	data := t.TempDir()
	client, err := Open(data)
	if err != nil {
		t.Fatal(err)
	}
	if err = client.Enroll(context.Background(), server.URL, enrollment, true); err != nil {
		t.Fatal(err)
	}
	source := &fixtureSource{}
	source.pause.Store(true)
	sink := &fixtureSink{records: map[string]connector.Record{}}
	registry := connector.Registry{"source": {Name: "source", Kind: "polign", Fingerprint: "local-source-configuration", Resource: "source-resource", Source: source}, "sink": {Name: "sink", Kind: "polign", Fingerprint: "local-sink-configuration", Resource: "sink-resource", Sink: sink}}
	pairs := []control.WorkerPair{{Source: "source", Sink: "sink", Dimension: 2}}
	manifest, err := client.Manifest(registry, pairs)
	if err != nil {
		t.Fatal(err)
	}
	if err = ws.Publish(v.ID, manifest); err != nil {
		t.Fatal(err)
	}
	job, _, err := engine.Submit(control.Spec{WorkerID: v.ID, Name: "Private transfer", Source: "source", Sink: "sink", Dimension: 2, BatchSize: 1}, "", "test")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	failProgress.Store(true)
	go func() { done <- client.Run(ctx, registry, pairs, true) }()
	eventually(t, func() bool { return intercepted.Load() })
	eventually(t, func() bool { return source.stopped.Load() })
	cancel()
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	client.Close()
	localBytes, err := os.ReadFile(filepath.Join(data, "jobs", "journal.jsonl"))
	if err != nil || !strings.Contains(string(localBytes), "private-source-cursor") {
		t.Fatal("local checkpoint not persisted", err)
	}
	// Accelerate lease expiry after a lost progress response and a process restart.
	_, err = store.Update(job.ID, "fixture", "test", "", "", func(j *control.Job) error { past := time.Now().Add(-time.Minute); j.LeaseUntil = &past; return nil })
	if err != nil {
		t.Fatal(err)
	}
	client, err = Open(data)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	source.pause.Store(false)
	failProgress.Store(false)
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	go func() { done <- client.Run(ctx, registry, pairs, true) }()
	eventually(t, func() bool { j, _ := store.Get(job.ID); return j.State == "succeeded" })
	cancel()
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	sink.mu.Lock()
	if len(sink.records) != 2 || sink.writes != 2 {
		t.Fatalf("restart re-read acknowledged batch: records=%d writes=%d", len(sink.records), sink.writes)
	}
	sink.mu.Unlock()
	cpBytes, err := os.ReadFile(filepath.Join(cpdir, "journal.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	for _, private := range []string{"private-source-cursor", "private-id-one", "customer-only", "local-source-configuration"} {
		if strings.Contains(string(cpBytes), private) {
			t.Fatal("private data reached control plane:", private)
		}
	}
	j, _ := store.Get(job.ID)
	if j.Cursor != "" || j.Records != 2 || j.Batches != 2 {
		t.Fatal("invalid remote progress", j)
	}
}
func TestWorkerRejectsRemotePolicyAndMissingCheckpoint(t *testing.T) {
	dir := t.TempDir()
	c, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err = Open(dir); err == nil {
		t.Fatal("two worker processes share data directory")
	}
	for _, u := range []string{"http://example.com", "https://user:secret@example.com", "https://example.com/path", "https://example.com?secret=1", "http://localhost"} {
		if _, err = ValidateURL(u, false); err == nil {
			t.Fatal("unsafe control plane URL", u)
		}
	}
	s, err := control.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	a := control.WorkerAssignment{ID: strings.Repeat("a", 32), Records: 10, Checkpoint: true}
	if _, err = s.PrepareWorkerJob(a, "source", "sink", "resource"); err == nil {
		t.Fatal("missing checkpoint silently restarted")
	}
	m := control.WorkerManifest{Connections: []control.WorkerConnection{{Name: "source", CanRead: true}, {Name: "sink", CanWrite: true}}, Pairs: []control.WorkerPair{{Source: "source", Sink: "sink", Dimension: 2}}}
	for _, spec := range []control.Spec{{Source: "source", Sink: "attacker", Dimension: 2}, {Source: "source", Sink: "sink", Dimension: 3}} {
		if _, _, err = (control.Worker{Manifest: m}).Connections(spec); err == nil {
			t.Fatal("unapproved job allowed")
		}
	}
}
