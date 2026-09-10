package control

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Polign/vector-transfer/connector"
)

type sourceFunc func(context.Context, string, int) (connector.Page, error)

func (f sourceFunc) Read(c context.Context, s string, n int) (connector.Page, error) {
	return f(c, s, n)
}

type sinkFunc func(context.Context, []connector.Record) error

func (f sinkFunc) Upsert(c context.Context, rs []connector.Record) error { return f(c, rs) }

func record(id string) connector.Record {
	return connector.Record{ID: id, Values: []float32{0.25, 0.75}, Metadata: map[string]json.RawMessage{"large": json.RawMessage(`18446744073709551615`)}}
}
func spec() Spec {
	return Spec{Name: "test", Source: "source", Sink: "sink", Dimension: 2, BatchSize: 2, MaxAttempts: 2}
}
func testEngine(t *testing.T, src connector.Source, dst connector.Sink) *Engine {
	t.Helper()
	s, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return &Engine{Store: s, Registry: connector.Registry{
		"source": {Name: "source", Fingerprint: "source-v1", Source: src},
		"sink":   {Name: "sink", Fingerprint: "sink-v1", Sink: dst},
	}}
}
func submitClaim(t *testing.T, e *Engine) Job {
	t.Helper()
	if _, _, err := e.Submit(spec(), "", "tester"); err != nil {
		t.Fatal(err)
	}
	j, ok, err := e.Store.Claim()
	if err != nil || !ok {
		t.Fatalf("claim: %v %v", ok, err)
	}
	return j
}

func TestRetryAfterPartialWriteDoesNotAdvanceCheckpoint(t *testing.T) {
	stored := map[string]connector.Record{}
	writes := 0
	src := sourceFunc(func(context.Context, string, int) (connector.Page, error) {
		return connector.Page{Records: []connector.Record{record("a"), record("b")}, Done: true}, nil
	})
	var e *Engine
	var job Job
	dst := sinkFunc(func(_ context.Context, rs []connector.Record) error {
		writes++
		current, _ := e.Store.Get(job.ID)
		if current.Records != 0 || current.Cursor != "" {
			t.Fatal("checkpoint advanced before acknowledgement")
		}
		stored[rs[0].ID] = rs[0]
		if writes == 1 {
			return &connector.Transient{Err: errors.New("temporary failure after partial write")}
		}
		for _, r := range rs {
			stored[r.ID] = r
		}
		return nil
	})
	e = testEngine(t, src, dst)
	job = submitClaim(t, e)
	if err := e.execute(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	got, _ := e.Store.Get(job.ID)
	if got.State != "succeeded" || got.Records != 2 || got.Retries != 1 || writes != 2 || len(stored) != 2 {
		t.Fatalf("unexpected result: %+v, writes=%d", got, writes)
	}
	events, err := e.Store.Events(job.ID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	retryEvents := 0
	for _, event := range events {
		if event.Kind == "retry" {
			retryEvents++
		}
	}
	if retryEvents != 1 || events[len(events)-1].BatchSHA256 == "" {
		t.Fatalf("missing audit evidence: %+v", events)
	}
}

func TestResumeAfterFailedPageAndRestart(t *testing.T) {
	var cursors []string
	fail := true
	stored := map[string]connector.Record{}
	src := sourceFunc(func(_ context.Context, cursor string, _ int) (connector.Page, error) {
		cursors = append(cursors, cursor)
		if cursor == "" {
			return connector.Page{Records: []connector.Record{record("a"), record("b")}, Next: "page-2"}, nil
		}
		if cursor != "page-2" {
			t.Fatalf("unexpected cursor %q", cursor)
		}
		return connector.Page{Records: []connector.Record{record("c")}, Done: true}, nil
	})
	dst := sinkFunc(func(_ context.Context, rs []connector.Record) error {
		if rs[0].ID == "c" && fail {
			return errors.New("configuration failure")
		}
		for _, r := range rs {
			stored[r.ID] = r
		}
		return nil
	})
	e := testEngine(t, src, dst)
	job := submitClaim(t, e)
	if err := e.execute(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	got, _ := e.Store.Get(job.ID)
	if got.State != "failed" || got.Cursor != "page-2" || got.Records != 2 {
		t.Fatalf("unsafe checkpoint: %+v", got)
	}
	dir := filepath.Dir(e.Store.file.Name())
	e.Store.Close()
	reopened, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	e.Store = reopened
	fail = false
	if _, err := e.Action(job.ID, "resume", "tester"); err != nil {
		t.Fatal(err)
	}
	job, ok, err := e.Store.Claim()
	if err != nil || !ok {
		t.Fatal("resume not queued")
	}
	if err := e.execute(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	got, _ = e.Store.Get(job.ID)
	if got.State != "succeeded" || got.Records != 3 || len(stored) != 3 || strings.Join(cursors, ",") != ",page-2,page-2" {
		t.Fatalf("resume result: %+v cursors=%v", got, cursors)
	}
}

func TestCrashAfterSinkWriteReplaysUncheckpointedPage(t *testing.T) {
	stored := map[string]connector.Record{}
	writes := 0
	src := sourceFunc(func(context.Context, string, int) (connector.Page, error) {
		return connector.Page{Records: []connector.Record{record("a")}, Done: true}, nil
	})
	dst := sinkFunc(func(_ context.Context, rs []connector.Record) error {
		writes++
		for _, r := range rs {
			stored[r.ID] = r
		}
		return nil
	})
	e := testEngine(t, src, dst)
	job := submitClaim(t, e)
	// The sink committed, then the process died before writing a checkpoint.
	if err := dst.Upsert(context.Background(), []connector.Record{record("a")}); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Dir(e.Store.file.Name())
	e.Store.Close()
	reopened, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	e.Store = reopened
	if err := e.Store.Recover(); err != nil {
		t.Fatal(err)
	}
	job, ok, err := e.Store.Claim()
	if err != nil || !ok {
		t.Fatal("interrupted job was not recovered")
	}
	if err := e.execute(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	got, _ := e.Store.Get(job.ID)
	if got.State != "succeeded" || got.Records != 1 || writes != 2 || len(stored) != 1 || got.Runs != 2 {
		t.Fatalf("unsafe recovery: %+v writes=%d", got, writes)
	}
}

func TestEmptyPageWithContinuationAndInvalidPagination(t *testing.T) {
	for _, stuck := range []bool{false, true} {
		t.Run(map[bool]string{false: "empty continuation", true: "stuck cursor"}[stuck], func(t *testing.T) {
			reads, writes := 0, 0
			src := sourceFunc(func(_ context.Context, cursor string, _ int) (connector.Page, error) {
				reads++
				if cursor == "" {
					return connector.Page{Next: "next"}, nil
				}
				if stuck {
					return connector.Page{Next: "next"}, nil
				}
				return connector.Page{Records: []connector.Record{record("a")}, Done: true}, nil
			})
			e := testEngine(t, src, sinkFunc(func(context.Context, []connector.Record) error { writes++; return nil }))
			j := submitClaim(t, e)
			if err := e.execute(context.Background(), j); err != nil {
				t.Fatal(err)
			}
			got, _ := e.Store.Get(j.ID)
			if stuck {
				if got.State != "failed" || writes != 0 {
					t.Fatalf("stuck pagination accepted: %+v", got)
				}
			} else if got.State != "succeeded" || writes != 1 || reads != 2 || got.Records != 1 {
				t.Fatalf("empty page lost records: %+v", got)
			}
		})
	}
}

func TestCancelDuringFinalBatchHasAccurateCountAndAudit(t *testing.T) {
	var e *Engine
	var j Job
	reads := 0
	src := sourceFunc(func(context.Context, string, int) (connector.Page, error) {
		reads++
		return connector.Page{Records: []connector.Record{record("a")}, Done: true}, nil
	})
	e = testEngine(t, src, sinkFunc(func(context.Context, []connector.Record) error {
		_, err := e.Action(j.ID, "cancel", "tester")
		return err
	}))
	j = submitClaim(t, e)
	if err := e.execute(context.Background(), j); err != nil {
		t.Fatal(err)
	}
	got, _ := e.Store.Get(j.ID)
	if got.State != "canceled" || got.Records != 1 {
		t.Fatalf("cancellation lost acknowledgement: %+v", got)
	}
	events, _ := e.Store.Events(j.ID, 0, 100)
	if events[len(events)-1].Kind != "canceled" {
		t.Fatal("audit contradicts canceled job state")
	}
	if _, err := e.Action(j.ID, "resume", "tester"); err != nil {
		t.Fatal(err)
	}
	resumed, ok, err := e.Store.Claim()
	if err != nil || !ok {
		t.Fatal("resume was not queued")
	}
	if err := e.execute(context.Background(), resumed); err != nil {
		t.Fatal(err)
	}
	got, _ = e.Store.Get(j.ID)
	if reads != 1 || got.Records != 1 || got.State != "succeeded" {
		t.Fatalf("resume replayed the exhausted source: %+v, reads=%d", got, reads)
	}
}

func TestDimensionMismatchFailsBeforeWrite(t *testing.T) {
	r := record("a")
	r.Values = []float32{1}
	e := testEngine(t, sourceFunc(func(context.Context, string, int) (connector.Page, error) {
		return connector.Page{Records: []connector.Record{r}, Done: true}, nil
	}), sinkFunc(func(context.Context, []connector.Record) error { t.Fatal("wrote incompatible vector"); return nil }))
	j := submitClaim(t, e)
	if err := e.execute(context.Background(), j); err != nil {
		t.Fatal(err)
	}
	got, _ := e.Store.Get(j.ID)
	if got.State != "failed" || got.Records != 0 {
		t.Fatalf("invalid vector was not rejected: %+v", got)
	}
}

func TestConnectionChangePreventsResumeWrites(t *testing.T) {
	e := testEngine(t, sourceFunc(func(context.Context, string, int) (connector.Page, error) {
		t.Fatal("read changed source")
		return connector.Page{}, nil
	}), sinkFunc(func(context.Context, []connector.Record) error { return nil }))
	j := submitClaim(t, e)
	binding := e.Registry["source"]
	binding.Fingerprint = "different"
	e.Registry["source"] = binding
	if err := e.execute(context.Background(), j); err != nil {
		t.Fatal(err)
	}
	got, _ := e.Store.Get(j.ID)
	if got.State != "failed" {
		t.Fatal("configuration drift was accepted")
	}
}

func TestJournalTailRecoveryIntegrityAndExclusiveLock(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	j, _, err := s.Create(spec(), "src", "dst", "dst", "key", "tester")
	if err != nil {
		t.Fatal(err)
	}
	if other, err := OpenStore(dir); err == nil {
		other.Close()
		t.Fatal("two stores acquired the same directory")
	}
	s.Close()
	path := filepath.Join(dir, "journal.jsonl")
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString(`{"interrupted":`)
	f.Close()
	s, err = OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.Get(j.ID)
	if err != nil || got.Spec != j.Spec {
		t.Fatal("tail recovery lost committed data")
	}
	s.Close()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	b = []byte(strings.Replace(string(b), `"queued"`, `"failed"`, 1))
	if err := os.WriteFile(path, b, 0600); err != nil {
		t.Fatal(err)
	}
	if s, err := OpenStore(dir); err == nil {
		s.Close()
		t.Fatal("modified journal passed integrity check")
	}
}

func TestFailedJournalWritePoisonsStore(t *testing.T) {
	s, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s.file.Close()
	if _, _, err := s.Create(spec(), "src", "dst", "dst", "", "tester"); err == nil {
		t.Fatal("closed journal accepted job")
	}
	if s.Health() == nil {
		t.Fatal("journal failure not surfaced")
	}
	if _, _, err := s.Claim(); err == nil {
		t.Fatal("worker continued after journal failure")
	}
	if len(s.List()) != 0 {
		t.Fatal("uncommitted job became visible")
	}
}

func TestClaimSerializesDestinationAliases(t *testing.T) {
	s, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i, resource := range []string{"same-resource", "same-resource", "other-resource"} {
		sp := spec()
		sp.Name = string(rune('a' + i))
		if _, _, err := s.Create(sp, "src", sp.Name, resource, "", "tester"); err != nil {
			t.Fatal(err)
		}
	}
	first, ok, err := s.Claim()
	if err != nil || !ok {
		t.Fatal("first claim failed")
	}
	second, ok, err := s.Claim()
	if err != nil || !ok || first.SinkResource == second.SinkResource {
		t.Fatal("destination was concurrently claimed")
	}
	if _, ok, err := s.Claim(); err != nil || ok {
		t.Fatal("busy destination was claimed")
	}
}
