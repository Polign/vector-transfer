package control

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/Polign/vector-transfer/connector"
)

func TestRealPolignCheckpointRecovery(t *testing.T) {
	base := os.Getenv("VECTOR_TRANSFER_TEST_POLIGN_URL")
	if base == "" {
		t.Skip("set isolated Polign fixture URL")
	}
	sink := connector.NewPolign(base, "transfer-checkpoint-recovery", "")
	fail := true
	source := sourceFunc(func(_ context.Context, cursor string, _ int) (connector.Page, error) {
		if cursor == "" {
			return connector.Page{Records: []connector.Record{record("a"), record("b")}, Next: "last-page"}, nil
		}
		return connector.Page{Records: []connector.Record{record("c")}, Done: true}, nil
	})
	destination := sinkFunc(func(ctx context.Context, rs []connector.Record) error {
		if err := sink.Upsert(ctx, rs); err != nil {
			return err
		}
		if rs[0].ID == "c" && fail {
			return errors.New("fixture: acknowledgement lost after actual write")
		}
		return nil
	})
	e := testEngine(t, source, destination)
	job := submitClaim(t, e)
	if err := e.execute(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	saved, _ := e.Store.Get(job.ID)
	if saved.State != "failed" || saved.Records != 2 || saved.Cursor != "last-page" {
		t.Fatal("unacknowledged write advanced checkpoint")
	}
	dir := filepath.Dir(e.Store.file.Name())
	e.Store.Close()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	e.Store = store
	fail = false
	if _, err := e.Action(job.ID, "resume", "tester"); err != nil {
		t.Fatal(err)
	}
	claimed, ok, err := store.Claim()
	if err != nil || !ok {
		t.Fatal("resume failed")
	}
	if err := e.execute(context.Background(), claimed); err != nil {
		t.Fatal(err)
	}
	saved, _ = store.Get(job.ID)
	page, err := sink.Read(context.Background(), "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if saved.State != "succeeded" || saved.Records != 3 || len(page.Records) != 3 || !page.Done {
		t.Fatal("real Polign recovery duplicated or lost records")
	}
}
