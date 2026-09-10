package connector_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/anuptalwalkar/vector-transfer/connector"
)

type emptySource struct{}

func (emptySource) Read(ctx context.Context, _ string, _ int) (connector.Page, error) {
	return connector.Page{Done: true}, ctx.Err()
}

type discardSink struct{}

func (discardSink) Upsert(ctx context.Context, _ []connector.Record) error { return ctx.Err() }
func configFile(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "connections.json")
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestEmptyConnectionRegistryForAccountOnboarding(t *testing.T) {
	r, err := connector.LoadConfig(context.Background(), configFile(t, `{"connections":{}}`))
	if err != nil || len(r) != 0 {
		t.Fatalf("empty registry: %v %v", r, err)
	}
	for _, body := range []string{`{}`, `{"connections":null}`} {
		if _, err := connector.LoadConfig(context.Background(), configFile(t, body)); err == nil {
			t.Fatal("missing connections object accepted")
		}
	}
}

func TestFactoryAccountGrantsAndDirectionRestrictions(t *testing.T) {
	factory := func(ctx context.Context, raw json.RawMessage) (connector.Adapter, error) {
		var opts struct {
			Database string `json:"database"`
		}
		if err := connector.DecodeOptions(raw, &opts); err != nil {
			return connector.Adapter{}, err
		}
		return connector.Adapter{Source: emptySource{}, Sink: discardSink{}, ResourceID: opts.Database, CheckpointVersion: "1"}, nil
	}
	load := func(extra string) connector.Binding {
		t.Helper()
		r, err := connector.LoadConfig(context.Background(), configFile(t, `{"connections":{"test":{"kind":"external","options":{"database":"docs"}`+extra+`}}}`), connector.Factories{"external": factory})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { r.Close() })
		return r["test"]
	}
	base := load("")
	granted := load(`,"read_subjects":["alice"],"write_subjects":["bob"]`)
	if base.AllowsRead("alice") || base.AllowsWrite("bob") || !granted.AllowsRead("alice") || granted.AllowsWrite("alice") || granted.AllowsRead("bob") || !granted.AllowsWrite("bob") {
		t.Fatal("directional grants incorrect")
	}
	if base.Fingerprint != granted.Fingerprint {
		t.Fatal("grant change invalidated cursor compatibility")
	}
	writeOnly := load(`,"write_only":true,"read_subjects":["alice"],"write_subjects":["bob"]`)
	if writeOnly.Source != nil || writeOnly.CanRead || writeOnly.AllowsRead("alice") || !writeOnly.AllowsWrite("bob") {
		t.Fatal("write_only not enforced")
	}
	if writeOnly.Fingerprint == base.Fingerprint || writeOnly.Resource != base.Resource {
		t.Fatal("direction restriction identity incorrect")
	}
}

func TestExternalFactoriesAndIdentity(t *testing.T) {
	version := "1"
	closed := 0
	factory := func(ctx context.Context, raw json.RawMessage) (connector.Adapter, error) {
		var opts struct {
			Cluster string      `json:"cluster"`
			Count   json.Number `json:"count"`
		}
		if err := connector.DecodeOptions(raw, &opts); err != nil {
			return connector.Adapter{}, err
		}
		if opts.Count.String() != "9007199254740993" {
			t.Fatal("options lost numeric precision")
		}
		return connector.Adapter{Source: emptySource{}, Sink: discardSink{}, ResourceID: opts.Cluster + "/docs", CheckpointVersion: version, Close: func() error { closed++; return nil }}, nil
	}
	load := func(options string, readOnly bool) connector.Registry {
		t.Helper()
		body := `{"connections":{"test":{"kind":"external","read_only":` + map[bool]string{true: "true", false: "false"}[readOnly] + `,"options":` + options + `}}}`
		r, err := connector.LoadConfig(context.Background(), configFile(t, body), connector.Factories{"external": factory})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { r.Close() })
		return r
	}
	a := load(`{"cluster":"one","count":9007199254740993}`, false)
	b := load(`{ "count":9007199254740993, "cluster":"one" }`, false)
	if a["test"].Fingerprint != b["test"].Fingerprint {
		t.Fatal("option formatting changed fingerprint")
	}
	ro := load(`{"cluster":"one","count":9007199254740993}`, true)
	if ro["test"].CanWrite || ro["test"].Sink != nil || !ro["test"].CanRead || ro["test"].Resource != a["test"].Resource {
		t.Fatal("capabilities or physical identity incorrect")
	}
	version = "2"
	upgraded := load(`{"cluster":"one","count":9007199254740993}`, false)
	if upgraded["test"].Fingerprint == a["test"].Fingerprint || upgraded["test"].Resource != a["test"].Resource {
		t.Fatal("checkpoint version did not change job identity independently of resource")
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if closed != 1 {
		t.Fatalf("close ran %d times", closed)
	}
}

func TestFactoryCleanupOnStartupFailure(t *testing.T) {
	for _, invalid := range []bool{false, true} {
		t.Run(map[bool]string{false: "factory error", true: "invalid adapter"}[invalid], func(t *testing.T) {
			closed := 0
			factory := func(_ context.Context, raw json.RawMessage) (connector.Adapter, error) {
				var opts struct {
					Fail bool `json:"fail"`
				}
				if err := connector.DecodeOptions(raw, &opts); err != nil {
					return connector.Adapter{}, err
				}
				if opts.Fail && !invalid {
					return connector.Adapter{}, errors.New("could not open")
				}
				a := connector.Adapter{Source: emptySource{}, ResourceID: "resource", CheckpointVersion: "1", Close: func() error { closed++; return nil }}
				if opts.Fail {
					a.ResourceID = ""
				}
				return a, nil
			}
			path := configFile(t, `{"connections":{"a":{"kind":"external","options":{}},"b":{"kind":"external","options":{"fail":true}}}}`)
			if _, err := connector.LoadConfig(context.Background(), path, connector.Factories{"external": factory}); err == nil {
				t.Fatal("invalid configuration accepted")
			}
			want := 1
			if invalid {
				want = 2
			}
			if closed != want {
				t.Fatalf("closed %d connections; want %d", closed, want)
			}
		})
	}
}

func TestRegistrationsAndOptionsFailClosed(t *testing.T) {
	factory := func(context.Context, json.RawMessage) (connector.Adapter, error) {
		return connector.Adapter{Source: emptySource{}, ResourceID: "one", CheckpointVersion: "1"}, nil
	}
	path := configFile(t, `{"connections":{"test":{"kind":"external","options":{}}}}`)
	for _, groups := range [][]connector.Factories{
		{{"polign": factory}}, {{"external": nil}}, {{"invalid kind": factory}}, {{"external": factory}, {"external": factory}},
	} {
		if _, err := connector.LoadConfig(context.Background(), path, groups...); err == nil {
			t.Fatal("invalid factory registration accepted")
		}
	}
	for _, body := range []string{
		`{"connections":{"test":{"kind":"external","endpoint":"https://ignored.example","options":{}}}}`,
		`{"connections":{"test":{"kind":"external","options":null}}}`,
		`{"connections":{"test":{"kind":"external","options":[]}}}`,
		`{"connections":{"test":{"kind":"not-registered","options":{}}}}`,
	} {
		if _, err := connector.LoadConfig(context.Background(), configFile(t, body), connector.Factories{"external": factory}); err == nil {
			t.Fatalf("invalid custom config accepted: %s", body)
		}
	}
	for _, raw := range []string{`{"unknown":"secret"}`, `null`, `[]`, `{} {}`, `{"known":`} {
		var opts struct {
			Known string `json:"known"`
		}
		if err := connector.DecodeOptions(json.RawMessage(raw), &opts); err == nil {
			t.Fatalf("invalid options accepted: %s", raw)
		}
	}
}

func TestSinkOnlyAndLegacyBuiltInConfig(t *testing.T) {
	factory := func(context.Context, json.RawMessage) (connector.Adapter, error) {
		return connector.Adapter{Sink: discardSink{}, ResourceID: "one", CheckpointVersion: "1"}, nil
	}
	path := configFile(t, `{"connections":{"legacy":{"kind":"polign","endpoint":"http://127.0.0.1:23000","collection":"docs"},"custom":{"kind":"external","options":{}}}}`)
	r, err := connector.LoadConfig(context.Background(), path, connector.Factories{"external": factory})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if r["custom"].CanRead || !r["custom"].CanWrite || !r["legacy"].CanRead {
		t.Fatal("directional capabilities incorrect")
	}
	path = configFile(t, `{"connections":{"test":{"kind":"external","read_only":true,"options":{}}}}`)
	if _, err := connector.LoadConfig(context.Background(), path, connector.Factories{"external": factory}); err == nil {
		t.Fatal("read_only accepted a sink-only adapter")
	}
}
