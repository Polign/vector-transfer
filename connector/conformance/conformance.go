// Package conformance provides reusable connector contract tests.
// Run these against small, isolated fixtures owned by your test. They can write
// and replace records; never point a sink fixture at a production resource.
package conformance

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"testing"
	"time"

	"github.com/anuptalwalkar/vector-transfer/connector"
)

// SourceFixture describes a fixed dataset. Open must return a fresh connector
// to the SAME unchanged dataset each time; use t.Cleanup for client cleanup.
// Want should include at least three distinct IDs and representative metadata.
type SourceFixture struct {
	Open      func(t *testing.T) connector.Source
	Want      []connector.Record
	Dimension int
	// Timeout bounds cooperative connector calls; defaults to ten seconds.
	Timeout time.Duration
	// MaxPages caps scans, including empty pages; defaults to 1000.
	MaxPages int
}

// TestSource checks bounded exhaustive scans, unique records, data preservation,
// replay of page contents, restart-safe cursors, and cancellation.
// The dataset must remain stable throughout the test. Calls that ignore context
// can still hang: also use go test -timeout to bound the entire test process.
func TestSource(t *testing.T, f SourceFixture) {
	t.Helper()
	if f.Open == nil || len(f.Want) < 3 {
		t.Fatal("source fixture requires Open and at least three records")
	}
	if f.Dimension < 1 {
		t.Fatal("source fixture requires a positive dimension")
	}
	if err := connector.Validate(f.Want, f.Dimension); err != nil {
		t.Fatal(err)
	}
	if f.Timeout == 0 {
		f.Timeout = 10 * time.Second
	}
	if f.MaxPages == 0 {
		f.MaxPages = 1000
	}
	for _, limit := range []int{1, 2, 3} {
		t.Run(fmt.Sprintf("scan_limit_%d", limit), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), f.Timeout)
			defer cancel()
			source := f.Open(t)
			cursor := ""
			seen := map[string]bool{}
			var all []connector.Record
			for n := 0; n < f.MaxPages; n++ {
				page, err := source.Read(ctx, cursor, limit)
				if err != nil {
					t.Fatal(err)
				}
				checkPage(t, page, cursor, limit, f.Dimension)
				// Re-open before replay: cursors cannot depend on client-local state.
				replayed, err := f.Open(t).Read(ctx, cursor, limit)
				if err != nil {
					t.Fatalf("reopen/replay: %v", err)
				}
				checkPage(t, replayed, cursor, limit, f.Dimension)
				if page.Done != replayed.Done || !sameRecords(page.Records, replayed.Records) {
					t.Fatal("page changed when replayed on a fresh connector")
				}
				for _, r := range page.Records {
					if seen[r.ID] {
						t.Fatalf("record %q was emitted more than once", r.ID)
					}
					seen[r.ID] = true
				}
				all = append(all, page.Records...)
				if page.Done {
					if !sameRecords(all, f.Want) {
						t.Fatal("scan did not preserve the expected IDs, vectors, and metadata")
					}
					return
				}
				cursor = page.Next
			}
			t.Fatal("scan exceeded MaxPages without reaching Done")
		})
	}
	t.Run("canceled_context", func(t *testing.T) {
		source := f.Open(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := source.Read(ctx, "", 1); !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	})
}

func checkPage(t *testing.T, p connector.Page, cursor string, limit, dimension int) {
	t.Helper()
	if len(p.Records) > limit {
		t.Fatal("page exceeds requested limit")
	}
	if !p.Done && (p.Next == "" || p.Next == cursor) {
		t.Fatal("non-final page does not advance its cursor")
	}
	if err := connector.Validate(p.Records, dimension); err != nil {
		t.Fatal(err)
	}
}

// SinkFixture opens an EMPTY isolated destination and a readback function that
// lists its complete contents. Readback must poll for visibility if the provider
// is eventually consistent, honoring ctx. Neither callback may mask duplicates.
type SinkFixture struct {
	Open      func(t *testing.T) (connector.Sink, func(context.Context) ([]connector.Record, error))
	Records   []connector.Record
	Dimension int
	Timeout   time.Duration
}

// TestSink checks exact writes, idempotent replay, replacement by ID (including
// removal of old metadata), input ownership, and pre-canceled requests.
// Provider-specific tests must additionally inject partial writes, throttling,
// unsupported data, and cancellation during an in-flight operation.
func TestSink(t *testing.T, f SinkFixture) {
	t.Helper()
	if f.Open == nil || len(f.Records) < 2 {
		t.Fatal("sink fixture requires Open and at least two records")
	}
	if f.Dimension < 1 {
		t.Fatal("sink fixture requires a positive dimension")
	}
	if err := connector.Validate(f.Records, f.Dimension); err != nil {
		t.Fatal(err)
	}
	if f.Timeout == 0 {
		f.Timeout = 10 * time.Second
	}
	t.Run("upsert_replay_replace", func(t *testing.T) {
		sink, readback := f.Open(t)
		ctx, cancel := context.WithTimeout(context.Background(), f.Timeout)
		defer cancel()
		initial, err := readback(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(initial) != 0 {
			t.Fatal("sink fixture must start empty")
		}
		want := clone(f.Records)
		for attempt := 0; attempt < 2; attempt++ {
			input := clone(want)
			if err := sink.Upsert(ctx, input); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(input, want) {
				t.Fatal("Upsert mutated caller-owned records")
			}
			// Mutating released caller buffers must not change the destination.
			input[0].Values[0]++
			for key, raw := range input[0].Metadata {
				if len(raw) > 0 {
					raw[0] = ' '
				}
				input[0].Metadata[key] = json.RawMessage(`"caller-changed"`)
			}
			got, err := readback(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if !sameRecords(got, want) {
				t.Fatal("write/replay changed data, retained caller memory, or duplicated IDs")
			}
		}
		replacement := clone(want[:1])
		replacement[0].Values[0] += 0.125
		replacement[0].Metadata = map[string]json.RawMessage{"replacement": json.RawMessage(`true`)}
		if err := sink.Upsert(ctx, replacement); err != nil {
			t.Fatal(err)
		}
		want[0] = replacement[0]
		got, err := readback(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !sameRecords(got, want) {
			t.Fatal("upsert did not replace the record and its old metadata")
		}
	})
	t.Run("canceled_context", func(t *testing.T) {
		sink, readback := f.Open(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := sink.Upsert(ctx, clone(f.Records)); !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
		check, stop := context.WithTimeout(context.Background(), f.Timeout)
		defer stop()
		got, err := readback(check)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 0 {
			t.Fatal("pre-canceled write changed the destination")
		}
	})
}

func clone(rs []connector.Record) []connector.Record {
	b, err := json.Marshal(rs)
	if err != nil {
		panic(err)
	}
	var result []connector.Record
	if err := json.Unmarshal(b, &result); err != nil {
		panic(err)
	}
	return result
}

// Compare metadata structurally, preserving arbitrary JSON numeric precision
// while treating equivalent encodings such as 1 and 1.0 as the same value.
type rational string

func normalize(v any) any {
	switch x := v.(type) {
	case json.Number:
		if n, ok := new(big.Rat).SetString(string(x)); ok {
			return rational(n.RatString())
		}
	case []any:
		for i := range x {
			x[i] = normalize(x[i])
		}
	case map[string]any:
		for k, v := range x {
			x[k] = normalize(v)
		}
	}
	return v
}

func sameRecords(a, b []connector.Record) bool {
	if len(a) != len(b) {
		return false
	}
	byID := map[string]connector.Record{}
	for _, r := range a {
		if _, ok := byID[r.ID]; ok {
			return false
		}
		byID[r.ID] = r
	}
	for _, want := range b {
		got, ok := byID[want.ID]
		if !ok || !reflect.DeepEqual(got.Values, want.Values) || len(got.Metadata) != len(want.Metadata) {
			return false
		}
		for key, w := range want.Metadata {
			g, ok := got.Metadata[key]
			if !ok {
				return false
			}
			decode := func(raw []byte) (any, error) {
				if !json.Valid(raw) {
					return nil, errors.New("invalid metadata JSON")
				}
				d := json.NewDecoder(bytes.NewReader(raw))
				d.UseNumber()
				var v any
				err := d.Decode(&v)
				return normalize(v), err
			}
			gv, ge := decode(g)
			wv, we := decode(w)
			if ge != nil || we != nil || !reflect.DeepEqual(gv, wv) {
				return false
			}
		}
		delete(byID, want.ID)
	}
	return len(byID) == 0
}
