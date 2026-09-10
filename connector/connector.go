package connector

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
)

// Record is one dense embedding and its metadata. IDs must be nonempty, stable,
// and unique within the configured resource. Preserve IDs without lossy coercion.
// Values contains finite float32 values of the job's declared dimension.
// Metadata stays raw JSON so large integers do not pass through float64.
type Record struct {
	ID       string                     `json:"id"`
	Values   []float32                  `json:"values"`
	Metadata map[string]json.RawMessage `json:"metadata,omitempty"`
}

// Page is a bounded portion of an exhaustive source scan, not search results.
// An empty Records slice does not mean the scan has finished: consult Done.
type Page struct {
	Records []Record
	Next    string // Opaque, durable cursor for the NEXT page.
	Done    bool   // Explicit: an empty page may still have a continuation token.
}

// Source reads a stable dataset in pages. Implementations must be safe for
// concurrent calls by independent jobs; keep scan position in the cursor,
// not mutable fields on the connector. Returned records belong to the caller.
type Source interface {
	// Read returns at most limit records, starting at cursor ("" starts a scan).
	// If !Done, Next must be nonempty and differ from cursor. Cursors must work
	// on a fresh connector after a process restart, against the same dataset.
	// Honor ctx cancellation. On error, the engine ignores the entire Page and
	// may retry the same cursor. Never silently skip records or unsupported data.
	Read(ctx context.Context, cursor string, limit int) (Page, error)
}

// Sink writes records by ID. Implementations must be safe for concurrent use
// and must not mutate or retain the caller's records after Upsert returns.
type Sink interface {
	// Upsert must be safe to repeat by record ID. Return nil only when ALL
	// records are acknowledged. A partial write must return an error.
	// Repeated writes must replace the same records without accumulating copies.
	// Honor ctx cancellation; a canceled/failed write may have partial effects.
	// Reject unsupported data instead of truncating or dropping it. The engine
	// calls Upsert only for nonempty batches and owns checkpointing and retries.
	Upsert(ctx context.Context, records []Record) error
}

// Binding describes a configured collection/index/namespace, not a provider
// account. Fingerprint changes whenever its non-secret configuration changes.
type Binding struct {
	Label         string   `json:"label,omitempty"`
	Personal      bool     `json:"personal,omitempty"`
	ReadSubjects  []string `json:"-"`
	WriteSubjects []string `json:"-"`
	Name          string   `json:"name"`
	Kind          string   `json:"kind"`
	Description   string   `json:"description"`
	Fingerprint   string   `json:"fingerprint"`
	Resource      string   `json:"resource"`
	CanRead       bool     `json:"can_read"`
	CanWrite      bool     `json:"can_write"`
	Source        Source   `json:"-"`
	Sink          Sink     `json:"-"`
	close         func() error
}

type Registry map[string]Binding

func (r Registry) List() []Binding {
	list := make([]Binding, 0, len(r))
	for _, b := range r {
		list = append(list, b)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })
	return list
}

func Fingerprint(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	} // Only used with JSON configuration structs.
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func Validate(records []Record, dimension int) error {
	seen := make(map[string]bool, len(records))
	for _, r := range records {
		if r.ID == "" {
			return errors.New("source returned an empty record ID")
		}
		if seen[r.ID] {
			return errors.New("source page contains duplicate record IDs")
		}
		seen[r.ID] = true
		if len(r.Values) != dimension {
			return fmt.Errorf("vector dimension %d does not match job dimension %d", len(r.Values), dimension)
		}
		for _, v := range r.Values {
			if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
				return errors.New("vector contains a non-finite value")
			}
		}
		for _, v := range r.Metadata {
			if !json.Valid(v) {
				return errors.New("invalid metadata JSON")
			}
		}
	}
	return nil
}

// Transient classifies errors safe to retry. Other errors fail the job, which
// an operator can resume from its last acknowledged checkpoint after repair.
type Transient struct{ Err error }

func (e *Transient) Error() string { return e.Err.Error() }
func (e *Transient) Unwrap() error { return e.Err }
func Retryable(err error) bool     { var e *Transient; return errors.As(err, &e) }

// AllowsRead and AllowsWrite are fail-closed account grants, independent of provider credentials.
func (b Binding) AllowsRead(subject string) bool {
	return b.Source != nil && containsSubject(b.ReadSubjects, subject)
}
func (b Binding) AllowsWrite(subject string) bool {
	return b.Sink != nil && containsSubject(b.WriteSubjects, subject)
}
func containsSubject(subjects []string, subject string) bool {
	if subject == "" {
		return false
	}
	for _, allowed := range subjects {
		if allowed == subject {
			return true
		}
	}
	return false
}
