// Package memorydb is a complete teaching connector. It demonstrates the public
// contract with an in-memory database; its data does not survive process exit.
package memorydb

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"sync"

	"github.com/Polign/vector-transfer/connector"
)

// DB stores one configured resource. Real connectors replace the map with their
// database's client, exhaustive scan, and acknowledged upsert APIs.
type DB struct {
	mu      sync.RWMutex
	records map[string]connector.Record
}

var _ connector.Source = (*DB)(nil)
var _ connector.Sink = (*DB)(nil)

func New(records []connector.Record) *DB {
	db := &DB{records: map[string]connector.Record{}}
	for _, r := range records {
		db.records[r.ID] = copyRecord(r)
	}
	return db
}

// Read uses a versioned offset into sorted IDs. This is valid only while the
// source stays unchanged. A production connector should prefer native cursors.
func (db *DB) Read(ctx context.Context, cursor string, limit int) (connector.Page, error) {
	if err := ctx.Err(); err != nil {
		return connector.Page{}, err
	}
	if limit < 1 {
		return connector.Page{}, errors.New("limit must be positive")
	}
	var position struct {
		Version int `json:"v"`
		Offset  int `json:"offset"`
	}
	if cursor != "" {
		if err := connector.DecodeOptions(json.RawMessage(cursor), &position); err != nil || position.Version != 1 || position.Offset < 0 {
			return connector.Page{}, errors.New("invalid memorydb cursor")
		}
	}
	rs, err := db.Records(ctx)
	if err != nil {
		return connector.Page{}, err
	}
	if position.Offset > len(rs) {
		return connector.Page{}, errors.New("cursor exceeds source dataset")
	}
	end := position.Offset + min(limit, len(rs)-position.Offset)
	page := connector.Page{Records: rs[position.Offset:end], Done: end == len(rs)}
	if !page.Done {
		page.Next = `{"v":1,"offset":` + strconv.Itoa(end) + `}`
	}
	return page, nil
}

func (db *DB) Upsert(ctx context.Context, records []connector.Record) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	for _, r := range records {
		if err := ctx.Err(); err != nil {
			return err
		}
		db.records[r.ID] = copyRecord(r) // Replaces metadata as well as the vector.
	}
	return nil
}

// Records is test readback, not part of the connector interface.
func (db *DB) Records(ctx context.Context) ([]connector.Record, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	rs := make([]connector.Record, 0, len(db.records))
	for _, r := range db.records {
		rs = append(rs, copyRecord(r))
	}
	sort.Slice(rs, func(i, j int) bool { return rs[i].ID < rs[j].ID })
	return rs, nil
}

func copyRecord(r connector.Record) connector.Record {
	r.Values = append([]float32(nil), r.Values...)
	metadata := make(map[string]json.RawMessage, len(r.Metadata))
	for k, v := range r.Metadata {
		metadata[k] = append(json.RawMessage(nil), v...)
	}
	r.Metadata = metadata
	return r
}

// Factory creates a factory backed by a set of shared in-memory databases.
// Register the returned function once per application so aliases share data.
func Factory() connector.Factory {
	var mu sync.Mutex
	dbs := map[string]*DB{}
	return func(ctx context.Context, raw json.RawMessage) (connector.Adapter, error) {
		if err := ctx.Err(); err != nil {
			return connector.Adapter{}, err
		}
		var options struct {
			Database string `json:"database"`
		}
		if err := connector.DecodeOptions(raw, &options); err != nil {
			return connector.Adapter{}, err
		}
		if options.Database == "" {
			return connector.Adapter{}, errors.New("database is required")
		}
		mu.Lock()
		defer mu.Unlock()
		db := dbs[options.Database]
		if db == nil {
			var seed []connector.Record
			if options.Database == "sample" {
				seed = SampleRecords()
			}
			db = New(seed)
			dbs[options.Database] = db
		}
		return connector.Adapter{Source: db, Sink: db, ResourceID: options.Database, CheckpointVersion: "1", Description: "In-memory example: " + options.Database}, nil
	}
}

func SampleRecords() []connector.Record {
	return []connector.Record{
		{ID: "a", Values: []float32{0.25, 0.75}, Metadata: map[string]json.RawMessage{"title": json.RawMessage(`"First document"`), "count": json.RawMessage(`9007199254740993`)}},
		{ID: "b", Values: []float32{0.5, 0.5}, Metadata: map[string]json.RawMessage{"active": json.RawMessage(`true`)}},
		{ID: "c", Values: []float32{0.75, 0.25}, Metadata: map[string]json.RawMessage{"tags": json.RawMessage(`["example","vector"]`)}},
	}
}
