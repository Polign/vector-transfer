package connector

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

type demoSource struct{}

func (demoSource) Read(ctx context.Context, cursor string, limit int) (Page, error) {
	start := 0
	if cursor != "" {
		var err error
		start, err = strconv.Atoi(cursor)
		if err != nil || start < 0 || start > 1000 {
			return Page{}, errors.New("invalid demo cursor")
		}
	}
	select {
	case <-ctx.Done():
		return Page{}, ctx.Err()
	case <-time.After(40 * time.Millisecond):
	}
	end := min(start+limit, 1000)
	p := Page{Next: strconv.Itoa(end), Done: end == 1000}
	for i := start; i < end; i++ {
		metadata := map[string]json.RawMessage{"sequence": json.RawMessage(strconv.Itoa(i)), "dataset": json.RawMessage(`"demo"`)}
		p.Records = append(p.Records, Record{ID: fmt.Sprintf("doc-%06d", i), Values: []float32{float32(i) / 1000, 0.25, 0.5, 0.75}, Metadata: metadata})
	}
	return p, nil
}

type demoSink struct{ directory string }

func (s demoSink) Upsert(ctx context.Context, rs []Record) error {
	for _, r := range rs {
		if err := ctx.Err(); err != nil {
			return err
		}
		b, err := json.Marshal(r)
		if err != nil {
			return err
		}
		h := sha256.Sum256([]byte(r.ID))
		path := filepath.Join(s.directory, fmt.Sprintf("%x.json", h))
		f, err := os.CreateTemp(s.directory, ".record-*")
		if err != nil {
			return err
		}
		name := f.Name()
		_, err = f.Write(b)
		if err == nil {
			err = f.Sync()
		}
		closeErr := f.Close()
		if err == nil {
			err = closeErr
		}
		if err == nil {
			err = os.Rename(name, path)
		}
		if err != nil {
			os.Remove(name)
			return err
		}
	}
	d, err := os.Open(s.directory)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func DemoRegistry(dataDir string) (Registry, error) {
	dir := filepath.Join(dataDir, "demo-vectors")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	return Registry{
		"demo-source": {Name: "demo-source", Kind: "demo", Description: "1,000 sample vectors · 4 dimensions", Fingerprint: Fingerprint("demo-source-v1"), CanRead: true, Source: demoSource{}},
		"demo-sink":   {Name: "demo-sink", Kind: "demo", Description: "Local JSON records", Fingerprint: Fingerprint("demo-sink-v1"), CanWrite: true, Sink: demoSink{dir}},
	}, nil
}
