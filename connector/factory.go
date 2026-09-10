package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
)

// Factory opens one configured resource. Options must be a JSON object and
// should contain credential references (for example api_key_env), not secrets.
// Factories resolve credentials and validate all provider-specific options.
// If opening fails, the factory must clean up any resources it allocated.
// On success, ownership of Adapter.Close passes to the returned Registry.
type Factory func(ctx context.Context, options json.RawMessage) (Adapter, error)

// Factories associates public kind names (for example "acme") with factories.
// Supply it to LoadConfig or app.Run before starting the service. Duplicate
// names, including names of built-in connectors, are rejected.
type Factories map[string]Factory

// Adapter is a connection to one physical collection, namespace, or index.
// Implement just Source, just Sink, or both. The loader derives capabilities
// and job fingerprints; authors do not need to construct Binding themselves.
type Adapter struct {
	Source Source
	Sink   Sink
	// ResourceID is a stable, non-secret identity, including account/cluster and
	// collection. Aliases and different credentials to the same resource must
	// return the same identity. The loader hashes it with the connector kind.
	ResourceID string
	// CheckpointVersion identifies cursor and data interpretation semantics.
	// Change it when an upgrade cannot safely resume old jobs (for example "1"
	// to "2"). It is not necessarily the connector's package release version.
	CheckpointVersion string
	Description       string
	// Close optionally releases clients/pools after all workers stop. It is
	// called at most once by Registry.Close, including configuration failures.
	Close func() error
}

// DecodeOptions decodes exactly one JSON object and rejects unknown fields.
// Pass a pointer to a provider-specific configuration struct. Validate required
// values after decoding. Errors deliberately omit the raw configuration.
func DecodeOptions(raw json.RawMessage, out any) error {
	if len(bytes.TrimSpace(raw)) == 0 || bytes.TrimSpace(raw)[0] != '{' {
		return errors.New("connector options must be a JSON object")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	d.UseNumber()
	if err := d.Decode(out); err != nil {
		return errors.New("invalid connector options or unknown fields")
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return errors.New("connector options must contain one JSON object")
	}
	return nil
}

func openCustom(ctx context.Context, name string, c Connection, factory Factory) (Binding, error) {
	// Avoid accepting old built-in fields that a custom factory cannot see.
	onlyOptions := Connection{Kind: c.Kind, ReadOnly: c.ReadOnly, WriteOnly: c.WriteOnly, ReadSubjects: c.ReadSubjects, WriteSubjects: c.WriteSubjects, Options: c.Options}
	if Fingerprint(c) != Fingerprint(onlyOptions) {
		return Binding{}, errors.New("custom connections accept kind, read_only, write_only, read_subjects, write_subjects, and options only")
	}
	var normalized map[string]any
	if err := DecodeOptions(c.Options, &normalized); err != nil {
		return Binding{}, err
	}
	adapter, err := factory(ctx, c.Options)
	if err != nil {
		return Binding{}, err
	}
	var close func() error
	if adapter.Close != nil {
		close = sync.OnceValue(adapter.Close)
	}
	fail := func(message string) (Binding, error) {
		var closeErr error
		if close != nil {
			closeErr = close()
		}
		return Binding{}, errors.Join(errors.New(message), closeErr)
	}
	if adapter.ResourceID == "" || adapter.CheckpointVersion == "" {
		return fail("factory must return resource identity and checkpoint version")
	}
	if adapter.Source == nil && adapter.Sink == nil {
		return fail("factory must provide a source or sink")
	}
	if c.ReadOnly {
		adapter.Sink = nil
	}
	if c.WriteOnly {
		adapter.Source = nil
	}
	if adapter.Source == nil && adapter.Sink == nil {
		return fail("direction restriction: read_only disables this sink-only connection")
	}
	return Binding{
		Name: name, Kind: c.Kind, Description: adapter.Description,
		Resource: Fingerprint([]string{c.Kind, adapter.ResourceID}),
		Fingerprint: Fingerprint(struct {
			Kind, Resource, Version string
			Options                 map[string]any
			ReadOnly                bool
			WriteOnly               bool `json:",omitempty"`
		}{c.Kind, adapter.ResourceID, adapter.CheckpointVersion, normalized, c.ReadOnly, c.WriteOnly}),
		CanRead: adapter.Source != nil, CanWrite: adapter.Sink != nil,
		Source: adapter.Source, Sink: adapter.Sink, close: close,
	}, nil
}

// Close releases factory-owned connections. Stop all workers before calling it.
// It is safe to call more than once. It does not close manually added bindings.
func (r Registry) Close() error {
	names := make([]string, 0, len(r))
	for name := range r {
		names = append(names, name)
	}
	sort.Strings(names)
	var errs []error
	for _, name := range names {
		if close := r[name].close; close != nil {
			if err := close(); err != nil {
				errs = append(errs, fmt.Errorf("close connection %s: %w", name, err))
			}
		}
	}
	return errors.Join(errs...)
}
