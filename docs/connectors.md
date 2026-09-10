# Build a connector

Implement one method per direction. Vector Transfer handles scheduling, retries,
checkpoints, progress, and audit records. Your connector handles database access.

## 1. Implement reads and writes

```go
type Source interface {
    Read(ctx context.Context, cursor string, limit int) (connector.Page, error)
}

type Sink interface {
    Upsert(ctx context.Context, records []connector.Record) error
}
```

Both use:

```go
type Record struct {
    ID       string
    Values   []float32
    Metadata map[string]json.RawMessage
}

type Page struct {
    Records []Record
    Next    string
    Done    bool
}
```

| Operation | Requirement |
|---|---|
| `Read(ctx, "", 100)` | Start a complete scan; return at most 100 records |
| `Read(ctx, page.Next, 100)` | Continue after the saved page, including after reopening the client |
| `Upsert(ctx, records)` | Replace records by ID; succeed only after all writes are acknowledged |

Use scan or export APIs, not nearest-neighbor search. Keep position in the cursor.
Use `Done` for completion; short or empty pages can have more data. Preserve IDs,
vectors, and metadata, and reject unsupported values. Honor cancellation and
support concurrent calls.

[Full contract](connector-contract.md) · [Implementation example](../examples/custom/memorydb/memorydb.go)

## 2. Export a factory

```go
type Factory func(context.Context, json.RawMessage) (connector.Adapter, error)
```

Use `connector.DecodeOptions` to decode a typed options struct. Validate settings,
resolve credential references, open the client, then return:

```go
return connector.Adapter{
    Source:            client, // Omit for sink-only support.
    Sink:              client, // Omit for source-only support.
    ResourceID:        clusterID + "/" + collectionID,
    CheckpointVersion: "1",
    Description:       collectionID,
    Close:             client.Close, // Optional func() error.
}, nil
```

`ResourceID` must identify the account/cluster and resource independently of
aliases and credentials. It prevents self-transfers and lets hosted workers
serialize writes within their process. It does not lock destinations across workers.

Change `CheckpointVersion` when old cursors or data interpretation become unsafe.
The loader derives fingerprints from kind, options, resource identity, and version;
you do not need to manage `Binding` fields.

Keep secrets out of options: accept references such as `api_key_env` and resolve
them in the factory. Clean up after a failed open. After success, the registry
owns `Close` and calls it at most once after workers stop or configuration loading fails.

## 3. Register and configure

Register your package in a custom executable:

```go
func main() {
    if err := app.Run(os.Args[1:], connector.Factories{
        "acme": acme.Open,
    }); err != nil {
        log.Fatal(err)
    }
}
```

Import your connector package, `app`, `connector`, `log`, and `os`. See the
[complete main package](../examples/custom/main.go). Built-in providers remain
available. Duplicate kind names are rejected.

Custom provider settings belong under `options`:

```json
{
  "connections": {
    "source": {
      "kind": "acme",
      "read_only": true,
      "options": {"database": "sample", "api_key_env": "SOURCE_KEY"}
    },
    "destination": {
      "kind": "acme",
      "write_only": true,
      "options": {"database": "destination", "api_key_env": "DESTINATION_KEY"}
    }
  }
}
```

Built-in providers retain flat configuration fields. Account-mode operator
connections also need `read_subjects` and `write_subjects`. Customer workers need
[approved pairs](customer-workers.md#configure-connections). Factories receive
options, not Polign identity or grants.

The custom binary supports `serve` and `worker run`. Registration is compiled in;
installing a package does not update an existing binary. The hosted credential
form needs [separate integration](../CONTRIBUTING.md#hosted-connection-form).

For embedding, use `connector.LoadConfig(ctx, path, factories)` and close the
registry after execution stops.

## 4. Run an example

From the repository root:

```sh
go run ./examples/custom serve \
  -config examples/custom/config.json \
  -data /tmp/vector-transfer-custom-demo
```

Submit from another terminal:

```sh
go run ./cmd/vtransfer submit -file examples/custom/job.json
```

The in-memory example transfers three records in two pages. Use a fresh data
directory for each run: database contents disappear on exit, but the job journal persists.

## 5. Test and distribute

Use `connector/conformance.TestSource` and `TestSink`; the
[example tests](../examples/custom/memorydb/memorydb_test.go) show the fixtures.
Source fixtures must reopen the same stable dataset. Sink fixtures need an empty
destination and independent readback. Register cleanup with `t.Cleanup`.

The suites check bounded scans, completion, exact data, cursor replay, idempotent
replacement, buffer ownership, and cancellation. Add provider-specific coverage
for partial writes, request limits, transient errors, malformed cursors, unsupported
data, and concurrent calls.

```sh
go test -race -timeout 60s ./examples/custom/...
```

Ship the factory, tests, configuration, supported versions, and limitations.
Pin a tested Vector Transfer release or commit. Use a local module `replace`
directive while developing against an unpublished checkout.

Follow [CONTRIBUTING.md](../CONTRIBUTING.md) for built-in submissions. Revision 1
covers Go connectors compiled into the executable; it does not define dynamic
plugins or a cross-language connector protocol.
