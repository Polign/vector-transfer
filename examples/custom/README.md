# Custom connector example

`memorydb` implements source and sink methods. [main.go](main.go) registers its
factory with `app.Run`.

From the repository root:

```sh
go test -race -timeout 60s ./examples/custom/...
go run ./examples/custom serve -config examples/custom/config.json -data /tmp/vector-transfer-custom-demo
```

Submit from another terminal:

```sh
go run ./cmd/vtransfer submit -file examples/custom/job.json
```

Track the three-record transfer at http://127.0.0.1:23005. Use a fresh data
directory for each demo: database contents disappear on exit, but the journal persists.

Start with [the implementation](memorydb/memorydb.go) and
[contract tests](memorydb/memorydb_test.go). Replace map operations with your
database's scan and upsert APIs. The offset cursor requires a stable source.

[Connector author guide](../../docs/connectors.md)
