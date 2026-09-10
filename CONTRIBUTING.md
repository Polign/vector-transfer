# Contributing connectors

Implement a source, sink, or both using the public Go API. Start with the
[author guide](docs/connectors.md), [contract](docs/connector-contract.md), and
[working example](examples/custom/README.md).

## 1. Implement and register

Use a separate package, such as `connectors/acme/`, with implementation, factory,
and test files. Depend on the public `connector` package; keep provider logic
out of the transfer engine.

Export a factory:

```go
func Open(ctx context.Context, options json.RawMessage) (connector.Adapter, error)
```

Decode typed options with `connector.DecodeOptions`. Return supported directions,
a stable `ResourceID`, a `CheckpointVersion`, and optional `Close` function.
Resolve credentials from references, not from job records.

Register it in your own executable or in `cmd/vtransfer/main.go`:

```go
err := app.Run(os.Args[1:], connector.Factories{
    "acme": acme.Open,
})
```

Registration is compiled into the binary. The factory works with both `serve`
and `worker run`. An independent Go module needs no repository changes. For a
built-in contribution, include the import and registration in your pull request.

## 2. Add setup examples

Provide a connection configuration and a job with matching names and dimensions:

```json
{
  "connections": {
    "acme-documents": {
      "kind": "acme",
      "options": {
        "endpoint": "https://YOUR-DATABASE-HOST",
        "collection": "documents",
        "api_key_env": "ACME_API_KEY"
      }
    }
  }
}
```

Document supported versions, IDs, vectors, metadata, authentication, resource
preparation, cursor lifetime, request limits, and known restrictions. Use
placeholders for endpoints and references for secrets. Update the
[provider catalog](docs/provider-catalog.md).

### Connection forms

Factory registration supports local configuration and customer workers. It does
not automatically add credential fields to the hosted setup form.

To add hosted self-service, update typed configuration and credential validation
in `connector/personal.go` or `additional.go`, register the builder in
`connector/config.go`, and add fields in `control/web/connection-fields.js`.
These fields are shared by hosted setup and the customer worker form. Update
`control/web/connection-setup.js` for provider-specific local configuration or
credential prompts. Customer setup must generate configuration in the browser;
only the worker name is submitted to the registration API.

Require verified TLS, guarded public-network dialing for every discovered host,
and no redirects or host-credential fallback. Do not accept arbitrary paths,
queries, environment references, IAM roles, or unrestricted factory options from
UI requests. Keep credentials out of bindings, jobs, cursors, errors, and audit events.

Operator connections in account mode require exact `read_subjects` and
`write_subjects` grants. Customer workers use locally approved pairs. See
[security](docs/accounts-and-security.md).

## 3. Test

Use `connector/conformance.TestSource` and/or `TestSink`. Add coverage for:

- Complete scans, empty continuation pages, and cursors reused by a fresh client.
- Idempotent writes, metadata replacement, partial writes, and request limits.
- Unsupported data, transient failures, cancellation, and concurrent calls.
- Safe handling of fake credentials and provider errors.

Keep default tests local. Make cloud tests opt-in and use isolated resources.
For Polign ingress changes, run the [integration tests](docs/provider-catalog.md#verification).

```sh
go test -race -timeout 60s ./...
go vet ./...
go build ./...
make test-ui
git diff --check
```

Run a small transfer through the registered factory and check destination records.
For worker changes, verify lease loss stops execution and local checkpoints survive restart.
For setup UI changes, run `node tests/setup-flow.browser.mjs` with a local preview
on port 23825 and an isolated Chrome debugging session on port 23816. It checks
setup, readiness, job submission, and recovery using a mocked account API.

## 4. Submit

Include the implementation, registration, setup examples, tests and results,
supported versions, limitations, and any new dependencies. Distinguish protocol
fixture tests from live database tests.

Change `CheckpointVersion` when old cursors or data interpretation become
incompatible, and document migration. Keep required `Source` and `Sink` methods
unchanged in compatible releases.

For public landing-page changes, start a fresh `serve -demo` process on port
23856 and an isolated Chrome debugging session on port 23836, then run
`node tests/landing.browser.mjs`. It verifies the public guide, platform commands,
mobile layout and a real 1,000-record demo transfer. The test closes its browser.
