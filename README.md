# Vector Transfer

Move dense embeddings and metadata between vector databases. Submit jobs through
the UI or CLI, track progress, and resume interrupted transfers from checkpoints.

## Start a transfer

Sign in at [transfer.polign.com](https://transfer.polign.com) and choose where it runs:

| Mode | Setup | Data access |
|---|---|---|
| **Customer worker — recommended** | Open [Create connection](https://transfer.polign.com/connections/new), run the setup script, and continue when your worker connects | Credentials, vectors, and checkpoints stay in your infrastructure |
| **Polign hosted** | Select **Polign hosted** in **New transfer**, then enter connection settings | Polign stores credentials encrypted and processes the vectors |

Before submitting:

- Prepare the destination for the source dimensions, IDs, and metadata types.
- Keep the source unchanged during the transfer and any resume.
- Check existing destination IDs: upserts replace matching records.

Transfers leave source data in place. They do not regenerate embeddings, copy
index definitions, or convert incompatible metadata.

[Connection setup](docs/transfer-setup.md) · [Supported databases](docs/provider-catalog.md)

## Try it locally

Requires Go 1.25+ on macOS or Linux. From the repository root:

```sh
make demo
```

Open **http://127.0.0.1:23005**. Choose **New transfer → Start transfer** to submit the demo's 1,000
four-dimensional vectors. The **This server** option runs them on your machine.
Output is saved in `data/demo-vectors/`. The demo makes no cloud requests.

To submit from another terminal:

```sh
./bin/vtransfer submit -file examples/demo-job.json -idempotency-key first-demo
./bin/vtransfer show JOB_ID
./bin/vtransfer events JOB_ID
```

## Guides

[Public getting-started guide](https://transfer.polign.com/docs.html) ·
[Local demo quickstart](https://transfer.polign.com/#try-demo)

- [Customer worker setup](docs/customer-workers.md): installation, connections, and recovery.
- [Hosted and operator setup](docs/transfer-setup.md): UI credentials and configuration files.
- [Provider catalog](docs/provider-catalog.md): schemas, authentication, and limits.
- [Account setup and security](docs/accounts-and-security.md): sign-in and credential boundaries.
- [Deployment](deploy/README.md): release, update, and backup procedures.
- [API and CLI](docs/api.md): job and worker endpoints.
- [Architecture](docs/architecture.md): execution, retries, checkpoints, and audit storage.

## Add a connector

Implement `Source.Read` and/or `Sink.Upsert`, then register a factory with
`app.Run`. The same connector works in the server and customer worker.

[Author guide](docs/connectors.md) · [Example](examples/custom/README.md) ·
[Contract](docs/connector-contract.md) · [Contributing](CONTRIBUTING.md)

```sh
make test   # Go tests with race detection
make test-ui # Generated worker script tests; requires Node
make check  # Go vet and JavaScript syntax check; requires Node
```

Most provider tests use protocol fixtures. See the
[catalog](docs/provider-catalog.md#verification) for live integration test setup.

## License

Licensed under [Apache 2.0](LICENSE). Bundled fonts retain their
[separate license](control/web/FONT-LICENSE.txt).
