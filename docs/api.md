# API and CLI

## Authentication

For account deployments, inject `VECTOR_TRANSFER_ACCOUNT_TOKEN` into the CLI
and pass `-url https://transfer.polign.com`. HTTP clients use
`Authorization: Bearer TOKEN`. Browser mutations require the session cookie,
matching Origin, and `X-CSRF-Token`.

For single-operator deployments, the CLI reads `VECTOR_TRANSFER_TOKEN`.
Operator mode shares jobs and connections. Local demo calls need no token.

Send JSON with `Content-Type: application/json`. Credentials belong in connection
setup, never in job specifications.

## Jobs

```json
{
  "name": "Production migration",
  "worker_id": "REGISTERED_WORKER_ID",
  "source": "production-source",
  "sink": "polign-destination",
  "dimension": 1536,
  "batch_size": 100
}
```

Omit `worker_id` for hosted execution. Customer jobs must match a locally approved
connection pair and dimension. Optional `max_attempts` defaults to 5 (range 1–10).
`max_restarts` defaults to 3; use -1 to disable automatic restarts or 1–10 to set a budget.

| Endpoint | Purpose |
|---|---|
| `POST /v1/jobs` | Submit; 202 for a new job, 200 for an idempotent repeat |
| `GET /v1/jobs?state=running&limit=100&offset=0` | List or filter jobs |
| `GET /v1/jobs/{id}` | State and progress |
| `GET /v1/jobs/{id}/events?after=0&limit=100` | Audit events and `next_after` cursor |
| `POST /v1/jobs/{id}/cancel` | Cancel; send `{}` |
| `POST /v1/jobs/{id}/resume` | Resume failed/canceled jobs or retry now; send `{}` |

Use `Idempotency-Key` to deduplicate submissions. Reusing a key with different
settings returns 409. Keys are scoped to the verified owner.

CLI flags precede positional job IDs:

```sh
./bin/vtransfer connections
./bin/vtransfer submit -file job.json -idempotency-key migration-001
./bin/vtransfer jobs
./bin/vtransfer show JOB_ID
./bin/vtransfer events -after 100 JOB_ID
./bin/vtransfer cancel JOB_ID
./bin/vtransfer resume JOB_ID
```

The default URL is `http://127.0.0.1:23005`. Add `-url` to each command for a
remote deployment. The CLI supports jobs and connections; worker registration
and revocation use the UI or API.

## Connections and workers

| Endpoint | Purpose |
|---|---|
| `GET /v1/connections` | Accessible hosted connections and setup feature flags |
| `POST /v1/connections` | Save a private connection; requires account mode, vault, and `Idempotency-Key` |
| `PUT /v1/connections/{id}/credentials` | Replace saved credentials |
| `DELETE /v1/connections/{id}` | Remove a connection after canceling active jobs |
| `GET /v1/workers` | List the owner's workers and approved connections |
| `POST /v1/workers` | Register with `{"name":"production"}`; returns enrollment token once |
| `POST /v1/workers/{id}/revoke` | Revoke; send `{}` |
| `GET /healthz` | Public store health; does not test database connectivity |

## Worker protocol

The worker CLI handles these endpoints. It uses its own identity and session,
separate from Polign account credentials.

| Endpoint | Purpose |
|---|---|
| `POST /worker/v1/enroll` | Bind enrollment token to a public key |
| `POST /worker/v1/challenge` | Request an authentication challenge |
| `POST /worker/v1/session` | Prove key possession and obtain a session |
| `POST /worker/v1/manifest` | Publish approved connection capabilities |
| `POST /worker/v1/claim` | Long-poll for an assigned job and lease |
| `POST /worker/v1/jobs/{id}/progress` | Report progress using the job lease |

Progress requests accept counts, predefined phases, and failure codes—not raw
provider errors, payloads, or cursors. See [worker setup](customer-workers.md)
and [security](accounts-and-security.md#customer-workers).
