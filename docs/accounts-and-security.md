# Account setup and security

Polign sign-in uses Cognito and the account service's authenticated `GET /v1/me`.
Jobs, connections, workers, and idempotency keys are scoped to the verified
subject. Other subjects receive 404 for private job resources.

For the existing deployment, see the [operations guide](../deploy/README.md).

## Configure account mode

1. Copy [account-config.example.json](../account-config.example.json) to
   `account-config.json`. Set the transfer, account portal, account API, and Cognito
   HTTPS origins and public client ID. Origins cannot include paths or queries.
2. Configure the Cognito client for authorization code + PKCE, with `openid`,
   `email`, `profile`, and `https://account.polign.com/account` scopes. Register
   `https://YOUR-TRANSFER-HOST/auth/callback` and sign-out URL
   `https://YOUR-TRANSFER-HOST/`. Preserve existing portal URLs. The client must
   support `refresh_token`; client secrets and refresh-token rotation are unsupported.
3. Create `account-connections.json` containing `{"connections":{}}`.
4. Put the service behind HTTPS. Keep its listener private. The proxy must preserve
   `Host`, overwrite `X-Forwarded-Proto` with the actual scheme, and forward
   `/auth/*`, `/v1/*`, `/worker/*`, `/downloads/*`, and static assets.
5. Start the control plane with persistent storage:

   ```sh
   go build -o bin/vtransfer ./cmd/vtransfer
   ./bin/vtransfer serve \
     -account-config account-config.json \
     -config account-connections.json \
     -data /var/lib/vector-transfer \
     -workers 0
   ```

`-workers 0` allows customer workers and disables hosted execution. To enable
hosted jobs and private UI connections, set `-workers 2` and add
`-credential-key-file /etc/vector-transfer/credential.key`. Provision that file
through your secret store with a base64-encoded, random 32-byte key.

Account mode rejects `VECTOR_TRANSFER_TOKEN` and `-demo`. Local jobs cannot run
in account mode, and account jobs cannot run under another account API origin.
Use a separate data directory for a different account realm. A separate Cognito
client also needs approval by the account API's audience and scope validation.

Verify sign-in, sign-out, and a small transfer after configuring the deployment.

## Database credentials

| Mode | Credential access | Rotation |
|---|---|---|
| Customer worker | Local worker resolves injected credentials or workload identity | Restart for new API key values; AWS role credentials refresh automatically |
| Polign-hosted private connection | Server decrypts an account-owned vault entry | Update credentials in job details |
| Operator-managed connection | Server resolves configured credential references | Restart after changing injected values |

Use separate resource-scoped source and destination credentials. Application
`read_only` and `write_only` settings do not replace database permissions.

### Hosted private connections

Connection entries are encrypted with AES-256-GCM in `data/connections/`.
The filename is authenticated; writes use private files, fsync, and atomic
replacement. Wrong keys or modified entries stop access. Keep the master key
separate and back it up with the encrypted entries. **The server can decrypt
credentials, so its administrators and runtime remain trusted.**

Private HTTP endpoints require public HTTPS. Native pgvector, Redis, and MongoDB
require verified TLS. Dialers reject private, loopback, link-local, metadata-service,
and mixed public/private DNS answers. They pin validated addresses, including
hosts discovered by MongoDB. HTTP redirects and environment proxies are disabled.
Use a customer worker to reach private database endpoints.

Private S3 Vectors connections use only the submitted AWS credentials, never
host IAM. Temporary credentials must be replaced when they expire. Rotation
applies to subsequent batches; an in-flight request may use the old key.
Deleting a connection requires canceling its active jobs and prevents their resume.
Limits are 50 connections per account and 2,000 per deployment.

### Operator-managed grants

Operator connections use exact `read_subjects` and `write_subjects` lists.
Missing grants deny account access in that direction. The server checks them
at submission, resume, and execution. Edit the configuration and restart to
change grants. Grant changes preserve checkpoint compatibility.

Custom factories run trusted code in the server or worker process. They receive
configuration options, not Polign tokens or account grants. Review them before
installation; they are not sandboxed plugins.

## Customer workers

Enrollment binds a public key to the registering subject. The single-use token
expires in 10 minutes; the worker stores its private Ed25519 seed locally with
mode 0600. One-minute, single-use challenges issue 10-minute worker sessions.
Sessions stay in memory and renew through proof of private-key possession.
Revocation blocks subsequent requests and renewal. Replace a compromised
identity by registering a new worker; in-place key rotation is unsupported.

Workers independently enforce approved connection pairs, dimensions, and
configuration hashes. The control plane receives connection names, capabilities,
keyed hashes, counts, phases, and coarse failure codes. It receives no database
credentials, endpoints, secret references, vectors, metadata, cursors, or batch
content hashes.

A compromised control plane can still request transfers within approved pairs.
Restrict database permissions and network egress accordingly. Customers control
worker installation and updates and must trust that software. Leases stop stale
progress reports; they cannot retract an in-flight database write.

[Worker setup and recovery](customer-workers.md) · [Protocol](api.md#worker-protocol)

## Sign-in and storage

- Cognito access and refresh tokens stay in server memory. Browsers receive an
  opaque Secure, HttpOnly, SameSite=Lax `__Host-` cookie; no tokens enter browser storage.
- Login uses browser-bound, single-use state and PKCE with a five-minute limit.
  Sessions last at most eight hours. A server restart requires signing in again.
- Each account API request is checked through `/v1/me`. Cookie-based mutations
  require the public Origin and a session-bound CSRF header. Revocation timing
  depends on the account API's token validation.
- Do not log cookies, Authorization headers, callback queries, or token exchanges.
  Runtime job errors exclude raw provider errors and payloads.
- Journals and identity files use mode 0600; new data directories use 0700.
  Encrypt storage and backups. Hosted journals retain cursors and batch hashes;
  customer worker cursors and receipts remain in the customer's journal.

For CLI access, inject `VECTOR_TRANSFER_ACCOUNT_TOKEN` and use
`-url https://YOUR-TRANSFER-HOST`. Bearer requests do not need browser CSRF tokens.
The CLI refuses to send tokens over HTTP outside loopback.

The control plane supports one process per data directory. Horizontal replicas
require shared sessions and a distributed state store.

Cognito references: [PKCE](https://docs.aws.amazon.com/cognito/latest/developerguide/authorization-endpoint.html),
[token exchange](https://docs.aws.amazon.com/cognito/latest/developerguide/token-endpoint.html),
[logout](https://docs.aws.amazon.com/cognito/latest/developerguide/logout-endpoint.html).
