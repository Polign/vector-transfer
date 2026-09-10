# Customer worker setup

A worker is a program you run on your own server to move embeddings and metadata between databases. Use it to keep database credentials and checkpoints in your infrastructure while you create and monitor transfers in the Polign dashboard. It connects to Polign over HTTPS and reports progress; vector data does not pass through the control plane.

## Create connection

Open [Create connection](https://transfer.polign.com/connections/new). Enter a
worker name, platform, vector dimension, and source and destination settings.
Choose **Continue to worker setup** and download `setup-worker.sh`.

Move the script privately to the worker machine, review it, and run:

```sh
chmod 600 setup-worker.sh
bash setup-worker.sh
```

The script downloads and verifies the worker, saves its configuration, enrolls
it with your account, and prompts for database credentials locally. S3 Vectors
uses the machine's AWS credentials or workload identity. Settings stay in your
browser until downloaded; only the worker name is sent during setup.

Run the script within 10 minutes, then delete it: it contains the enrollment
token. Leave the worker running and keep the setup page open. It detects the
connection automatically. Choose **Create transfer** to open a job with the
worker, connections, and vector dimension filled in. Review it and choose
**Start transfer**.

If setup expires, choose **Generate a new setup script**. If the worker is
offline, the page shows its restart command. You can return through **Continue
setup** in the dashboard; settings and enrollment tokens are not saved in the
browser. A connected worker has a **Create transfer** action.

Files are installed under `~/.polign-transfer/WORKER_ID/`. To restart, run
`bash ~/.polign-transfer/WORKER_ID/run-worker.sh`. Credentials can be injected
through environment variables or entered at its prompts; they are not saved by
the script. Use a supervisor to keep the worker running after reboots.

The script is for initial setup. To change an installed worker's connections,
edit its local configuration and restart it. Rerunning the installer preserves
existing state. Keep the source unchanged during transfers and resumes.

## Manual download

Choose your platform. These commands download the worker, verify its checksum,
and save it as `vtransfer`. Then enroll and configure it below.

### Linux x64

[Download vtransfer-linux-amd64](https://transfer.polign.com/downloads/vtransfer-linux-amd64)

```sh
curl -fSLO https://transfer.polign.com/downloads/vtransfer-linux-amd64 &&
curl -fSLO https://transfer.polign.com/downloads/SHA256SUMS &&
sha256sum --ignore-missing --check SHA256SUMS &&
chmod 755 vtransfer-linux-amd64 &&
mv vtransfer-linux-amd64 vtransfer
```

### Linux ARM64

[Download vtransfer-linux-arm64](https://transfer.polign.com/downloads/vtransfer-linux-arm64)

```sh
curl -fSLO https://transfer.polign.com/downloads/vtransfer-linux-arm64 &&
curl -fSLO https://transfer.polign.com/downloads/SHA256SUMS &&
sha256sum --ignore-missing --check SHA256SUMS &&
chmod 755 vtransfer-linux-arm64 &&
mv vtransfer-linux-arm64 vtransfer
```

### macOS Apple Silicon

[Download vtransfer-darwin-arm64](https://transfer.polign.com/downloads/vtransfer-darwin-arm64)

```sh
curl -fSLO https://transfer.polign.com/downloads/vtransfer-darwin-arm64 &&
curl -fSLO https://transfer.polign.com/downloads/SHA256SUMS &&
shasum -a 256 --ignore-missing --check SHA256SUMS &&
chmod 755 vtransfer-darwin-arm64 &&
mv vtransfer-darwin-arm64 vtransfer
```

To build from source instead: `go build -trimpath -o vtransfer ./cmd/vtransfer`.

## Enroll

The generated setup script handles enrollment. For manual API setup, register
with `POST /v1/workers` and body `{"name":"production"}` using your authenticated
session and CSRF token. Save the returned `enrollment_token` privately as
`enrollment.token` on the worker machine. It expires in 10 minutes.
See [API authentication](api.md).

```sh
chmod 600 enrollment.token
./vtransfer worker enroll -url https://transfer.polign.com -data ./worker-data -token-file ./enrollment.token
```

Remove the token file after enrollment succeeds. If the response is interrupted,
retry with the same token and data directory. Keep `worker-data` private and
persistent; it holds the worker identity and checkpoints. Run one process per
identity and data directory.

## Configure connections

Copy the [example connection file](../examples/customer-worker/connections.json)
to `connections.json` on the worker machine. Set the database endpoints and
resources, then inject its `SOURCE_API_KEY` and `DESTINATION_API_KEY` variables
through your secret manager or process supervisor.

Create `worker.json` to approve a source, destination, and vector dimension:

```json
{
  "connections_file": "connections.json",
  "pairs": [
    {"source": "production-source", "sink": "polign-destination", "dimension": 1536}
  ]
}
```

Connection paths are relative to `worker.json`. Use the exact connection names
and dimension from your databases. The worker rejects jobs outside these pairs.
Check [provider requirements](provider-catalog.md) before running a transfer.

Credentials are supplied locally, not through the Polign UI. The worker reads
API key variables at startup; restart it after rotating them. Your deployment
must inject secrets—the worker does not fetch them from a secret manager itself.
S3 Vectors can use the worker's AWS workload identity with automatic credential
refresh. See [connection settings](transfer-setup.md#operator-managed-connections).

## Run and submit

```sh
./vtransfer worker run -data ./worker-data -config ./worker.json
```

Wait for **online** in the dashboard. Choose **New transfer**, select the worker
and approved connections, then submit. One worker executes one job at a time.
Use a process supervisor to start it after a machine reboot.

Keep the source unchanged during execution and resume. Destination upserts
replace matching IDs. Restart the worker to load changed policy or configuration;
resource changes can make existing checkpoints incompatible.

## Recovery

| Situation | Action |
|---|---|
| Worker disconnected | Restore connectivity; it reconnects and reacquires a lease |
| Worker process restarted | Use the same identity, data directory, and connection settings |
| Job failed | Fix the cause, then choose **Resume transfer** |
| Local checkpoint missing or behind reported progress | Restore the original worker data; the job will not silently restart |
| Worker identity compromised | Revoke it in the dashboard and revoke affected database credentials |

Encrypt and back up the worker volume. Its files have restricted permissions,
but the journal is not separately encrypted by the application. Jobs are not
automatically reassigned to another worker. Customer workers do not auto-update.

Connection loss stops execution. Cancellation and revocation take effect when
detected; a database request already in flight may finish. A crash between a
write and checkpoint can replay a batch. Avoid concurrent migrations to the same
destination from different workers or hosted jobs.

[Security boundaries](accounts-and-security.md#customer-workers) ·
[Execution details](architecture.md) · [Worker API](api.md#worker-protocol)

## Docker

Build [Dockerfile.worker](../Dockerfile.worker) from a reviewed checkout:

```sh
docker build -f Dockerfile.worker -t vector-transfer-worker:local .
```

The image runs as UID/GID 65532. Give that user a persistent writable `/data`
mount and a read-only `/config` mount containing `worker.json` and
`connections.json`. For enrollment, also make `enrollment.token` readable by
that user with mode 0600.

```sh
docker run --rm --read-only --cap-drop ALL --security-opt no-new-privileges \
  --mount type=bind,src="$PWD/worker-data",dst=/data \
  --mount type=bind,src="$PWD/config",dst=/config,readonly \
  vector-transfer-worker:local worker enroll -data /data -token-file /config/enrollment.token
```

After enrollment, remove the token file and run with locally injected credentials:

```sh
docker run --rm --read-only --cap-drop ALL --security-opt no-new-privileges \
  --mount type=bind,src="$PWD/worker-data",dst=/data \
  --mount type=bind,src="$PWD/config",dst=/config,readonly \
  --env SOURCE_API_KEY --env DESTINATION_API_KEY \
  vector-transfer-worker:local
```

`--env NAME` passes an existing environment variable. Do not bake credentials or
worker identities into the image. Container execution was not validated for the
initial release; see [release verification](../deploy/live.json).
