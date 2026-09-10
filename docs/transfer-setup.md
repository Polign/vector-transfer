# Transfer setup

For execution in your own infrastructure, use the
[customer worker guide](customer-workers.md). This page covers hosted execution
and operator-managed connections.

## Polign-hosted transfers

1. Sign in at [transfer.polign.com](https://transfer.polign.com).
2. Choose **New transfer → Polign hosted**.
3. Select a saved source and destination, or choose **Set up a new connection**
   and enter each database's settings and credentials.
4. Enter a job name, vector dimension, and batch size. Select **Submit transfer**.

Saved connections belong to your account; they need no administrator grant.
Polign decrypts their credentials to run transfers. See
[credential boundaries](accounts-and-security.md#database-credentials).

To replace an expired key, open the job's **Source credentials** or
**Destination credentials**, save the new credentials, then choose **Resume
transfer**. A resumed job keeps its checkpoint. Changing the database or resource
requires a new connection and job.

## Prepare the databases

- Match the destination's vector dimension, distance metric, ID format, and
  metadata schema to the source. Check the [provider catalog](provider-catalog.md).
- Grant source read access and the destination's required read/write permissions.
- Keep the source unchanged until the job and any retries or resumes finish.
- Upserts replace matching destination IDs. Cancellation does not undo writes.

The common record is `{ "id": "...", "values": [0.1, 0.2], "metadata": {...} }`.
Vectors are dense float32; metadata remains raw JSON. Provider type and size
limits still apply. Sparse vectors, multiple vector fields, schema creation,
and automatic metadata conversion are unsupported.

## Operator-managed connections

From the repository root:

```sh
cp config.example.json config.json
make build
```

Edit `config.json`: keep only the connections you need and set their resource
names and endpoints. Each connection identifies one index, collection, or
namespace. Inject the referenced credential environment variables before startup.

```sh
./bin/vtransfer serve -config config.json -data ./data
```

In another terminal, edit the example job's connection names and dimension, then
submit it:

```sh
./bin/vtransfer submit -file examples/pinecone-to-polign.json
```

| Setting | Purpose |
|---|---|
| `api_key_env` | Environment variable holding an API key |
| `username_env`, `password_env` | Environment variables for native database credentials |
| `read_only: true` | Make the connection source-only |
| `write_only: true` | Make the connection destination-only |
| `read_subjects`, `write_subjects` | Exact account subjects allowed to use an operator connection in account mode |

Operator mode shares jobs and connections. It listens on loopback by default.
For remote single-operator access, set `VECTOR_TRANSFER_TOKEN` and put the service
behind HTTPS. Use [account mode](accounts-and-security.md) for multiple users.

API key values load at startup; restart to apply changes. Rotating a value behind
the same reference preserves checkpoints. Changing resource settings prevents
existing jobs from resuming until their original configuration is restored.

### S3 Vectors

Operator connections and customer workers use the AWS SDK credential chain,
including workload identities. Set `aws_role_arn` per connection to assume a role;
`aws_external_id_env` supplies its external ID when required.

The source needs `s3vectors:ListVectors` and `s3vectors:GetVectors`; the destination
needs `s3vectors:PutVectors`. Customer-managed KMS keys may require key permissions.
The default endpoint is `https://s3vectors.REGION.api.aws`; other AWS partitions
need an explicit endpoint.

Polign-hosted private connections use the AWS credentials entered in the form,
not the host's IAM role. Replace temporary credentials when they expire.

## Monitor and recover

The dashboard shows state, acknowledged records, batches, retries, and the last
checkpoint. It polls every two seconds. Total source size is unknown, so running
jobs show activity rather than a percentage.

Temporary failures retry automatically. For a failed job, fix the cause and
choose **Resume transfer**. **Retry now** skips a scheduled retry delay. **Cancel
transfer** stops at an operation boundary; an in-flight write may finish.

A checkpoint is saved after the destination acknowledges a full batch. A crash
before that save can replay the batch. `succeeded` means all source pages were
acknowledged; it does not verify destination readback or search visibility.

See [execution details](architecture.md) and [CLI commands](api.md).
