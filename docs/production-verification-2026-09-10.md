# Production verification — September 10, 2026

**Passed: S3 Vectors → Polign through https://transfer.polign.com**, owned by
`anuptalwalkar@gmail.com`. Sign-in, worker setup, submission, cancellation, and
resume used the real production UI and API, without mocked responses.

## Test

- Job: `24266a1851a950c95f9b52eed0b6a92f` — **Production verification — S3 Vectors to Polign**.
- Worker: `586160c095503519819611d06448d720`, running on a temporary AWS EC2 instance.
- AWS account/region: `298123941500`, `us-east-1`.
- Source: dedicated S3 Vectors index containing 1,200 synthetic four-dimensional vectors, with string, numeric, and array metadata.
- Destination: Polign 0.6.1 on the test host, backed by a separate S3 bucket. TLS and data-key authentication enabled; no inbound ports.
- Transfer release: `9a1007c32055e15da81c6ed010f81ab739318eb22311806ce534bf0102b49ff1`.

## Results

| Check | Result |
|---|---|
| Production health, HTTPS, assets, and worker binary checksum | Passed |
| Sign-in and job ownership | Confirmed against the requested account's Cognito subject |
| Setup handoff | UI detected the AWS worker and prefilled source, destination, and dimension |
| Worker crash | SIGKILL after 163 locally checkpointed records; production showed progress and then offline status |
| Worker restart | Same identity and state directory continued from saved progress |
| Cancel and resume | Canceled through the UI at 503 records; resumed past that checkpoint |
| Final job | Succeeded: 1,200 records, 1,200 batches, three runs |
| Data comparison | All 1,200 source IDs, vector values, and metadata matched destination point reads |
| Polign restart | All 1,200 records matched again after replay from S3 |
| Source permissions | Worker role's attempted source write was denied |
| Account isolation | Another authenticated test account received 404 for the job and audit trail; the worker was absent from its list |
| Credential boundary | Database key stayed on the test host; job/audit responses contained no database credentials, vectors, metadata, or source cursor |

Canonical JSON checksums matched before and after the Polign restart:

```text
744e8ba1b6df5484016240511a0b76f47685d0329154bfd8f17549286bf598bc
```

## Findings and limits

Polign's listing endpoint returned 501 for this cold-served collection. Verification
used a point read for every source ID. This validates ingress and the transferred
records; it does not independently enumerate unknown destination IDs. The current
Polign source connector cannot export a cold-served collection through that endpoint.

Polign took about 29 seconds to replay its retained log after restart. The initial
10-second health wait was too short; the subsequent health and full data checks passed.

A completed job incorrectly displayed “awaiting reconnection” after its worker was
retired. The correction passed an isolated browser regression check and is now present in
the live JavaScript asset, published alongside the separate UI changes.

This was a functional test with synthetic data, not a throughput or large-dataset benchmark.

## Cleanup

The completed job and audit trail remain in the requested Polign account. The test
worker was revoked. The temporary EC2 instance, S3 bucket, S3 Vectors index/bucket,
IAM stack resources, and disposable secondary Cognito identity were removed.
No existing customer datasets or production infrastructure were deleted.
