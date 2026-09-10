# Connector contract — revision 1

This document defines the behavior expected by the current Go engine. Revision 1
is a documentation revision, not a released module version. The required methods
remain `Source.Read(ctx, cursor, limit)` and `Sink.Upsert(ctx, records)`.

## Responsibilities

| Connector owns | Vector Transfer owns |
|---|---|
| Provider clients, authentication, and cleanup | Job submission and operator actions |
| Exhaustive export and pagination | Scheduling and worker concurrency |
| Stable IDs and explicit type conversion | ID, dimension, and finite-vector validation |
| Request limits and write acknowledgement | Retries, checkpoints, and progress |
| Safe error classification | Audit history and restart recovery |

## Records and ownership

- A record has a nonempty string ID, one dense float32 vector, and JSON metadata.
  IDs are unique within the resource and retain their meaning across replay.
- Numeric IDs need a lossless, documented conversion. Do not merge distinct IDs
  through truncation or normalization.
- Preserve metadata within the declared source/sink type contract. Reject
  unsupported values; do not omit fields or overwrite reserved attributes.
- Source results belong to the caller. Do not reuse their underlying vector
  slices, metadata maps, or JSON buffers in subsequent calls.
- Sink input belongs to the caller. Do not modify it; copy anything retained
  after returning. A successful upsert replaces the vector and metadata by ID,
  including removal of obsolete metadata fields.

## Source reads

- The engine supplies a positive limit and an empty initial cursor. Return no
  more than that limit.
- `Done` is authoritative. Short and empty pages may be non-final. Every
  non-final page requires a nonempty `Next` different from its input cursor.
- A complete scan emits every record once. Reusing a cursor against the same
  stable dataset replays the same page contents. Tokens need not be byte-identical
  if they identify equivalent positions.
- Cursors must not contain credentials or signed URLs; they enter durable state.
- Cursors must work on fresh connections after a process restart. Do not depend
  on local iterators or session-only state. Document cursor expiry and fail
  explicitly if resumption becomes impossible; do not restart at the beginning.
- The engine discards any page returned with an error. Never advance mutable
  state that prevents retrying the original cursor.
- Source data must stay unchanged throughout execution and resume. Revision 1
  does not require or imply snapshot isolation or change capture.

## Sink writes and retries

- The engine supplies a nonempty batch. Split it internally for provider count
  and byte limits. Return nil only when every part is acknowledged at the
  database's documented durable acceptance boundary.
- Errors and cancellation may follow partial writes. The engine can replay the
  entire page with identical IDs and content; this must not create duplicate
  records or accumulate extra side effects.
- The next cursor is saved only after the whole page succeeds. Remote writes
  and the local checkpoint are not a single transaction.
- Delivery is at least once. No rollback, exactly-once behavior, independent
  destination verification, or immediate search visibility is implied.

## Concurrency, cancellation, and lifecycle

- Factories create resource-level adapters, not per-job iterators. An adapter
  can be shared by independent jobs; operations must be safe for concurrent use.
- Check cancellation before work and during blocking calls. Return errors wrapping
  `context.Canceled` or `context.DeadlineExceeded` as appropriate. Apply finite
  timeouts to provider requests.
- Cleanup runs after workers stop. The owning registry invokes `Adapter.Close`
  at most once. Each factory call owns its cleanup independently; shared pools
  need suitable reference counting.
- Let the engine own retry policy. Bound any retries performed by a provider SDK
  and honor the supplied context.

## Errors

Ordinary errors fail the job. Request a bounded retry with:

```go
return &connector.Transient{Err: errors.New("database temporarily unavailable")}
```

Transient network failures, throttling, and temporary service unavailability
usually qualify. Authentication, schema, cursor, and unsupported-type failures
usually need operator intervention.

The engine stores generic runtime failure messages as a defense against secret
leakage; connector errors must still be safe for callers and startup diagnostics. Exclude credentials, record contents,
and raw provider bodies. Partial acknowledgement is an error; the contract has
no silent-skip or dead-letter behavior.

## Configuration and compatibility

- Custom factories receive an `options` JSON object. Use typed decoding and
  reject unknown settings. Resolve credential references through your client.
- `ResourceID` must distinguish accounts, clusters, and resources independently
  of aliases and credentials. It is hashed with `kind` to identify target jobs.
- Change `CheckpointVersion` for incompatible cursor or data-interpretation
  changes. It and the options participate in the job fingerprint, so unsafe
  resumes fail explicitly.
- Add future capabilities, such as partitioning or schema inspection, through
  optional interfaces. Do not expand the required Source/Sink method sets in a
  compatible release. Incompatible changes require a documented module migration;
  this revision does not announce a stable release.

Revision 1 covers Go implementations compiled into a service. Sparse vectors,
multiple vector fields, change streams, runtime plugin loading, and cross-language
RPC are outside this contract.
