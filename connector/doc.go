// Package connector is the public Go contract for Vector Transfer connectors.
//
// Implement Source.Read to export a stable dataset, Sink.Upsert to receive it,
// or both. The engine owns scheduling, retries, checkpoints, and audit history.
// A connector owns provider authentication, pagination, data conversion, request
// limits, and acknowledgement of writes.
//
// To distribute a connector independently, export a Factory that opens an
// Adapter from provider-specific JSON options. Pass it to LoadConfig or app.Run
// using a Factories map. No changes to the built-in provider switch are needed.
//
// Delivery is at least once. Source cursors must survive reopening, and sink
// writes must be idempotent by record ID. Source data must remain stable during
// a transfer and resume. This contract covers one dense vector per record;
// sparse vectors, multivectors, and change streams are outside its scope.
//
// Errors may appear in the control plane's audit history. Return safe messages
// without credentials, metadata values, or raw provider response bodies. Wrap
// temporary failures with Transient; other errors fail the job without retries.
//
// See docs/connectors.md for the author guide and connector/conformance for
// reusable source and sink contract checks.
package connector
