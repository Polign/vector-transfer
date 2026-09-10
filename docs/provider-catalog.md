# Supported database connections

All 15 providers support source and destination connections. Configure them
[on a customer worker](customer-workers.md) or [in hosted setup](transfer-setup.md).
Prepare the destination schema and keep the source unchanged through any resume.

| Provider | Settings | Supported behavior |
|---|---|---|
| Pinecone | Index HTTPS endpoint, namespace, API key | Serverless ID listing + fetch; dense vector upserts |
| Weaviate | HTTPS endpoint, collection, optional tenant, API key | Object cursor scan, batch upsert; UUID IDs and unnamed vectors |
| Milvus | HTTPS endpoint, database, collection, ID/vector fields, token | REST v2 (2.6+); INT64 primary keys; AutoID disabled |
| Elasticsearch | HTTPS endpoint, concrete index, ID/vector fields, encoded API key | 8.19+ search_after; checked bulk writes; vectors explicitly included |
| OpenSearch | HTTPS endpoint, concrete index, ID/vector fields, username/password | Search_after and checked bulk writes; basic-auth domains |
| pgvector | `postgresql://host:port`, database, table, ID/vector/metadata columns, username/password | Native PostgreSQL TLS; ordered reads and transactional ON CONFLICT writes |
| S3 Vectors | Region, vector bucket, index; AWS workload identity on customer workers or credentials in hosted setup | Signed vector listing and upserts; standard AWS partition |
| Chroma | HTTPS endpoint, tenant, database, collection UUID, API key | API v2 get/upsert with embeddings, metadata, documents and URIs |
| Redis | `rediss://host:port`, DB number, key prefix, vector/metadata fields, ACL credentials | Standalone HASH records; FLOAT32 little-endian vectors |
| MongoDB | `mongodb://host:port` or `mongodb+srv://host`, database, auth database, collection, vector field, ID type, username/password | Native TLS; `_id` keyset scans and majority-acknowledged replacement writes |
| FAISS | Adapter HTTPS endpoint, collection, API key | [Included file adapter](../adapters/faiss/README.md), real FAISS snapshots with IDs and metadata |
| Solr | HTTPS endpoint, collection/core, unique key/vector fields, username/password | Stateless cursorMark scans and committed overwrite-by-ID updates |
| Qdrant | HTTPS endpoint, collection, API key | Scroll with unnamed vectors/payload; completed upserts; UUID or uint64 IDs |
| Polign | HTTPS endpoint, collection, API key | Batch upsert with exact ID acknowledgement; source reads require listing support, which cold-served collections lack |
| Turbopuffer | Regional HTTPS endpoint, namespace, metric, ID type, API key | Ordered ID scans and checked row upserts; dense `vector` attribute |

Hosted private connections require public endpoints and verified TLS. Customer
workers can reach databases on their own network. Use workload identity for S3
Vectors on a worker; hosted private connections use the credentials entered in
the form. See [credential handling](accounts-and-security.md#database-credentials).

## Data compatibility

- **Pinecone:** source ID listing requires a serverless index. Sparse values are
  rejected. Missing fetch results fail the job rather than skipping IDs.
- **Turbopuffer:** transfers use the dense `vector` attribute. Sink `id_type` is
  `string` (default, at most 64 bytes), `uint` (canonical decimal), or `uuid`.
  Metadata keys `id`, `vector`, and keys starting with `$` are rejected.
- **Elasticsearch/OpenSearch:** use a unique, single-valued sortable field with
  doc values (typically keyword `id`) equal to each document's `_id`. Every source
  document must contain it and the vector field. Sorting on `_id` itself is not
  supported. Saved cursors do not depend on expiring scroll/PIT leases. Source
  mutation during transfer/resume is unsupported. IAM/SigV4 OpenSearch domains
  are not supported by this initial basic-auth adapter.
- **Milvus:** ordinary 2.x queries are unordered and offset windows are capped.
  The adapter splits disjoint INT64 primary-key ranges until a whole range fits
  in one page, then advances past that range. This costs extra queries but avoids
  skipping records or depending on a server-side iterator lease. Primary-key
  strings are exact decimal integers. Upserts verify the returned original IDs;
  AutoID must be disabled. Other entity fields become metadata.
- **pgvector:** use a public-schema table with a unique ID column, a `vector(N)`
  column, and a JSONB metadata column. Names are configurable simple identifiers.
  Metadata must be an object or null. Keyset ordering uses the ID's text value
  with C collation; query values are bound, not interpolated from input.
- **Redis:** HASH vectors use FLOAT32 little-endian bytes. The metadata field is
  a JSON object; other HASH fields become string metadata. Writes replace the
  entire HASH and clear its TTL. Configure a search index over the chosen vector
  field. SCAN can repeat keys, so acknowledged record counts can exceed unique
  IDs; upsert replay does not create duplicates. Cluster/Sentinel, RedisJSON and
  FLOAT64 blobs are outside this adapter's initial contract.
- **MongoDB:** source `_id` values must all use the selected type: ObjectID,
  string or INT64. Mixed types fail before scanning. The vector must be a dense
  numeric array. Other fields travel as canonical Extended JSON to preserve BSON
  types and large integers. BSON wrappers may be incompatible with a scalar-only
  sink such as Polign; incompatible writes fail explicitly. Authentication database
  defaults to `admin` and can be changed separately from the data database.
- **Chroma:** `_chroma_document` and `_chroma_uri` are reserved metadata keys
  carrying document/URI values; writes to Chroma restore their native fields.
  Destination credentials also need read access: existing metadata keys absent
  from the source are cleared before acknowledgement. If a destination document
  or URI is absent from the source, the adapter rejects the write; use an empty
  destination for that case. Null source metadata values are unsupported.
  Destination metadata types must satisfy Chroma's schema. Use a collection UUID.
- **Solr:** the vector and metadata fields must be stored and returned by `fl=*`.
  Choose the schema's uniqueKey for cursor sorting. Internal `_version_` values
  are excluded because they belong to that Solr instance.
- **FAISS:** bare index files need an ID/metadata sidecar for import. The adapter
  exports reconstructable dense vectors, not the original index topology. It
  rebuilds a flat snapshot per batch; see its README for performance constraints.
- **Polign and other destinations:** provider ID formats, vector dimensions and
  metadata schemas still apply. The service does not regenerate embeddings,
  create database schemas, truncate metadata, or silently rename IDs.

## Local configuration

Use the same connection format for customer workers and operator-managed servers:

```json
{
  "connections": {
    "qdrant-source": {
      "kind": "qdrant", "endpoint": "https://your-qdrant-host",
      "collection": "documents", "api_key_env": "QDRANT_READ_KEY", "read_only": true
    },
    "pgvector-sink": {
      "kind": "pgvector", "endpoint": "postgresql://your-postgres-host:5432",
      "database": "documents", "collection": "embeddings",
      "id_field": "id", "vector_field": "vector", "metadata_field": "metadata",
      "username_env": "PGVECTOR_USER", "password_env": "PGVECTOR_PASSWORD", "write_only": true
    }
  }
}
```

Kinds use lowercase provider names (`s3vectors` for S3 Vectors). Add subject
grants for operator connections in account mode, or approved pairs for customer
workers. See [setup](transfer-setup.md#operator-managed-connections).

## Verification

The default `go test -race ./...` suite verifies provider wire formats, exact
metadata, partial acknowledgements, Milvus range scans, secret isolation and
checkpoint behavior. Redis uses an isolated protocol server; MongoDB uses the
official driver's wire-response fixtures. FAISS Python tests use real index files.

Optional integration tests write to **isolated test resources**:

```sh
VECTOR_TRANSFER_TEST_POLIGN_URL=http://127.0.0.1:23820 \
VECTOR_TRANSFER_TEST_PGVECTOR_ENDPOINT=postgresql://localhost:23822 \
VECTOR_TRANSFER_TEST_PGVECTOR_CA=/path/to/test-ca.crt \
VECTOR_TRANSFER_TEST_FAISS_URL=http://127.0.0.1:23824 \
go test -race -timeout 60s ./...
```

The PostgreSQL fixture expects user `transfer_test`, database `postgres`, and
permission to create the vector extension and a disposable `transfer_fixture`
table, which the test truncates. FAISS expects collection `vectors`, dimension 2,
and the test-only key documented in `connector/faiss_integration_test.go`.
Polign tests use `transfer-integration-*` and `transfer-checkpoint-recovery`
collections. They check ingress, readback, and recovery after a lost write acknowledgement.
S3 Vectors → Polign was [verified through production](production-verification-2026-09-10.md)
with an AWS worker, 1,200 records, checkpoint recovery, and an S3-backed destination.
Polign, pgvector, and FAISS integrations have also been tested locally. Other provider
checks use protocol fixtures; validate your database version and schema before migration.

## Protocol references

- [S3 Vectors listing](https://docs.aws.amazon.com/AmazonS3/latest/API/API_S3VectorBuckets_ListVectors.html), [writes](https://docs.aws.amazon.com/AmazonS3/latest/API/API_S3VectorBuckets_PutVectors.html)
- [Pinecone listing](https://docs.pinecone.io/reference/api/2025-10/data-plane/list), [fetch](https://docs.pinecone.io/reference/api/2025-10/data-plane/fetch), [upsert](https://docs.pinecone.io/reference/api/2025-10/data-plane/upsert)
- [Turbopuffer export](https://turbopuffer.com/docs/export), [writes](https://turbopuffer.com/docs/write)

- [Weaviate REST](https://docs.weaviate.io/weaviate/api/rest), [Milvus query](https://milvus.io/docs/get-and-scalar-query.md), [Milvus upsert](https://milvus.io/api-reference/restful/v2.6.x/v2/Vector%20(v2)/Upsert.md)
- [Elasticsearch pagination](https://www.elastic.co/docs/reference/elasticsearch/rest-apis/paginate-search-results), [OpenSearch bulk](https://docs.opensearch.org/latest/api-reference/document-apis/bulk/)
- [pgvector](https://github.com/pgvector/pgvector), [Chroma collections](https://docs.trychroma.com/reference/python/collection), [Redis SCAN](https://redis.io/docs/latest/commands/scan/)
- [MongoDB Extended JSON](https://www.mongodb.com/docs/drivers/go/current/data-formats/extended-json/), [Solr cursors](https://solr.apache.org/guide/solr/latest/query-guide/pagination-of-results.html), [Qdrant scroll](https://api.qdrant.tech/api-reference/points/scroll-points)
