# FAISS adapter setup

Run this service on the machine holding the FAISS files. It exposes paged reads
and upserts through an HTTP API.

## Start

Requires Python 3.12+ and a compatible FAISS CPU wheel. From the repository root:

```sh
python3 -m venv .venv
.venv/bin/pip install -r adapters/faiss/requirements.txt
.venv/bin/python adapters/faiss/server.py --data /var/lib/faiss-transfer --dimension 1536 --collection documents
```

Before starting, inject `FAISS_TRANSFER_API_KEY` with at least 24 random characters.
The listener defaults to `127.0.0.1:23006`. Put it behind HTTPS and restrict access
to the transfer worker. Only one process can own its data directory.

## Connect

For a customer worker, configure a local connection:

```json
{
  "kind": "faiss",
  "endpoint": "https://YOUR-FAISS-ADAPTER",
  "collection": "documents",
  "api_key_env": "FAISS_TRANSFER_API_KEY"
}
```

Add its connection name to an approved pair in `worker.json`. See
[worker setup](../../docs/customer-workers.md).

For Polign-hosted execution, choose **FAISS** in **New transfer** and enter the
adapter's public HTTPS endpoint, collection, and key. The adapter uses its own
API key, not a Polign sign-in token.

## Import an existing index

Add `--import-index index.faiss --metadata-jsonl metadata.jsonl` at startup.
Import only trusted local files. The index must support `reconstruct(position)`;
the metadata file must contain one row per index position:

```json
{"id":"document-1","metadata":{"category":"manual"}}
{"id":"document-2","metadata":{"category":"reference"}}
```

A bare FAISS index does not contain arbitrary document IDs and metadata. There
is no file-upload API. Compressed indexes may reconstruct approximate vectors.

## Storage and limits

SQLite commits records before acknowledgement. Each batch also publishes an
`IndexFlatL2` snapshot and ordered metadata file. Read `CURRENT` once to locate
both files; the prior generation remains available to overlapping readers.
Startup rebuilds the snapshot after a crash. Repeated IDs replace existing records.

Rebuilding the flat index per batch limits throughput. The adapter transfers
reconstructable vectors, IDs, and metadata—not IVF/PQ topology. Protect and back
up the data directory; it contains embeddings and metadata.

Test with `.venv/bin/python -m unittest discover -s adapters/faiss`.
