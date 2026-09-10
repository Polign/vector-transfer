"""FAISS file adapter for Vector Transfer. Run behind an HTTPS reverse proxy.

SQLite owns durable IDs, embeddings and JSON metadata. Acknowledged writes also
publish a FAISS IndexFlatL2 snapshot and an ordered ID/metadata sidecar. CURRENT
atomically selects one complete generation. No pickle or remote file loading.
"""
import argparse
import fcntl
import hmac
import json
import os
from pathlib import Path
import re
import sqlite3
import threading
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit, parse_qs, unquote

import faiss
import numpy as np


def sync_dir(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class Store:
    def __init__(self, root, dimension):
        self.root = Path(root)
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.lock_file = open(self.root / "LOCK", "a")
        fcntl.flock(self.lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        self.lock = threading.RLock()
        self.dimension = dimension
        self.db = sqlite3.connect(self.root / "records.sqlite", check_same_thread=False)
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("CREATE TABLE IF NOT EXISTS settings (dimension INTEGER NOT NULL)")
        row = self.db.execute("SELECT dimension FROM settings").fetchone()
        if row and row[0] != dimension:
            raise ValueError("stored dimension differs from configuration")
        if not row:
            self.db.execute("INSERT INTO settings VALUES (?)", (dimension,))
        self.db.execute("CREATE TABLE IF NOT EXISTS records (id TEXT PRIMARY KEY, vector BLOB NOT NULL, metadata TEXT NOT NULL)")
        self.db.commit()
        self.publish()

    def publish(self):
        # Rebuild before serving after any interrupted SQL commit/export.
        rows = self.db.execute("SELECT id,vector,metadata FROM records ORDER BY id").fetchall()
        index = faiss.IndexFlatL2(self.dimension)
        if rows:
            index.add(np.stack([np.frombuffer(row[1], dtype="<f4") for row in rows]))
        generation = "generation-" + uuid.uuid4().hex
        directory = self.root / generation
        directory.mkdir(mode=0o700)
        faiss.write_index(index, str(directory / "index.faiss"))
        with open(directory / "metadata.jsonl", "w") as output:
            for row in rows:
                output.write(json.dumps({"id": row[0], "metadata": json.loads(row[2])}, allow_nan=False) + "\n")
            output.flush()
            os.fsync(output.fileno())
        with open(directory / "index.faiss", "rb") as source:
            os.fsync(source.fileno())
        sync_dir(directory)
        with open(self.root / "CURRENT.tmp", "w") as output:
            output.write(generation)
            output.flush()
            os.fsync(output.fileno())
        os.replace(self.root / "CURRENT.tmp", self.root / "CURRENT")
        sync_dir(self.root)
        # Keep the prior generation for readers that resolved CURRENT already.
        generations = sorted(self.root.glob("generation-*"), key=lambda p: p.stat().st_mtime)
        for old in generations[:-2]:
            for file in old.iterdir():
                file.unlink()
            old.rmdir()

    def read(self, offset, limit):
        with self.lock:
            rows = self.db.execute("SELECT id,vector,metadata FROM records ORDER BY id LIMIT ? OFFSET ?", (limit, offset)).fetchall()
            return {"vectors": [{"id": row[0], "values": np.frombuffer(row[1], dtype="<f4").tolist(), "metadata": json.loads(row[2])} for row in rows],
                    "total": self.db.execute("SELECT COUNT(*) FROM records").fetchone()[0]}

    def upsert(self, records):
        rows = []
        seen = set()
        for record in records:
            if set(record) - {"id", "values", "metadata"}:
                raise ValueError("unsupported record fields")
            key = record["id"]
            if not isinstance(key, str) or not key or key in seen:
                raise ValueError("IDs must be nonempty and unique per batch")
            seen.add(key)
            values = np.asarray(record["values"], dtype="<f4")
            if values.shape != (self.dimension,) or not np.isfinite(values).all():
                raise ValueError("invalid vector dimension or value")
            metadata = record.get("metadata") or {}
            if not isinstance(metadata, dict):
                raise ValueError("metadata must be an object")
            rows.append((key, values.tobytes(), json.dumps(metadata, allow_nan=False)))
        with self.lock:
            with self.db:
                self.db.executemany("INSERT INTO records VALUES (?,?,?) ON CONFLICT(id) DO UPDATE SET vector=excluded.vector,metadata=excluded.metadata", rows)
            self.publish()
        return {"ids": [row[0] for row in rows]}

    def import_index(self, index_path, metadata_path):
        # Local operator-owned files only. FAISS native deserialization is not
        # safe for untrusted uploads; the HTTP API never exposes this operation.
        index = faiss.read_index(str(index_path))
        metadata = [json.loads(line) for line in Path(metadata_path).read_text().splitlines()]
        if index.d != self.dimension or index.ntotal != len(metadata):
            raise ValueError("index and sidecar dimension/count mismatch")
        records = [{"id": item["id"], "values": index.reconstruct(i).tolist(), "metadata": item.get("metadata", {})} for i, item in enumerate(metadata)]
        self.upsert(records)


def handler(store, collection, token):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass  # No request paths, headers, credentials, or record bodies.

        def reply(self, status, value):
            data = json.dumps(value, allow_nan=False).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def route(self, write):
            if not hmac.compare_digest(self.headers.get("Authorization", ""), "Bearer " + token):
                return self.reply(401, {"error": "unauthorized"})
            parsed = urlsplit(self.path)
            expected = "/v1/collections/" + collection + ("/vectors:batch" if write else "/vectors")
            if unquote(parsed.path) != expected:
                return self.reply(404, {"error": "not found"})
            try:
                if write:
                    size = int(self.headers.get("Content-Length", "0"))
                    if size <= 0 or size > 8 << 20:
                        return self.reply(413, {"error": "invalid request size"})
                    body = json.loads(self.rfile.read(size))
                    if set(body) != {"vectors"} or not isinstance(body["vectors"], list) or len(body["vectors"]) > 500:
                        raise ValueError("invalid batch")
                    result = store.upsert(body["vectors"])
                else:
                    query = parse_qs(parsed.query)
                    limit, offset = int(query.get("limit", ["100"])[0]), int(query.get("offset", ["0"])[0])
                    if limit < 1 or limit > 10000 or offset < 0:
                        raise ValueError("invalid pagination")
                    result = store.read(offset, limit)
                self.reply(200, result)
            except (ValueError, KeyError, TypeError):
                self.reply(400, {"error": "invalid vectors, metadata, or pagination"})
            except Exception:
                self.reply(503, {"error": "FAISS storage operation failed"})

        def do_GET(self):
            self.route(False)

        def do_POST(self):
            self.route(True)
    return Handler


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", required=True)
    parser.add_argument("--dimension", required=True, type=int)
    parser.add_argument("--collection", default="vectors")
    parser.add_argument("--listen", default="127.0.0.1")
    parser.add_argument("--port", default=23006, type=int)
    parser.add_argument("--import-index")
    parser.add_argument("--metadata-jsonl")
    args = parser.parse_args()
    token = os.environ.get("FAISS_TRANSFER_API_KEY", "")
    if len(token) < 24 or args.dimension < 1 or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", args.collection):
        parser.error("set a random FAISS_TRANSFER_API_KEY (24+ characters), valid collection, and positive dimension")
    if bool(args.import_index) != bool(args.metadata_jsonl):
        parser.error("--import-index and --metadata-jsonl must be supplied together")
    store = Store(args.data, args.dimension)
    if args.import_index:
        store.import_index(args.import_index, args.metadata_jsonl)
    server = ThreadingHTTPServer((args.listen, args.port), handler(store, args.collection, token))
    server.serve_forever()


if __name__ == "__main__":
    main()
