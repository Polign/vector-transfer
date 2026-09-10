import json
import tempfile
import unittest
from pathlib import Path

import faiss
import numpy as np

from server import Store


class FileAdapterTest(unittest.TestCase):
    def test_replay_reopen_and_real_faiss_snapshot(self):
        with tempfile.TemporaryDirectory() as root:
            store = Store(root, 2)
            records = [{"id": "b", "values": [1, 2], "metadata": {"count": 9007199254740993}}, {"id": "a", "values": [3, 4], "metadata": {"tag": "kept"}}]
            for _ in range(2):
                self.assertEqual(store.upsert(records)["ids"], ["b", "a"])
            first = store.read(0, 1)
            self.assertEqual(first["total"], 2)
            self.assertEqual(first["vectors"][0]["id"], "a")
            generation = Path(root) / (Path(root) / "CURRENT").read_text()
            index = faiss.read_index(str(generation / "index.faiss"))
            self.assertEqual(index.ntotal, 2)
            np.testing.assert_array_equal(index.reconstruct(0), [3, 4])
            sidecar = [json.loads(line) for line in (generation / "metadata.jsonl").read_text().splitlines()]
            self.assertEqual(sidecar[1]["metadata"]["count"], 9007199254740993)
            store.db.close()
            store.lock_file.close()
            reopened = Store(root, 2)
            self.assertEqual(reopened.read(1, 1)["vectors"], records[:1])
            reopened.upsert([{"id": "b", "values": [5, 6]}])
            self.assertEqual(reopened.read(1, 1)["vectors"][0]["metadata"], {})
            with self.assertRaises(ValueError):
                reopened.upsert([{"id": "bad", "values": [1]}])
            reopened.db.close()
            reopened.lock_file.close()

    def test_import_index_with_exact_ids_and_metadata(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            index = faiss.IndexFlatL2(2)
            index.add(np.array([[1, 2], [3, 4]], dtype="float32"))
            faiss.write_index(index, str(root / "original.faiss"))
            (root / "metadata.jsonl").write_text('{"id":"x","metadata":{"tag":"kept"}}\n{"id":"y"}\n')
            store = Store(root / "adapter", 2)
            store.import_index(root / "original.faiss", root / "metadata.jsonl")
            self.assertEqual([r["id"] for r in store.read(0, 10)["vectors"]], ["x", "y"])
            store.db.close()
            store.lock_file.close()


if __name__ == "__main__":
    unittest.main()
