"""Local Delta proof for offset ordering and physical Debezium deletes."""

import hashlib
import json
import os
import tempfile
import unittest
from pathlib import Path

from spark_jobs.silver_transform import local_spark, run_cdc_dataset, save_cdc_checkpoint


@unittest.skipUnless(os.environ.get("SILVER_TEST_DOCKER") == "1", "run in the Silver Docker image")
class SilverCdcLocalTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = local_spark()
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.silver = self.root / "silver"

    def publish(self, offset: int, op: str | None, after: dict | None = None,
                wrapped: bool = False) -> None:
        topic = "ecommerce_cdc.public.order_reviews"
        run = (self.root / "bronze" / "postgres_debezium" / "order_reviews" /
               f"topic={topic}" / "partition=0" / f"offset={offset:020d}-{offset:020d}")
        run.mkdir(parents=True)
        key = {"review_row_id": 1}
        value = ({"op": op, "before": {"review_row_id": 1} if op == "d" else None,
                  "after": after, "source": {"lsn": 100 + offset}} if op else None)
        if wrapped:
            key = {"schema": {"type": "struct"}, "payload": key}
            if value is not None:
                value = {"schema": {"type": "struct"}, "payload": value}
        record = {
            "topic": topic, "partition": 0, "offset": offset,
            "key": key, "value": value,
            "ingested_at": "2026-11-10T00:00:00+00:00",
        }
        data = (json.dumps(record) + "\n").encode()
        (run / "events.jsonl").write_bytes(data)
        manifest = {
            "run_id": run.name, "source_system": "postgres_debezium", "dataset": "order_reviews",
            "status": "committed", "topic": topic, "partition": 0,
            "offset_start": offset, "offset_end": offset, "row_count": 1,
            "files": [{"name": "events.jsonl", "row_count": 1,
                       "size_bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}],
        }
        (run / "_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    def rows(self):
        return self.spark.read.format("delta").load(str(self.silver / "slv_reviews")).collect()

    def test_update_uses_offset_and_delete_survives_replay(self):
        row = {"review_row_id": 1, "review_id": "r1", "order_id": "o1",
               "review_score": 4, "updated_at": "2026-11-10T00:00:00Z"}
        self.publish(0, "r", row, wrapped=True)
        self.assertEqual(run_cdc_dataset(self.spark, str(self.root), str(self.silver), "order_reviews"), (1, 0, 0))
        self.assertEqual(self.rows()[0].review_score, 4)
        save_cdc_checkpoint(str(self.silver), "order_reviews", 0)

        self.publish(1, "u", {**row, "review_score": 5, "updated_at": "2026-11-09T00:00:00Z"})
        run_cdc_dataset(self.spark, str(self.root), str(self.silver), "order_reviews")
        self.assertEqual(self.rows()[0].review_score, 5)
        save_cdc_checkpoint(str(self.silver), "order_reviews", 1)

        self.publish(2, "d", wrapped=True)
        run_cdc_dataset(self.spark, str(self.root), str(self.silver), "order_reviews")
        self.assertEqual(self.rows(), [])
        run_cdc_dataset(self.spark, str(self.root), str(self.silver), "order_reviews")
        self.assertEqual(self.rows(), [])
        save_cdc_checkpoint(str(self.silver), "order_reviews", 2)

        self.publish(3, None, wrapped=True)
        self.assertEqual(run_cdc_dataset(self.spark, str(self.root), str(self.silver), "order_reviews"), (0, 0, 3))
        save_cdc_checkpoint(str(self.silver), "order_reviews", 3)

        self.publish(4, "c", {**row, "review_score": 2})
        run_cdc_dataset(self.spark, str(self.root), str(self.silver), "order_reviews")
        self.assertEqual(self.rows()[0].review_score, 2)


if __name__ == "__main__":
    unittest.main()
