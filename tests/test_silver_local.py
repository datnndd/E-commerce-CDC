"""Local Bronze-to-Delta integration proof; run inside the Silver Docker image."""

import gzip
import hashlib
import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from spark_jobs.silver_transform import KEYS, OLTP_NAMES, SILVER_NAMES, local_spark, run_dataset


@unittest.skipUnless(os.environ.get("SILVER_TEST_DOCKER") == "1", "run with SILVER_TEST_DOCKER=1 in the local Silver Docker image")
class SilverLocalTest(unittest.TestCase):
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

    def publish(self, dataset, run_id, rows, load_type="snapshot", committed=True):
        if dataset in OLTP_NAMES:
            run = self.root / "bronze" / "postgres_oltp" / dataset / "load_date=2026-11-01" / f"run_id={run_id}"
            file_name = "part-0000.parquet"
        elif dataset == "clickstream":
            run = self.root / "bronze" / "clickstream" / "dt=2026-11-01" / "hour=00" / f"run_id={run_id}"
            file_name = "events.json.gz"
        else:
            run = self.root / "bronze" / "api" / dataset / "dt=2026-11-01" / f"run_id={run_id}"
            file_name = "response.json"
        run.mkdir(parents=True)
        file_path = run / file_name
        if dataset in OLTP_NAMES:
            pq.write_table(pa.Table.from_pylist(rows), file_path)
        elif dataset == "clickstream":
            with gzip.open(file_path, "wt", encoding="utf-8") as stream:
                for row in rows:
                    stream.write(json.dumps(row) + "\n")
        else:
            file_path.write_text(json.dumps(rows), encoding="utf-8")
        body = file_path.read_bytes()
        file = {"name": file_name, "size_bytes": len(body), "sha256": hashlib.sha256(body).hexdigest()}
        if dataset in OLTP_NAMES or dataset == "clickstream":
            file["row_count"] = len(rows)
        manifest = {
            "run_id": run_id,
            "source_system": "postgres_oltp" if dataset in OLTP_NAMES else ("clickstream" if dataset == "clickstream" else "api"),
            "dataset": "events" if dataset == "clickstream" else dataset,
            "load_type": load_type,
            "status": "committed" if committed else "writing",
            "started_at": "2026-11-01T00:00:00Z",
            "files": [file],
        }
        if dataset in OLTP_NAMES or dataset == "clickstream":
            manifest["row_count"] = len(rows)
        (run / "_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
        return file_path

    def table_rows(self, name):
        return self.spark.read.format("delta").load(str(self.silver / name)).collect()

    def test_all_twelve_tables_replay_overlap_late_update_and_quarantine(self):
        early = datetime(2026, 1, 1, tzinfo=timezone.utc)
        later = datetime(2026, 1, 2, tzinfo=timezone.utc)
        for dataset in OLTP_NAMES:
            row = {key: (1 if key in ("geolocation_id", "review_row_id", "order_item_id", "payment_sequential") else f"{key}-1")
                   for key in KEYS[dataset]}
            row.update(updated_at=early, ingested_at=early)
            if dataset == "orders":
                row.update(customer_id="customer-1", order_status="CREATED")
            if dataset == "order_items":
                row.update(product_id="product-1", price=10.0)
            if dataset == "products":
                row["current_price"] = 10.0
            if dataset == "customers":
                row["is_deleted"] = False
            self.publish(dataset, "20261101T000000", [row])

        self.publish("orders", "20261101T000100", [
            {"order_id": "order_id-1", "customer_id": "customer-1", "order_status": "SHIPPED", "updated_at": later, "ingested_at": later},
            {"order_id": "order-2", "customer_id": "customer-2", "order_status": "CREATED", "updated_at": later, "ingested_at": later},
            {"order_id": "bad-order", "customer_id": None, "order_status": "CREATED", "updated_at": later, "ingested_at": later},
        ], load_type="delta")
        self.publish("order_items", "20261101T000100", [
            {"order_id": "order-2", "order_item_id": 1, "product_id": "product-1", "price": -3.0,
             "updated_at": later, "ingested_at": later},
        ], load_type="delta")
        self.publish("products", "20261101T000100", [
            {"product_id": "product_id-1", "current_price": 20.0,
             "updated_at": later, "ingested_at": later},
        ], load_type="delta")
        self.publish("customers", "20261101T000100", [
            {"customer_id": "customer_id-1", "is_deleted": True,
             "updated_at": later, "ingested_at": later},
        ], load_type="delta")
        self.publish("clickstream", "20261101T000000", [
            {"event_id": "event-1", "timestamp": "2026-01-01T00:00:00Z", "event_type": "purchase",
             "session_id": "session-1", "customer_id": "customer-1", "product_id": "product-1", "order_id": "order_id-1"},
            {"event_id": "event-bad", "timestamp": "invalid", "event_type": "view_product",
             "session_id": "session-2", "customer_id": "customer-1", "product_id": "product-1", "order_id": None},
        ])
        self.publish("exchange_rate", "20261101T000000", [
            {"date": "2026-01-01", "base": "BRL", "quote": "USD", "rate": 0.2},
            {"date": "2026-01-02", "base": "BRL", "quote": "USD", "rate": "invalid"},
        ])
        self.publish("campaign", "20261101T000000", {
            "synthetic": True,
            "campaigns": [{"campaign_id": "campaign-1", "utm_source": "social", "discount_pct": 10},
                          {"campaign_id": "campaign-bad", "utm_source": "search", "discount_pct": "invalid"}],
        })

        for dataset in SILVER_NAMES:
            run_dataset(self.spark, self.root, self.silver, dataset)
        first_counts = {dataset: len(self.table_rows(name)) for dataset, name in SILVER_NAMES.items()}
        self.assertEqual(len(first_counts), 12)
        self.assertEqual(first_counts["orders"], 2)
        self.assertEqual(first_counts["order_items"], 1)
        self.assertTrue(all(count >= 1 for count in first_counts.values()))
        self.assertEqual(len(self.table_rows("silver_quarantine")), 5)
        orders = {row.order_id: row for row in self.table_rows("slv_orders")}
        self.assertEqual(orders["order_id-1"].order_status, "shipped")
        self.assertEqual(self.table_rows("slv_products")[0].current_price, 20.0)
        self.assertTrue(self.table_rows("slv_customers")[0].is_deleted)

        for dataset in SILVER_NAMES:
            run_dataset(self.spark, self.root, self.silver, dataset)
        self.assertEqual(first_counts, {dataset: len(self.table_rows(name)) for dataset, name in SILVER_NAMES.items()})
        self.assertEqual(len(self.table_rows("silver_quarantine")), 5)

        self.publish("orders", "20261101T000200", [
            {"order_id": "order_id-1", "customer_id": "customer-1", "order_status": "CANCELED",
             "updated_at": early, "ingested_at": later},
        ], load_type="delta")
        run_dataset(self.spark, self.root, self.silver, "orders")
        orders = {row.order_id: row for row in self.table_rows("slv_orders")}
        self.assertEqual(len(orders), 2)
        self.assertEqual(orders["order_id-1"].order_status, "shipped")
        self.publish("products", "20261101T000200", [
            {"product_id": "product_id-1", "current_price": 5.0,
             "updated_at": early, "ingested_at": later},
        ], load_type="delta")
        run_dataset(self.spark, self.root, self.silver, "products")
        self.assertEqual(self.table_rows("slv_products")[0].current_price, 20.0)

    def test_uncommitted_and_unlisted_files_ignored_but_bad_hash_rejected(self):
        early = datetime(2026, 1, 1, tzinfo=timezone.utc)
        row = {"order_id": "o1", "customer_id": "c1", "order_status": "created",
               "updated_at": early, "ingested_at": early}
        path = self.publish("orders", "20261101T000000", [row])
        (path.parent / "unlisted.parquet").write_bytes(b"junk")
        self.publish("orders", "20261101T000100", [{**row, "order_id": "ignored"}], committed=False)
        run_dataset(self.spark, self.root, self.silver, "orders")
        self.assertEqual([row.order_id for row in self.table_rows("slv_orders")], ["o1"])
        path.write_bytes(b"corrupt")
        with self.assertRaisesRegex(ValueError, "SHA-256 differs from manifest"):
            run_dataset(self.spark, self.root, self.silver, "orders")
        self.assertEqual([row.order_id for row in self.table_rows("slv_orders")], ["o1"])

    def test_negative_payment_rejected_before_silver_merge(self):
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        self.publish("order_payments", "20261101T000000", [{
            "order_id": "o1", "payment_sequential": 1, "payment_value": -1.0,
            "updated_at": now, "ingested_at": now,
        }])
        with self.assertRaisesRegex(ValueError, "negative payment values"):
            run_dataset(self.spark, self.root, self.silver, "order_payments")
        self.assertFalse((self.silver / "slv_payments").exists())


if __name__ == "__main__":
    unittest.main()
