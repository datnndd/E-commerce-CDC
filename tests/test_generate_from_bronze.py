"""Clickstream events must retain real snapshot keys and purchase time."""

import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from sources.clickstream.generate import build_events as build_simulator_events
from sources.clickstream.generate_from_bronze import build_events, current_cdc_rows
from ingestion.kafka_to_s3 import publish_batch
from tests.test_kafka_to_s3 import FakeS3


class GenerateFromBronzeTest(unittest.TestCase):
    def test_cdc_rows_follow_ordered_update_delete_and_reinsert(self):
        s3 = FakeS3()
        topic = "ecommerce_cdc.public.orders"
        def record(offset, op, after=None):
            return {"topic": topic, "partition": 0, "offset": offset,
                    "key": {"order_id": "order-1"}, "value": {"op": op, "after": after},
                    "ingested_at": "2026-11-10T00:00:00+00:00"}
        row = {"order_id": "order-1", "customer_id": "customer-1", "order_status": "delivered",
               "order_purchase_timestamp": "2026-11-10T00:00:00Z"}
        columns = list(row)
        with patch("ingestion.kafka_to_s3.pipeline_s3_prefix", return_value="stage9_local/"), \
             patch("sources.clickstream.generate_from_bronze.pipeline_s3_prefix", return_value="stage9_local/"):
            publish_batch(s3, "bucket", [record(0, "r", row)], "ecommerce_cdc")
            self.assertEqual(current_cdc_rows(s3, "bucket", "orders", columns)[0]["order_status"], "delivered")
            publish_batch(s3, "bucket", [record(1, "d")], "ecommerce_cdc")
            self.assertEqual(current_cdc_rows(s3, "bucket", "orders", columns), [])
            publish_batch(s3, "bucket", [record(2, "c", {**row, "order_status": "approved"})], "ecommerce_cdc")
            self.assertEqual(current_cdc_rows(s3, "bucket", "orders", columns)[0]["order_status"], "approved")

    def test_events_join_to_orders_and_items_and_are_repeatable(self):
        purchase = datetime(2018, 1, 2, 12, tzinfo=timezone.utc)
        orders = [
            {"order_id": "real-order", "customer_id": "real-customer",
             "order_status": "delivered", "order_purchase_timestamp": purchase},
            {"order_id": "canceled-order", "customer_id": "other",
             "order_status": "canceled", "order_purchase_timestamp": purchase},
        ]
        items = [{"order_id": "real-order", "product_id": "real-product"}]
        events = build_events(orders, items, 1)

        self.assertEqual([event["event_type"] for event in events],
                         ["view_product", "add_to_cart", "begin_checkout", "purchase"])
        self.assertEqual({event["customer_id"] for event in events}, {"real-customer"})
        self.assertEqual({event["product_id"] for event in events}, {"real-product"})
        self.assertEqual(events[-1]["order_id"], "real-order")
        self.assertEqual(events[-1]["timestamp"], purchase.isoformat())
        self.assertEqual(events, build_events(orders, items, 1))
        with self.assertRaisesRegex(ValueError, "Only 1 delivered orders"):
            build_events(orders, items, 2)

    def test_simulator_events_are_deterministic_for_report_seed_and_order_id(self):
        report = {
            "seed": 42,
            "at_utc": "2026-10-30T09:15:00+00:00",
            "orders": [
                {
                    "order_id": "sim-order-1",
                    "customer_id": "sim-customer-1",
                    "product_ids": ["sim-product-1", "sim-product-2"],
                    "status": "approved",
                }
            ],
        }
        first = build_simulator_events(report)
        second = build_simulator_events(report)
        self.assertEqual(first, second)
        self.assertEqual([event["event_type"] for event in first],
                         ["view_product", "add_to_cart", "begin_checkout", "purchase"])
        self.assertEqual(first[-1]["order_id"], "sim-order-1")
        self.assertEqual(len({event["event_id"] for event in first}), 4)


if __name__ == "__main__":
    unittest.main()
