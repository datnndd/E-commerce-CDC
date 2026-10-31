"""Clickstream events must retain real snapshot keys and purchase time."""

import unittest
from datetime import datetime, timezone

from sources.clickstream.generate_from_bronze import build_events


class GenerateFromBronzeTest(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
