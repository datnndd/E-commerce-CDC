"""Generate a small, reproducible day of OLTP changes on an isolated database."""

import argparse
import json
import random
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import psycopg
from faker import Faker

from sources.oltp.load_olist import ROOT, connection_settings, database_settings


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--orders", type=int, default=100)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.orders < 1:
        parser.error("--orders must be positive")
    settings = database_settings()
    dbname = settings["POSTGRES_DB"]
    if not dbname.endswith("_1c_test"):
        parser.error("Stage 1C may only run against a database ending in _1c_test")

    rng = random.Random(args.seed)
    fake = Faker("pt_BR")
    fake.seed_instance(args.seed)
    now = datetime.now(timezone.utc)
    run_id = uuid4().hex[:12]
    output = args.output or ROOT / "logs" / "simulator" / f"{run_id}.json"

    with psycopg.connect(**connection_settings(settings)) as connection:
        with connection.cursor() as cur:
            cur.execute("SELECT customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state FROM customers WHERE NOT is_deleted ORDER BY customer_id")
            customers = cur.fetchall()
            addresses = {row[0]: row[2:] for row in customers}
            cur.execute("""
                SELECT DISTINCT ON (oi.product_id, oi.seller_id)
                    oi.product_id, oi.seller_id, oi.freight_value
                FROM order_items oi JOIN products p ON p.product_id = oi.product_id
                WHERE p.current_price IS NOT NULL AND p.current_price > 0
                ORDER BY oi.product_id, oi.seller_id, oi.order_id DESC
            """)
            pairs = cur.fetchall()
            cur.execute("SELECT product_id, current_price FROM products WHERE current_price IS NOT NULL AND current_price > 0 ORDER BY product_id")
            prices = dict(cur.fetchall())
            if not customers or not pairs or not prices:
                raise RuntimeError("Load the Olist snapshot before simulation")

            order_events = []
            for _ in range(args.orders):
                repeat = rng.random() < 0.8
                _, unique_id, zip_code, city, state = rng.choice(customers)
                customer_id = uuid4().hex
                if not repeat:
                    unique_id = fake.md5()
                cur.execute("""
                    INSERT INTO customers (customer_id, customer_unique_id, customer_zip_code_prefix,
                        customer_city, customer_state, created_at, updated_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                """, (customer_id, unique_id, zip_code, city, state, now, now))
                order_id = uuid4().hex
                cancelled = rng.random() < 0.05
                status = "canceled" if cancelled else "approved"
                cur.execute("""
                    INSERT INTO orders (order_id, customer_id, order_status, order_purchase_timestamp,
                        order_approved_at, order_estimated_delivery_date, created_at, updated_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                """, (order_id, customer_id, status, now, None if cancelled else now,
                      now + timedelta(days=7), now, now))
                total = Decimal("0.00")
                product_ids = []
                for item_number in range(1, rng.randint(1, 3) + 1):
                    product_id, seller_id, freight = rng.choice(pairs)
                    price = prices[product_id]
                    total += price + freight
                    product_ids.append(product_id)
                    cur.execute("""
                        INSERT INTO order_items (order_id, order_item_id, product_id, seller_id,
                            shipping_limit_date, price, freight_value, created_at, updated_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """, (order_id, item_number, product_id, seller_id, now + timedelta(days=2),
                          price, freight, now, now))
                cur.execute("""
                    INSERT INTO order_payments (order_id, payment_sequential, payment_type,
                        payment_installments, payment_value, created_at, updated_at)
                    VALUES (%s,1,'credit_card',1,%s,%s,%s)
                """, (order_id, total, now, now))
                order_events.append({"order_id": order_id, "customer_id": customer_id,
                                     "product_ids": product_ids, "status": status})

            product_ids = rng.sample(sorted(prices), max(1, round(len(prices) * 0.03)))
            for product_id in product_ids:
                new_price = (prices[product_id] * Decimal("1.05")).quantize(Decimal("0.01"))
                cur.execute("UPDATE products SET current_price=%s, updated_at=%s WHERE product_id=%s",
                            (new_price, now, product_id))

            cur.execute("SELECT customer_id FROM customers WHERE created_at < %s AND NOT is_deleted ORDER BY customer_id", (now,))
            original_ids = [row[0] for row in cur.fetchall()]
            changed_ids = rng.sample(original_ids, max(1, round(len(original_ids) * 0.02)))
            changed_set = set(changed_ids)
            deleted_ids = rng.sample([cid for cid in original_ids if cid not in changed_set],
                                     max(1, round(len(original_ids) * 0.001)))
            for customer_id in changed_ids:
                old_city, old_state = addresses[customer_id][1:]
                while True:
                    _, _, zip_code, city, state = rng.choice(customers)
                    if (city, state) != (old_city, old_state):
                        break
                cur.execute("""
                    UPDATE customers SET customer_zip_code_prefix=%s, customer_city=%s,
                        customer_state=%s, updated_at=%s WHERE customer_id=%s
                """, (zip_code, city, state, now, customer_id))
            cur.executemany("""
                UPDATE customers SET is_deleted=true, deleted_at=%s, updated_at=%s
                WHERE customer_id=%s
            """, [(now, now, customer_id) for customer_id in deleted_ids])

            report = {"run_id": run_id, "database": dbname, "at_utc": now.isoformat(),
                      "seed": args.seed, "orders_created": len(order_events),
                      "products_repriced": len(product_ids), "customers_relocated": len(changed_ids),
                      "customers_soft_deleted": len(deleted_ids), "orders": order_events}
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"Committed 1C simulation: {output}")


if __name__ == "__main__":
    main()
