"""Capture a versioned, read-only baseline for an Olist initial snapshot."""

import argparse
import hashlib
import json
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from sources.oltp.load_olist import FILES, ROOT, database_settings


METRICS = {
    "table_counts": {
        "definition": "Number of source rows in each OLTP table.",
        "unit": "rows",
        "sql": "\nUNION ALL\n".join(
            f"SELECT '{model.__tablename__}' AS table_name, count(*) AS row_count "
            f"FROM {model.__tablename__}"
            for model, _ in FILES
        ) + "\nORDER BY table_name",
    },
    "orders_by_status": {
        "definition": "Order count by the original Olist status; no status mapping applied.",
        "unit": "orders",
        "sql": """
            SELECT order_status, count(*) AS order_count
            FROM orders
            GROUP BY order_status
            ORDER BY order_status
        """,
    },
    "item_price_by_purchase_month": {
        "definition": "Sum of order_items.price by order purchase month, across all order statuses; freight and payments excluded.",
        "unit": "BRL, item price only",
        "timezone": "America/Sao_Paulo",
        "sql": """
            SELECT date_trunc('month', o.order_purchase_timestamp AT TIME ZONE 'America/Sao_Paulo')::date AS purchase_month,
                   count(*) AS item_count, sum(oi.price) AS item_price_total
            FROM order_items AS oi
            JOIN orders AS o ON o.order_id = oi.order_id
            GROUP BY 1
            ORDER BY 1
        """,
    },
    "orphan_counts": {
        "definition": "Rows in each child table whose referenced parent row is absent.",
        "unit": "rows",
        "sql": """
            SELECT 'orders.customer_id -> customers' AS relationship, count(*) AS orphan_count
            FROM orders c LEFT JOIN customers p ON p.customer_id = c.customer_id WHERE p.customer_id IS NULL
            UNION ALL
            SELECT 'order_items.order_id -> orders', count(*)
            FROM order_items c LEFT JOIN orders p ON p.order_id = c.order_id WHERE p.order_id IS NULL
            UNION ALL
            SELECT 'order_items.product_id -> products', count(*)
            FROM order_items c LEFT JOIN products p ON p.product_id = c.product_id WHERE p.product_id IS NULL
            UNION ALL
            SELECT 'order_items.seller_id -> sellers', count(*)
            FROM order_items c LEFT JOIN sellers p ON p.seller_id = c.seller_id WHERE p.seller_id IS NULL
            UNION ALL
            SELECT 'order_payments.order_id -> orders', count(*)
            FROM order_payments c LEFT JOIN orders p ON p.order_id = c.order_id WHERE p.order_id IS NULL
            UNION ALL
            SELECT 'order_reviews.order_id -> orders', count(*)
            FROM order_reviews c LEFT JOIN orders p ON p.order_id = c.order_id WHERE p.order_id IS NULL
            ORDER BY relationship
        """,
    },
}

EXPLORATION = {
    "delivered_item_price_by_category_top10": {
        "definition": "Top categories by delivered-order item price; untranslated categories use the source name.",
        "unit": "BRL, item price only",
        "sql": """
            SELECT coalesce(t.product_category_name_english, p.product_category_name, '(missing)') AS category,
                   count(*) AS item_count, sum(oi.price) AS item_price_total
            FROM order_items oi
            JOIN orders o ON o.order_id = oi.order_id
            JOIN products p ON p.product_id = oi.product_id
            LEFT JOIN product_category_name_translation t ON t.product_category_name = p.product_category_name
            WHERE o.order_status = 'delivered'
            GROUP BY 1 ORDER BY item_price_total DESC LIMIT 10
        """,
    },
    "canceled_order_rate": {
        "definition": "Raw Olist status 'canceled' divided by all orders; 'unavailable' is separate.",
        "unit": "percent of orders",
        "sql": """
            SELECT count(*) FILTER (WHERE order_status = 'canceled') AS canceled_orders,
                   count(*) AS all_orders,
                   round(100.0 * count(*) FILTER (WHERE order_status = 'canceled') / nullif(count(*), 0), 4) AS canceled_percent
            FROM orders
        """,
    },
    "review_score_by_delivery_timing": {
        "definition": "Review-row average for delivered orders with a delivery date, grouped by whether delivery exceeded its estimate.",
        "unit": "review score, 1-5",
        "sql": """
            SELECT CASE WHEN o.order_delivered_customer_date > o.order_estimated_delivery_date
                        THEN 'late' ELSE 'on_time' END AS delivery_timing,
                   count(*) AS review_rows,
                   round(avg(r.review_score)::numeric, 4) AS average_review_score
            FROM order_reviews r JOIN orders o ON o.order_id = r.order_id
            WHERE o.order_status = 'delivered' AND o.order_delivered_customer_date IS NOT NULL
            GROUP BY 1 ORDER BY 1
        """,
    },
    "top_products_by_delivered_item_price": {
        "definition": "Top products by sum of item price on delivered orders.",
        "unit": "BRL, item price only",
        "sql": """
            SELECT oi.product_id, count(*) AS item_count, sum(oi.price) AS item_price_total
            FROM order_items oi JOIN orders o ON o.order_id = oi.order_id
            WHERE o.order_status = 'delivered'
            GROUP BY oi.product_id ORDER BY item_price_total DESC, oi.product_id LIMIT 10
        """,
    },
    "top_sellers_by_delivered_item_price": {
        "definition": "Top sellers by sum of item price on delivered orders.",
        "unit": "BRL, item price only",
        "sql": """
            SELECT oi.seller_id, count(*) AS item_count, sum(oi.price) AS item_price_total
            FROM order_items oi JOIN orders o ON o.order_id = oi.order_id
            WHERE o.order_status = 'delivered'
            GROUP BY oi.seller_id ORDER BY item_price_total DESC, oi.seller_id LIMIT 10
        """,
    },
    "delivered_item_price_by_calendar_month": {
        "definition": "Delivered-order item price grouped by local purchase month number across all years; exploratory seasonality only.",
        "unit": "BRL, item price only",
        "timezone": "America/Sao_Paulo",
        "sql": """
            SELECT extract(month FROM o.order_purchase_timestamp AT TIME ZONE 'America/Sao_Paulo')::int AS calendar_month,
                   count(*) AS item_count, sum(oi.price) AS item_price_total
            FROM order_items oi JOIN orders o ON o.order_id = oi.order_id
            WHERE o.order_status = 'delivered'
            GROUP BY 1 ORDER BY 1
        """,
    },
}


def json_value(value):
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    raise TypeError(f"Cannot serialize {type(value).__name__}")


def csv_identity(dataset: Path, filename: str, source_rows: int) -> dict:
    path = dataset / filename
    with path.open("rb") as source:
        digest = hashlib.file_digest(source, "sha256").hexdigest()
    return {
        "file": filename,
        "bytes": path.stat().st_size,
        "sha256": digest,
        "source_rows": source_rows,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--load-report", type=Path, required=True)
    parser.add_argument("--dataset", type=Path, default=ROOT / "olist_dataset")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    load_report = json.loads(args.load_report.read_text(encoding="utf-8"))
    snapshot_id = f"olist_{args.load_report.parent.name}"
    output = args.output or ROOT / "docs" / "baselines" / f"{snapshot_id}.json"
    if output.exists():
        parser.error(f"Baseline already exists: {output}")
    dataset = args.dataset.resolve()
    files = {
        model.__tablename__: csv_identity(
            dataset, filename, load_report["tables"][model.__tablename__]["source"]
        )
        for model, filename in FILES
    }
    settings = database_settings()
    with psycopg.connect(
        host="127.0.0.1",
        port=int(settings["POSTGRES_PORT"]),
        dbname=settings["POSTGRES_DB"],
        user=settings["POSTGRES_USER"],
        password=settings["POSTGRES_PASSWORD"],
        row_factory=dict_row,
    ) as connection:
        with connection.transaction():
            connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            captured_at = connection.execute("SELECT transaction_timestamp() AS captured_at").fetchone()["captured_at"]
            revision = connection.execute("SELECT version_num FROM alembic_version").fetchone()["version_num"]
            metrics = {}
            for name, definition in METRICS.items():
                rows = connection.execute(definition["sql"]).fetchall()
                metrics[name] = {**definition, "rows": rows}
            counts = {row["table_name"]: row["row_count"] for row in metrics["table_counts"]["rows"]}
            for table, source in load_report["tables"].items():
                if counts[table] != source["loaded"] or source["source"] != source["loaded"] + sum(source["quarantined"].values()):
                    raise RuntimeError(f"{table}: current row count differs from load report")
            for table in counts:
                changed = connection.execute(
                    sql.SQL("SELECT count(*) AS changed FROM {} WHERE updated_at > %s").format(sql.Identifier(table)),
                    (load_report["snapshot_at_utc"],),
                ).fetchone()["changed"]
                if changed:
                    raise RuntimeError(f"{table}: {changed} rows changed after the snapshot")
            if any(row["orphan_count"] for row in metrics["orphan_counts"]["rows"]):
                raise RuntimeError("Orphan rows found in the main snapshot")
            exploration = {
                name: {**definition, "rows": connection.execute(definition["sql"]).fetchall()}
                for name, definition in EXPLORATION.items()
            }

    baseline = {
        "snapshot_id": snapshot_id,
        "source_database": settings["POSTGRES_DB"],
        "alembic_revision": revision,
        "snapshot_at_utc": load_report["snapshot_at_utc"],
        "captured_at_utc": captured_at.astimezone(timezone.utc).isoformat(),
        "source_timezone": load_report["source_timezone"],
        "csv_files": files,
        "load_report": load_report["tables"],
        "metrics": metrics,
        "exploration": exploration,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(baseline, indent=2, default=json_value) + "\n", encoding="utf-8")
    print(f"Captured {snapshot_id}: {output}")


if __name__ == "__main__":
    main()
