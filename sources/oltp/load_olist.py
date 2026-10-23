"""Load the nine Olist CSV files into an empty, migrated PostgreSQL database."""

import argparse
import csv
import io
import json
import math
import os
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from uuid import uuid4
from zoneinfo import ZoneInfo

import psycopg
from dotenv import dotenv_values
from psycopg import sql
from sqlalchemy import DateTime, Float, Integer, Numeric, String

from sources.oltp.models import (
    CategoryNameTranslation,
    Customer,
    Geolocation,
    Order,
    OrderItem,
    OrderPayment,
    OrderReview,
    Product,
    Seller,
)


ROOT = Path(__file__).resolve().parents[2]
FILES = (
    (Customer, "olist_customers_dataset.csv"),
    (Geolocation, "olist_geolocation_dataset.csv"),
    (CategoryNameTranslation, "product_category_name_translation.csv"),
    (Product, "olist_products_dataset.csv"),
    (Seller, "olist_sellers_dataset.csv"),
    (Order, "olist_orders_dataset.csv"),
    (OrderItem, "olist_order_items_dataset.csv"),
    (OrderPayment, "olist_order_payments_dataset.csv"),
    (OrderReview, "olist_order_reviews_dataset.csv"),
)
GENERATED = {"created_at", "updated_at", "geolocation_id", "review_row_id", "is_deleted", "deleted_at", "current_price"}
BATCH_ROWS = 10_000


def database_settings() -> dict[str, str]:
    local = dotenv_values(ROOT / ".env")
    settings = {}
    for name in ("POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB", "POSTGRES_PORT"):
        value = os.environ.get(name) or local.get(name)
        if not value or value == "replace-with-a-local-password":
            raise ValueError(f"Set {name} in the environment or .env")
        settings[name] = value
    return settings


def source_columns(table) -> list:
    return [column for column in table.columns if column.name not in GENERATED]


def normalize(row: dict, columns: list, source_zone: ZoneInfo) -> dict:
    values = {}
    for column in columns:
        name = column.name
        raw = row[name]
        if raw is None or raw == "":
            if not column.nullable:
                raise ValueError(f"{name} is empty")
            values[name] = None
            continue
        try:
            if isinstance(column.type, DateTime):
                parsed = datetime.fromisoformat(raw)
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=source_zone)
                values[name] = parsed.astimezone(timezone.utc)
            elif isinstance(column.type, Integer):
                values[name] = int(raw)
            elif isinstance(column.type, Float):
                number = float(raw)
                if not math.isfinite(number):
                    raise ValueError("invalid float value")
                values[name] = number
            elif isinstance(column.type, Numeric):
                amount = Decimal(raw)
                if not amount.is_finite() or amount.as_tuple().exponent < -column.type.scale:
                    raise ValueError("invalid money value")
                values[name] = amount
            else:
                if isinstance(column.type, String) and column.type.length is not None and len(raw) > column.type.length:
                    raise ValueError("value exceeds column length")
                values[name] = raw
        except (ValueError, OverflowError, InvalidOperation) as exc:
            raise ValueError(f"{name}: {exc}") from exc
    return values


def timestamps(table_name: str, values: dict, snapshot_at: datetime) -> tuple[datetime, datetime]:
    if table_name == "orders":
        created = values["order_purchase_timestamp"]
        changed = max(
            values[name] or created
            for name in (
                "order_purchase_timestamp",
                "order_approved_at",
                "order_delivered_carrier_date",
                "order_delivered_customer_date",
            )
        )
        return created, changed
    if table_name == "order_reviews":
        return values["review_creation_date"], values["review_answer_timestamp"]
    return snapshot_at, snapshot_at


def load_table(
    cursor,
    model,
    path: Path,
    accepted_ids: dict[str, set[str]],
    source_zone: ZoneInfo,
    snapshot_at: datetime,
    run_dir: Path,
) -> dict:
    table = model.__table__
    name = table.name
    columns = source_columns(table)
    field_names = [column.name for column in columns]
    primary_key = [column.name for column in table.primary_key.columns]
    has_source_key = all(key in field_names for key in primary_key)
    seen_keys = set()
    source_count = loaded = 0
    rejected = Counter()
    quarantine = None
    quarantine_writer = None
    copy_columns = field_names + ["created_at", "updated_at"]
    copy_query = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT CSV)").format(
        sql.Identifier(name), sql.SQL(", ").join(map(sql.Identifier, copy_columns))
    )

    try:
        with path.open(encoding="utf-8-sig", newline="") as source:
            reader = csv.DictReader(source)
            if reader.fieldnames is None or set(reader.fieldnames) != set(field_names):
                raise ValueError(f"{path.name}: CSV headers do not match {name} model")
            with cursor.copy(copy_query) as copy:
                batch = io.StringIO()
                writer = csv.writer(batch, lineterminator="\n")
                buffered = 0
                for line_number, row in enumerate(reader, start=2):
                    source_count += 1
                    try:
                        if None in row:
                            raise ValueError("extra CSV fields")
                        values = normalize(row, columns, source_zone)
                        if has_source_key:
                            key = tuple(values[column] for column in primary_key)
                            if key in seen_keys:
                                raise ValueError("duplicate primary key")
                        for fk in table.foreign_keys:
                            parent = fk.column.table.name
                            if values[fk.parent.name] not in accepted_ids[parent]:
                                raise ValueError(f"missing {parent}.{fk.column.name}")
                    except ValueError as exc:
                        reason = str(exc)
                        rejected[reason] += 1
                        if quarantine is None:
                            run_dir.mkdir(parents=True, exist_ok=True)
                            quarantine = (run_dir / f"{name}_quarantine.csv").open(
                                "w", encoding="utf-8", newline=""
                            )
                            quarantine_writer = csv.writer(quarantine)
                            quarantine_writer.writerow(["line", "reason", *reader.fieldnames])
                        quarantine_writer.writerow(
                            [line_number, reason, *(row.get(field) for field in reader.fieldnames)]
                        )
                        continue

                    created, updated = timestamps(name, values, snapshot_at)
                    writer.writerow([values[field] for field in field_names] + [created, updated])
                    loaded += 1
                    buffered += 1
                    if has_source_key:
                        seen_keys.add(key)
                        if len(primary_key) == 1:
                            accepted_ids[name].add(key[0])
                    if buffered == BATCH_ROWS:
                        copy.write(batch.getvalue())
                        batch = io.StringIO()
                        writer = csv.writer(batch, lineterminator="\n")
                        buffered = 0
                if buffered:
                    copy.write(batch.getvalue())
    finally:
        if quarantine is not None:
            quarantine.close()

    cursor.execute(sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(name)))
    actual = cursor.fetchone()[0]
    if actual != loaded:
        raise RuntimeError(f"{name}: loaded {loaded} rows but found {actual} in PostgreSQL")
    print(f"{name}: {loaded}/{source_count} loaded, {sum(rejected.values())} quarantined")
    return {"source": source_count, "loaded": loaded, "quarantined": dict(rejected)}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", type=Path, default=ROOT / "olist_dataset")
    parser.add_argument("--source-timezone", default="America/Sao_Paulo")
    parser.add_argument("--report-dir", type=Path, default=ROOT / "logs" / "olist_load")
    args = parser.parse_args()
    source_zone = ZoneInfo(args.source_timezone)
    dataset = args.dataset.resolve()
    missing = [file for _, file in FILES if not (dataset / file).is_file()]
    if missing:
        parser.error(f"Missing CSV files in {dataset}: {', '.join(missing)}")

    settings = database_settings()
    snapshot_at = datetime.now(timezone.utc)
    run_dir = args.report_dir / f"{snapshot_at:%Y%m%dT%H%M%SZ}_{uuid4().hex[:8]}"
    accepted_ids: dict[str, set[str]] = {model.__tablename__: set() for model, _ in FILES}
    report = {}
    with psycopg.connect(
        host="127.0.0.1",
        port=int(settings["POSTGRES_PORT"]),
        dbname=settings["POSTGRES_DB"],
        user=settings["POSTGRES_USER"],
        password=settings["POSTGRES_PASSWORD"],
    ) as connection:
        with connection.cursor() as cursor:
            for model, _ in FILES:
                table_name = model.__tablename__
                cursor.execute(
                    sql.SQL("SELECT EXISTS(SELECT 1 FROM {} LIMIT 1)").format(
                        sql.Identifier(table_name)
                    )
                )
                if cursor.fetchone()[0]:
                    raise RuntimeError(f"{table_name} is not empty; snapshot load requires empty tables")
            for model, file in FILES:
                report[model.__tablename__] = load_table(
                    cursor, model, dataset / file, accepted_ids, source_zone, snapshot_at, run_dir
                )
            cursor.execute("""
                UPDATE products AS p SET current_price = latest.price
                FROM (
                    SELECT DISTINCT ON (oi.product_id) oi.product_id, oi.price
                    FROM order_items AS oi
                    JOIN orders AS o ON o.order_id = oi.order_id
                    ORDER BY oi.product_id, o.order_purchase_timestamp DESC, oi.order_id DESC, oi.order_item_id DESC
                ) AS latest
                WHERE p.product_id = latest.product_id
            """)

    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "report.json").write_text(
        json.dumps(
            {
                "snapshot_at_utc": snapshot_at.isoformat(),
                "source_timezone": args.source_timezone,
                "tables": report,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"Committed snapshot load. Report: {run_dir / 'report.json'}")


if __name__ == "__main__":
    main()
