"""Generate demo clickstream from committed RDS order snapshots in S3 Bronze."""

import argparse
import hashlib
import io
import json
from datetime import timedelta
from pathlib import Path

import pyarrow.parquet as pq

from ingestion.s3 import s3_client_and_bucket


def snapshot_rows(s3, bucket: str, table: str, columns: list[str]) -> list[dict]:
    prefix = f"bronze/postgres_oltp/{table}/"
    manifests = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/_manifest.json"):
                manifest = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
                if manifest.get("status") == "committed" and manifest.get("load_type") == "snapshot":
                    manifests.append((key, manifest))
    if len(manifests) != 1:
        raise ValueError(f"Expected one committed {table} snapshot, found {len(manifests)}")

    key, manifest = manifests[0]
    rows = []
    for file in manifest["files"]:
        data = s3.get_object(Bucket=bucket, Key=key.rsplit("/", 1)[0] + "/" + file["name"])["Body"].read()
        if len(data) != file["size_bytes"] or hashlib.sha256(data).hexdigest() != file["sha256"]:
            raise ValueError(f"{table}: snapshot file failed manifest integrity check")
        rows.extend(pq.read_table(io.BytesIO(data), columns=columns).to_pylist())
    if len(rows) != manifest["row_count"]:
        raise ValueError(f"{table}: snapshot row count differs from manifest")
    return rows


def build_events(orders: list[dict], items: list[dict], count: int) -> list[dict]:
    if count < 1:
        raise ValueError("count must be positive")
    first_product = {}
    for item in items:
        first_product.setdefault(item["order_id"], item["product_id"])
    eligible = sorted(
        (order for order in orders if order["order_status"] == "delivered"
         and order["order_id"] in first_product and order["order_purchase_timestamp"]),
        key=lambda order: (order["order_purchase_timestamp"], order["order_id"]),
    )
    if len(eligible) < count:
        raise ValueError(f"Only {len(eligible)} delivered orders have an item and purchase timestamp")
    selected = [eligible[index * len(eligible) // count] for index in range(count)]
    events = []
    for order in selected:
        order_id = order["order_id"]
        digest = hashlib.sha256(order_id.encode("ascii")).hexdigest()
        for offset, event_type in enumerate(("view_product", "add_to_cart", "begin_checkout", "purchase")):
            event = {
                "event_id": hashlib.sha256(f"{order_id}:{event_type}".encode("ascii")).hexdigest()[:32],
                "event_type": event_type,
                "timestamp": (order["order_purchase_timestamp"] - timedelta(minutes=3 - offset)).isoformat(),
                "session_id": digest[:32],
                "customer_id": order["customer_id"],
                "product_id": first_product[order_id],
                "device": ("mobile", "desktop", "tablet")[int(digest[:2], 16) % 3],
                "utm_source": ("direct", "search", "social")[int(digest[2:4], 16) % 3],
            }
            if event_type == "purchase":
                event["order_id"] = order_id
            events.append(event)
    return events


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", required=True, help="Project S3 bucket with committed RDS snapshots")
    parser.add_argument("--output", type=Path, required=True, help="New JSONL file to create")
    parser.add_argument("--count", type=int, default=20, help="Number of delivered orders to sample")
    args = parser.parse_args()
    s3, _ = s3_client_and_bucket()
    orders = snapshot_rows(s3, args.bucket, "orders", ["order_id", "customer_id", "order_status", "order_purchase_timestamp"])
    items = snapshot_rows(s3, args.bucket, "order_items", ["order_id", "product_id"])
    events = build_events(orders, items, args.count)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8", newline="\n") as stream:
        for event in events:
            stream.write(json.dumps(event) + "\n")
    print(f"Wrote {len(events)} events from {args.count} RDS snapshot orders: {args.output}")


if __name__ == "__main__":
    main()
