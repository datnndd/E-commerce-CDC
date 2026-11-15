"""Regenerate the deterministic demo clickstream from committed OLTP snapshots."""

import json
import os
import tempfile
from pathlib import Path

from ingestion.ingest_logs import ingest_logs
from ingestion.s3 import s3_client_and_bucket
from sources.clickstream.generate_from_bronze import build_events, current_cdc_rows, snapshot_rows


def main() -> None:
    s3, bucket = s3_client_and_bucket()
    source_rows = current_cdc_rows if os.environ.get("CDC_MODE") == "debezium" else snapshot_rows
    orders = source_rows(
        s3, bucket, "orders", ["order_id", "customer_id", "order_status", "order_purchase_timestamp"]
    )
    items = source_rows(s3, bucket, "order_items", ["order_id", "product_id"])
    events = build_events(orders, items, 20)
    with tempfile.TemporaryDirectory() as directory:
        source = Path(directory) / "demo_clickstream.jsonl"
        with source.open("w", encoding="utf-8", newline="\n") as stream:
            for event in events:
                stream.write(json.dumps(event) + "\n")
        manifest = ingest_logs(source, s3, bucket)
    print(f"Published {manifest['row_count']} deterministic demo events")


if __name__ == "__main__":
    main()
