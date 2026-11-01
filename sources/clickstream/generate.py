"""Generate matching clickstream JSONL from one Stage 1C simulation report."""

import argparse
import hashlib
import json
import random
from datetime import datetime, timedelta
from pathlib import Path


EVENT_TYPES = ("view_product", "add_to_cart", "begin_checkout", "purchase")
DEVICES = ("mobile", "desktop", "tablet")
UTM_SOURCES = ("direct", "search", "social")


def build_events(report: dict) -> list[dict]:
    rng = random.Random(report["seed"])
    start = datetime.fromisoformat(report["at_utc"])
    events = []
    for order in report["orders"]:
        order_id = order["order_id"]
        session_id = hashlib.sha256(order_id.encode("ascii")).hexdigest()[:32]
        device = rng.choice(DEVICES)
        utm_source = rng.choice(UTM_SOURCES)
        product_id = order["product_ids"][0]
        for offset, event_type in enumerate(EVENT_TYPES):
            event_id = hashlib.sha256(f"{order_id}:{event_type}".encode("ascii")).hexdigest()[:32]
            event = {
                "event_id": event_id,
                "event_type": event_type,
                "timestamp": (start - timedelta(minutes=3 - offset)).isoformat(),
                "session_id": session_id,
                "customer_id": order["customer_id"],
                "product_id": product_id,
                "device": device,
                "utm_source": utm_source,
            }
            if event_type == "purchase":
                event["order_id"] = order_id
            events.append(event)
    return events


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    report = json.loads(args.report.read_text(encoding="utf-8"))
    output = args.output or args.report.with_suffix(".jsonl")
    events = build_events(report)
    with output.open("w", encoding="utf-8", newline="\n") as stream:
        for event in events:
            stream.write(json.dumps(event) + "\n")
    print(f"Wrote {len(events)} events: {output}")


if __name__ == "__main__":
    main()
