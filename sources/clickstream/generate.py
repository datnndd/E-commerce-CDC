"""Generate matching clickstream JSONL from one Stage 1C simulation report."""

import argparse
import json
import random
from datetime import datetime, timedelta
from pathlib import Path
from uuid import uuid4


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    report = json.loads(args.report.read_text(encoding="utf-8"))
    output = args.output or args.report.with_suffix(".jsonl")
    rng = random.Random(report["seed"])
    start = datetime.fromisoformat(report["at_utc"])
    with output.open("w", encoding="utf-8", newline="\n") as stream:
        for order in report["orders"]:
            session_id = uuid4().hex
            device = rng.choice(["mobile", "desktop", "tablet"])
            utm_source = rng.choice(["direct", "search", "social"])
            product_id = order["product_ids"][0]
            for offset, event_type in enumerate(("view_product", "add_to_cart", "begin_checkout", "purchase")):
                event = {"event_id": uuid4().hex, "event_type": event_type,
                         "timestamp": (start - timedelta(minutes=3 - offset)).isoformat(),
                         "session_id": session_id, "customer_id": order["customer_id"],
                         "product_id": product_id, "device": device, "utm_source": utm_source}
                if event_type == "purchase":
                    event["order_id"] = order["order_id"]
                stream.write(json.dumps(event) + "\n")
    print(f"Wrote {len(report['orders']) * 4} events: {output}")


if __name__ == "__main__":
    main()
