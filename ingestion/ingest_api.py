"""Fetch JSON API responses and publish the raw bytes to S3 Bronze."""

import argparse
import hashlib
import json
import os
import re
import time
from datetime import datetime, timezone
from urllib.error import HTTPError
from urllib.request import urlopen

from dotenv import dotenv_values

from ingestion.s3 import ROOT, pipeline_s3_prefix, s3_client_and_bucket


FRANKFURTER_RATES_URL = "https://api.frankfurter.dev/v2/rates"


def exchange_rate_url(
    base: str | None = None,
    quotes: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> str:
    local = dotenv_values(ROOT / ".env")
    resolved_base = (
        base or os.environ.get("EXCHANGE_BASE_CURRENCY") or local.get("EXCHANGE_BASE_CURRENCY") or "BRL"
    ).strip().upper()
    if not re.fullmatch(r"[A-Z]{3}", resolved_base):
        raise ValueError(f"Invalid base currency code: {resolved_base!r}")

    raw_quotes = (
        quotes or os.environ.get("EXCHANGE_QUOTE_CURRENCIES") or local.get("EXCHANGE_QUOTE_CURRENCIES") or "USD"
    )
    resolved_quotes = ",".join(part.strip().upper() for part in raw_quotes.split(",") if part.strip())
    if not re.fullmatch(r"[A-Z]{3}(?:,[A-Z]{3})*", resolved_quotes):
        raise ValueError(f"Invalid quote currency code(s): {raw_quotes!r}")

    params = []
    if start_date:
        params.append(f"from={start_date}")
    if end_date:
        params.append(f"to={end_date}")
    params.append(f"base={resolved_base}")
    params.append(f"quotes={resolved_quotes}")
    return f"{FRANKFURTER_RATES_URL}?{'&'.join(params)}"


def fetch_json(url: str) -> bytes:
    for attempt in range(4):
        try:
            with urlopen(url, timeout=15) as response:
                body = response.read()
            json.loads(body)
            return body
        except HTTPError as exc:
            if 400 <= exc.code < 500 and exc.code != 429:
                raise RuntimeError(f"API returned HTTP {exc.code}") from None
        except (OSError, json.JSONDecodeError, UnicodeDecodeError):
            pass
        if attempt == 3:
            raise RuntimeError("API did not return valid JSON after 4 attempts") from None
        time.sleep(2 ** attempt)


def ingest_api(source: str, url: str, s3, bucket: str, started_at: datetime | None = None) -> dict:
    if source not in ("exchange_rate", "campaign"):
        raise ValueError("source must be exchange_rate or campaign")
    started_at = started_at or datetime.now(timezone.utc)
    body = fetch_json(url)
    document = json.loads(body)
    row_count = len(document) if isinstance(document, list) else (
        len(document["campaigns"]) if source == "campaign" else 1
    )
    run_id = started_at.strftime("%Y%m%dT%H%M%S%f")
    prefix = pipeline_s3_prefix() + f"bronze/api/{source}/dt={started_at:%Y-%m-%d}/run_id={run_id}/"
    file_name = "response.json"
    s3.put_object(Bucket=bucket, Key=prefix + file_name, Body=body, ContentType="application/json")
    manifest = {
        "run_id": run_id,
        "source_system": "api",
        "dataset": source,
        "load_type": "append",
        "status": "committed",
        "row_count": row_count,
        "files": [{
            "name": file_name,
            "size_bytes": len(body),
            "sha256": hashlib.sha256(body).hexdigest(),
        }],
        "started_at": started_at.isoformat().replace("+00:00", "Z"),
        "finished_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }
    s3.put_object(
        Bucket=bucket, Key=prefix + "_manifest.json",
        Body=json.dumps(manifest).encode("utf-8"), ContentType="application/json",
    )
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True, choices=("exchange_rate", "campaign"))
    parser.add_argument("--url", help="JSON API endpoint (defaults to Frankfurter URL for exchange_rate)")
    parser.add_argument("--base", help="3-letter base currency for exchange_rate (default: EXCHANGE_BASE_CURRENCY or BRL)")
    parser.add_argument("--quotes", help="Comma-separated quote currencies for exchange_rate (default: EXCHANGE_QUOTE_CURRENCIES or USD)")
    args = parser.parse_args()
    if args.url:
        url = args.url
    elif args.source == "exchange_rate":
        try:
            url = exchange_rate_url(args.base, args.quotes)
        except ValueError as exc:
            parser.error(str(exc))
    else:
        parser.error("--url is required when --source is campaign")
    s3, bucket = s3_client_and_bucket()
    manifest = ingest_api(args.source, url, s3, bucket)
    print(f"{args.source}: published API response in run {manifest['run_id']}")


if __name__ == "__main__":
    main()
