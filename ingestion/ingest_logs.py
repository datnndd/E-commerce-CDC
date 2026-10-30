"""Compress clickstream JSONL and publish it to S3 Bronze."""

import argparse
import gzip
import hashlib
import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from ingestion.s3 import s3_client_and_bucket


def ingest_logs(source: Path, s3, bucket: str, started_at: datetime | None = None) -> dict:
    started_at = started_at or datetime.now(timezone.utc)
    run_id = started_at.strftime("%Y%m%dT%H%M%S%f")
    prefix = (
        f"bronze/clickstream/dt={started_at:%Y-%m-%d}/hour={started_at:%H}/"
        f"run_id={run_id}/"
    )
    with tempfile.TemporaryDirectory() as temp_dir:
        compressed = Path(temp_dir) / "events.json.gz"
        row_count = 0
        with source.open("rb") as input_file, gzip.open(compressed, "wb") as output_file:
            for line_number, line in enumerate(input_file, 1):
                try:
                    event = json.loads(line.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    raise ValueError(f"{source.name}: invalid JSON at line {line_number}") from exc
                if not isinstance(event, dict):
                    raise ValueError(f"{source.name}: line {line_number} must be a JSON object")
                output_file.write(line)
                row_count += 1
        if not row_count:
            raise ValueError(f"{source.name}: no clickstream events")

        with compressed.open("rb") as data_file:
            sha256 = hashlib.file_digest(data_file, "sha256").hexdigest()
        size_bytes = compressed.stat().st_size
        s3.upload_file(str(compressed), bucket, prefix + compressed.name)
        manifest = {
            "run_id": run_id,
            "source_system": "clickstream",
            "dataset": "events",
            "load_type": "append",
            "status": "committed",
            "row_count": row_count,
            "source_file": source.name,
            "files": [{
                "name": compressed.name,
                "row_count": row_count,
                "size_bytes": size_bytes,
                "sha256": sha256,
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
    parser.add_argument("source", type=Path, help="Clickstream JSONL produced by sources.clickstream.generate")
    args = parser.parse_args()
    s3, bucket = s3_client_and_bucket()
    manifest = ingest_logs(args.source, s3, bucket)
    print(f"clickstream: published {manifest['row_count']} events in run {manifest['run_id']}")


if __name__ == "__main__":
    main()
