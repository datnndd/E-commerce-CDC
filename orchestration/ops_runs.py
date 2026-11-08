"""Persist one authoritative S3 Ops record per Airflow run."""

import argparse
from datetime import datetime, timezone
from hashlib import sha256
import json
import os
import re
from urllib.request import Request, urlopen

from botocore.exceptions import ClientError

from ingestion.s3 import pipeline_s3_prefix, s3_client_and_bucket
from spark_jobs.ops_key import silver_metrics_key


def namespace() -> str:
    return pipeline_s3_prefix().rstrip("/") or "main"


def run_key(dag_id: str, run_id: str, ns: str) -> str:
    digest = sha256(f"{dag_id}|{run_id}|{ns}".encode("utf-8")).hexdigest()
    prefix = "" if ns == "main" else f"{ns}/"
    return f"{prefix}ops/runs/{digest}.json"


def read_object(s3, bucket: str, key: str) -> dict | None:
    try:
        return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
    except ClientError as exc:
        if exc.response["Error"]["Code"] in {"NoSuchKey", "404"}:
            return None
        raise


def write_object(s3, bucket: str, key: str, record: dict) -> None:
    s3.put_object(Bucket=bucket, Key=key,
                  Body=json.dumps(record, ensure_ascii=False).encode("utf-8"),
                  ContentType="application/json")


def execution_date(run_id: str, explicit_date: str | None) -> str:
    if explicit_date:
        return explicit_date
    match = re.search(r"\d{4}-\d{2}-\d{2}(?:T[\d:+-]+)?", run_id)
    return match.group(0) if match else datetime.now(timezone.utc).isoformat()


def start(dag_id: str, run_id: str, execution_date_value: str | None = None,
          namespace_value: str | None = None) -> dict:
    ns = namespace_value or namespace()
    s3, bucket = s3_client_and_bucket()
    now = datetime.now(timezone.utc).isoformat()
    record = {
        "dag_id": dag_id, "run_id": run_id, "namespace": ns,
        "execution_date": execution_date(run_id, execution_date_value),
        "status": "running", "started_at": now, "finished_at": None,
        "duration_seconds": None, "failed_task": None, "error_message": None,
        "bronze_rows": None, "silver_rows": None, "gold_rows": None,
        "mart_rows": None, "quarantine_rows": None,
        "silver_counts_json": None, "gold_counts_json": None,
        "mart_counts_json": None, "volume_warning": False,
    }
    write_object(s3, bucket, run_key(dag_id, run_id, ns), record)
    return record


def bronze_rows_since(s3, bucket: str, ns: str, started_at: datetime) -> int | None:
    prefix = "" if ns == "main" else f"{ns}/"
    total = 0
    try:
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{prefix}bronze/"):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not key.endswith("/_manifest.json"):
                    continue
                manifest = read_object(s3, bucket, key)
                if manifest and manifest.get("status") == "committed":
                    published = datetime.fromisoformat(manifest["started_at"].replace("Z", "+00:00"))
                    if published >= started_at:
                        total += manifest.get("row_count", sum(f.get("row_count", 0) for f in manifest.get("files", [])))
        return total
    except (ClientError, KeyError, ValueError):
        return None


def previous_silver_counts(s3, bucket: str, dag_id: str, run_id: str, ns: str) -> list[int]:
    prefix = "" if ns == "main" else f"{ns}/"
    prior = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{prefix}ops/runs/"):
        for obj in page.get("Contents", []):
            record = read_object(s3, bucket, obj["Key"])
            if (record and record.get("dag_id") == dag_id and record.get("run_id") != run_id
                    and record.get("namespace") == ns and record.get("status") == "success"
                    and record.get("silver_rows") is not None):
                prior.append((record["started_at"], int(record["silver_rows"])))
    return [count for _, count in sorted(prior, reverse=True)[:7]]


def finish(dag_id: str, run_id: str, status: str, failed_task: str | None = None,
           error_message: str | None = None, execution_date_value: str | None = None,
           namespace_value: str | None = None) -> dict:
    if status not in {"success", "failed"}:
        raise ValueError("status must be success or failed")
    ns = namespace_value or namespace()
    s3, bucket = s3_client_and_bucket()
    key = run_key(dag_id, run_id, ns)
    record = read_object(s3, bucket, key)
    now = datetime.now(timezone.utc)
    if record is None:
        record = start(dag_id, run_id, execution_date_value, ns)
    started_at = datetime.fromisoformat(record["started_at"].replace("Z", "+00:00"))
    record.update(status=status, finished_at=now.isoformat(),
                  duration_seconds=max(0.0, round((now - started_at).total_seconds(), 2)),
                  failed_task=failed_task, error_message=error_message or failed_task)
    record["bronze_rows"] = bronze_rows_since(s3, bucket, ns, started_at)

    prefix = "" if ns == "main" else ns
    metrics = read_object(s3, bucket, silver_metrics_key(prefix, run_id))
    if metrics is not None:
        if metrics.get("run_id") != run_id:
            raise ValueError("Silver metrics belong to another run")
        silver_counts = metrics["silver_counts"]
        record["silver_rows"] = sum(silver_counts.values())
        record["quarantine_rows"] = metrics["quarantine_rows"]
        record["silver_counts_json"] = json.dumps(silver_counts, sort_keys=True)

    if status == "success":
        if metrics is None:
            raise ValueError("A successful run requires measured Silver metrics")
        from orchestration.athena_publish import count_gold_tables
        gold_counts, mart_counts = count_gold_tables()
        record["gold_rows"] = sum(gold_counts.values())
        record["mart_rows"] = sum(mart_counts.values())
        record["gold_counts_json"] = json.dumps(gold_counts, sort_keys=True)
        record["mart_counts_json"] = json.dumps(mart_counts, sort_keys=True)
        previous = previous_silver_counts(s3, bucket, dag_id, run_id, ns)
        record["volume_warning"] = (
            len(previous) == 7 and sum(previous) > 0
            and record["silver_rows"] < 0.8 * sum(previous) / 7
        )
        record["error_message"] = None
        record["failed_task"] = None

    write_object(s3, bucket, key, record)
    if record["volume_warning"]:
        url = os.environ.get("AIRFLOW_ALERT_WEBHOOK_URL")
        if url:
            payload = json.dumps({"text": f"E-commerce CDC Silver volume fell >20%: {dag_id}/{run_id}"}).encode()
            try:
                with urlopen(Request(url, data=payload, headers={"Content-Type": "application/json"}, method="POST"), timeout=10):
                    pass
            except Exception:
                pass
    return record


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("start", "finish"))
    parser.add_argument("--dag-id", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--status", choices=("success", "failed"))
    parser.add_argument("--failed-task")
    parser.add_argument("--error-message")
    parser.add_argument("--execution-date")
    parser.add_argument("--namespace")
    args = parser.parse_args()
    if args.action == "start":
        start(args.dag_id, args.run_id, args.execution_date, args.namespace)
    else:
        if args.status is None:
            parser.error("finish requires --status")
        finish(args.dag_id, args.run_id, args.status, args.failed_task,
               args.error_message, args.execution_date, args.namespace)


if __name__ == "__main__":
    main()
