"""Record Airflow runs and layer metadata in a decoupled local SQLite database, JSONL log, and optional S3 metadata."""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = ROOT / "logs" / "ops_runs.db"
DEFAULT_JSONL_PATH = ROOT / "logs" / "ops_runs.jsonl"

logger = logging.getLogger("ops_runs")


def get_db_path() -> Path:
    env_path = os.environ.get("OPS_RUNS_DB_PATH")
    if env_path:
        return Path(env_path)
    return DEFAULT_DB_PATH


def get_jsonl_path() -> Path:
    env_path = os.environ.get("OPS_RUNS_JSONL_PATH")
    if env_path:
        return Path(env_path)
    return DEFAULT_JSONL_PATH


def get_namespace() -> str:
    prefix = os.environ.get("PIPELINE_S3_PREFIX", "").strip("/")
    if not prefix:
        try:
            from ingestion.s3 import pipeline_s3_prefix
            prefix = pipeline_s3_prefix().strip("/")
        except Exception:
            prefix = ""
    return prefix or "main"


def get_db_connection(db_path: Path | None = None) -> sqlite3.Connection:
    target_path = db_path or get_db_path()
    target_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(target_path), timeout=30.0)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_table(conn: sqlite3.Connection) -> None:
    with conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ops_pipeline_runs (
                dag_id TEXT NOT NULL,
                run_id TEXT NOT NULL,
                execution_date TEXT,
                namespace TEXT NOT NULL,
                status TEXT NOT NULL,
                start_time TEXT NOT NULL,
                end_time TEXT,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                duration_seconds REAL,
                error_message TEXT,
                failed_task TEXT,
                bronze_rows INTEGER DEFAULT 0,
                silver_rows INTEGER DEFAULT 0,
                gold_rows INTEGER DEFAULT 0,
                mart_rows INTEGER DEFAULT 0,
                quarantine_rows INTEGER DEFAULT 0,
                silver_counts_json TEXT DEFAULT '{}',
                gold_counts_json TEXT DEFAULT '{}',
                mart_counts_json TEXT DEFAULT '{}',
                volume_warning INTEGER DEFAULT 0,
                PRIMARY KEY (dag_id, run_id, namespace)
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_ops_pipeline_runs_status_started
            ON ops_pipeline_runs (status, started_at DESC)
        """)


def append_jsonl(record: dict, jsonl_path: Path | None = None) -> None:
    try:
        target = jsonl_path or get_jsonl_path()
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception as exc:
        logger.warning("Failed to append to JSONL log %s: %s", target, exc)


def export_s3_metadata(record: dict) -> None:
    try:
        from ingestion.s3 import pipeline_s3_prefix, s3_client_and_bucket
        s3, bucket = s3_client_and_bucket()
        prefix = pipeline_s3_prefix().rstrip("/")
        prefix_segment = f"{prefix}/" if prefix else ""
        key = f"{prefix_segment}ops/runs/{record['dag_id']}__{record['run_id']}.json"
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(record, indent=2).encode("utf-8"),
            ContentType="application/json",
        )
    except Exception as exc:
        logger.debug("Optional S3 metadata export skipped: %s", exc)


def bronze_rows_since(started_at: datetime) -> int:
    try:
        from ingestion.s3 import pipeline_s3_prefix, s3_client_and_bucket
        s3, bucket = s3_client_and_bucket()
        total = 0
        if started_at.tzinfo is None:
            started_at = started_at.replace(tzinfo=timezone.utc)
        prefix = pipeline_s3_prefix().rstrip("/")
        search_prefix = f"{prefix}/bronze/" if prefix else "bronze/"
        for page in s3.get_paginator("list_objects_v2").paginate(
            Bucket=bucket, Prefix=search_prefix
        ):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not key.endswith("/_manifest.json"):
                    continue
                manifest = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
                if manifest.get("status") != "committed":
                    continue
                published = datetime.fromisoformat(manifest["started_at"].replace("Z", "+00:00"))
                if published >= started_at:
                    total += manifest.get(
                        "row_count",
                        sum(item.get("row_count", 0) for item in manifest.get("files", []))
                    )
        return total
    except Exception as exc:
        logger.debug("Bronze S3 manifest row count skipped: %s", exc)
        return 0


def extract_execution_date(run_id: str, explicit_date: str | None = None) -> str:
    if explicit_date:
        return explicit_date
    match = re.search(r"(\d{4}-\d{2}-\d{2}(?:T[\d:+-]+)?)", run_id)
    if match:
        return match.group(1)
    return datetime.now(timezone.utc).isoformat()


def start(
    dag_id: str,
    run_id: str,
    execution_date: str | None = None,
    namespace: str | None = None,
    db_path: Path | None = None,
    jsonl_path: Path | None = None,
) -> dict:
    ns = namespace or get_namespace()
    now_iso = datetime.now(timezone.utc).isoformat()
    exec_date = extract_execution_date(run_id, execution_date)

    conn = get_db_connection(db_path)
    try:
        ensure_table(conn)
        with conn:
            conn.execute("""
                INSERT INTO ops_pipeline_runs (
                    dag_id, run_id, execution_date, namespace, status,
                    start_time, end_time, started_at, finished_at, duration_seconds,
                    error_message, failed_task
                ) VALUES (
                    :dag_id, :run_id, :execution_date, :namespace, 'running',
                    :start_time, NULL, :started_at, NULL, NULL,
                    NULL, NULL
                )
                ON CONFLICT(dag_id, run_id, namespace) DO UPDATE SET
                    status = 'running',
                    start_time = excluded.start_time,
                    started_at = excluded.started_at,
                    end_time = NULL,
                    finished_at = NULL,
                    duration_seconds = NULL,
                    error_message = NULL,
                    failed_task = NULL
            """, {
                "dag_id": dag_id,
                "run_id": run_id,
                "execution_date": exec_date,
                "namespace": ns,
                "start_time": now_iso,
                "started_at": now_iso,
            })
    finally:
        conn.close()

    record = {
        "event": "start",
        "dag_id": dag_id,
        "run_id": run_id,
        "execution_date": exec_date,
        "namespace": ns,
        "status": "running",
        "start_time": now_iso,
        "started_at": now_iso,
    }
    append_jsonl(record, jsonl_path)
    print(f"Started Ops run for {dag_id}/{run_id} (namespace={ns})")
    return record


def finish(
    dag_id: str,
    run_id: str,
    status: str,
    failed_task: str | None = None,
    error_message: str | None = None,
    execution_date: str | None = None,
    namespace: str | None = None,
    db_path: Path | None = None,
    jsonl_path: Path | None = None,
) -> dict:
    ns = namespace or get_namespace()
    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()
    err = error_message or failed_task

    conn = get_db_connection(db_path)
    try:
        ensure_table(conn)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT start_time, started_at, execution_date FROM ops_pipeline_runs "
            "WHERE dag_id = ? AND run_id = ? AND namespace = ?",
            (dag_id, run_id, ns),
        )
        row = cursor.fetchone()

        if row:
            start_str = row["start_time"] or row["started_at"]
            exec_date = row["execution_date"] or extract_execution_date(run_id, execution_date)
            try:
                start_dt = datetime.fromisoformat(start_str.replace("Z", "+00:00"))
                if start_dt.tzinfo is None:
                    start_dt = start_dt.replace(tzinfo=timezone.utc)
                duration = max(0.0, (now - start_dt).total_seconds())
            except Exception:
                start_dt = now
                duration = 0.0
        else:
            start_str = now_iso
            start_dt = now
            duration = 0.0
            exec_date = extract_execution_date(run_id, execution_date)

        bronze = bronze_rows_since(start_dt)
        silver_counts = {}
        gold_counts = {}
        mart_counts = {}
        quarantine = 0
        warning = False

        if status == "success":
            cursor.execute("""
                SELECT silver_rows FROM ops_pipeline_runs
                WHERE status = 'success' AND dag_id = ? AND run_id <> ? AND namespace = ?
                ORDER BY started_at DESC LIMIT 7
            """, (dag_id, run_id, ns))
            previous = [r[0] for r in cursor.fetchall() if r[0] is not None]
            silver_total = sum(silver_counts.values()) if silver_counts else 0
            warning = len(previous) == 7 and (sum(previous) / 7.0 > 0) and (silver_total < 0.8 * sum(previous) / 7.0)

        with conn:
            conn.execute("""
                INSERT INTO ops_pipeline_runs (
                    dag_id, run_id, execution_date, namespace, status,
                    start_time, end_time, started_at, finished_at, duration_seconds,
                    error_message, failed_task, bronze_rows, silver_rows, gold_rows,
                    mart_rows, quarantine_rows, silver_counts_json, gold_counts_json,
                    mart_counts_json, volume_warning
                ) VALUES (
                    :dag_id, :run_id, :execution_date, :namespace, :status,
                    :start_time, :end_time, :started_at, :finished_at, :duration,
                    :err, :failed_task, :bronze, :silver, :gold,
                    :mart, :quarantine, :silver_json, :gold_json,
                    :mart_json, :warning
                )
                ON CONFLICT(dag_id, run_id, namespace) DO UPDATE SET
                    execution_date = coalesce(excluded.execution_date, ops_pipeline_runs.execution_date),
                    status = excluded.status,
                    end_time = excluded.end_time,
                    finished_at = excluded.finished_at,
                    duration_seconds = excluded.duration_seconds,
                    error_message = excluded.error_message,
                    failed_task = excluded.failed_task,
                    bronze_rows = excluded.bronze_rows,
                    silver_rows = excluded.silver_rows,
                    gold_rows = excluded.gold_rows,
                    mart_rows = excluded.mart_rows,
                    quarantine_rows = excluded.quarantine_rows,
                    silver_counts_json = excluded.silver_counts_json,
                    gold_counts_json = excluded.gold_counts_json,
                    mart_counts_json = excluded.mart_counts_json,
                    volume_warning = excluded.volume_warning
            """, {
                "dag_id": dag_id,
                "run_id": run_id,
                "execution_date": exec_date,
                "namespace": ns,
                "status": status,
                "start_time": start_str,
                "end_time": now_iso,
                "started_at": start_str,
                "finished_at": now_iso,
                "duration": round(duration, 2),
                "err": err,
                "failed_task": err,
                "bronze": bronze,
                "silver": sum(silver_counts.values()) if silver_counts else 0,
                "gold": sum(gold_counts.values()) if gold_counts else 0,
                "mart": sum(mart_counts.values()) if mart_counts else 0,
                "quarantine": quarantine,
                "silver_json": json.dumps(silver_counts),
                "gold_json": json.dumps(gold_counts),
                "mart_json": json.dumps(mart_counts),
                "warning": 1 if warning else 0,
            })
    finally:
        conn.close()

    record = {
        "event": "finish",
        "dag_id": dag_id,
        "run_id": run_id,
        "execution_date": exec_date,
        "namespace": ns,
        "status": status,
        "start_time": start_str,
        "end_time": now_iso,
        "started_at": start_str,
        "finished_at": now_iso,
        "duration_seconds": round(duration, 2),
        "error_message": err,
        "failed_task": err,
        "bronze_rows": bronze,
        "silver_rows": sum(silver_counts.values()) if silver_counts else 0,
        "gold_rows": sum(gold_counts.values()) if gold_counts else 0,
        "mart_rows": sum(mart_counts.values()) if mart_counts else 0,
        "quarantine_rows": quarantine,
        "silver_counts_json": silver_counts,
        "gold_counts_json": gold_counts,
        "mart_counts_json": mart_counts,
        "volume_warning": bool(warning),
    }

    append_jsonl(record, jsonl_path)
    export_s3_metadata(record)

    print(
        f"{dag_id}/{run_id}: {status}; duration={duration:.2f}s; "
        f"Bronze={bronze}, Silver={record['silver_rows']}, Gold={record['gold_rows']}, "
        f"marts={record['mart_rows']}, volume_warning={bool(warning)}"
    )

    if warning:
        url = os.environ.get("AIRFLOW_ALERT_WEBHOOK_URL")
        if url:
            try:
                payload = json.dumps({
                    "text": (
                        f"E-commerce CDC Silver row count fell over 20% versus previous seven successful runs: "
                        f"{dag_id}/{run_id}."
                    )
                }).encode("utf-8")
                req = Request(url, data=payload, headers={"Content-Type": "application/json"}, method="POST")
                with urlopen(req, timeout=10):
                    pass
            except Exception as exc:
                print(f"Volume warning webhook failed: {type(exc).__name__}")

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
    parser.add_argument("--db-path", type=Path)
    parser.add_argument("--jsonl-path", type=Path)
    args = parser.parse_args()

    if args.action == "start":
        start(
            dag_id=args.dag_id,
            run_id=args.run_id,
            execution_date=args.execution_date,
            namespace=args.namespace,
            db_path=args.db_path,
            jsonl_path=args.jsonl_path,
        )
    else:
        if args.status is None:
            parser.error("finish requires --status")
        finish(
            dag_id=args.dag_id,
            run_id=args.run_id,
            status=args.status,
            failed_task=args.failed_task,
            error_message=args.error_message,
            execution_date=args.execution_date,
            namespace=args.namespace,
            db_path=args.db_path,
            jsonl_path=args.jsonl_path,
        )


if __name__ == "__main__":
    main()
