"""Record Airflow runs and layer counts in a Databricks Delta Ops table."""

import argparse
import json
import os
import re
from datetime import datetime, timezone
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

from databricks import sql

from ingestion.s3 import pipeline_s3_prefix, s3_client_and_bucket
from spark_jobs.silver_transform import SILVER_NAMES


def identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", value):
        raise ValueError(f"Unsafe SQL identifier: {value!r}")
    return f"`{value}`"


GOLD_FACTS = (
    "fct_orders", "fct_order_items", "fct_payments", "fct_reviews",
    "fct_clickstream_events", "fct_sessions", "fct_exchange_rates",
)
BUSINESS_MARTS = (
    "mart_daily_sales", "mart_product_performance", "mart_customer_rfm",
    "mart_funnel_daily", "mart_payment_mix_daily", "mart_order_experience_daily",
)


def connection():
    token = os.environ.get("DATABRICKS_TOKEN")
    host = os.environ.get("DBT_DATABRICKS_HOST")
    path = os.environ.get("DBT_DATABRICKS_HTTP_PATH")
    if not all((token, host, path)):
        raise ValueError("Set DATABRICKS_TOKEN, DBT_DATABRICKS_HOST and DBT_DATABRICKS_HTTP_PATH")
    return sql.connect(server_hostname=urlsplit(host).hostname or host, http_path=path, access_token=token)


def ops_table() -> str:
    catalog = identifier(os.environ["DBT_CATALOG"])
    return f"{catalog}.`ops`.`ops_pipeline_runs`"


def ensure_table(cursor) -> str:
    table = ops_table()
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {table.rsplit('.', 1)[0]}")
    cursor.execute(f"""CREATE TABLE IF NOT EXISTS {table} (
        dag_id STRING, run_id STRING, namespace STRING, started_at TIMESTAMP, finished_at TIMESTAMP,
        status STRING, failed_task STRING, bronze_rows BIGINT, silver_rows BIGINT,
        gold_rows BIGINT, mart_rows BIGINT, quarantine_rows BIGINT,
        silver_counts_json STRING, gold_counts_json STRING, mart_counts_json STRING,
        volume_warning BOOLEAN
    ) USING DELTA""")
    return table


def start(dag_id: str, run_id: str) -> None:
    with connection() as db:
        with db.cursor() as cursor:
            table = ensure_table(cursor)
            cursor.execute(f"""MERGE INTO {table} AS target
                USING (SELECT :run_id AS run_id, :dag_id AS dag_id,
                              :namespace AS namespace) AS source
                ON target.run_id = source.run_id AND target.dag_id = source.dag_id
                   AND target.namespace = source.namespace
                WHEN MATCHED THEN UPDATE SET started_at = current_timestamp(),
                    finished_at = NULL, status = 'running', failed_task = NULL
                WHEN NOT MATCHED THEN INSERT (dag_id, run_id, namespace, started_at, status)
                    VALUES (source.dag_id, source.run_id, source.namespace,
                            current_timestamp(), 'running')""",
                {"run_id": run_id, "dag_id": dag_id,
                 "namespace": pipeline_s3_prefix().rstrip("/") or "main"})


def bronze_rows_since(started_at: datetime) -> int:
    s3, bucket = s3_client_and_bucket()
    total = 0
    if started_at.tzinfo is None:
        started_at = started_at.replace(tzinfo=timezone.utc)
    for page in s3.get_paginator("list_objects_v2").paginate(
            Bucket=bucket, Prefix=pipeline_s3_prefix() + "bronze/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith("/_manifest.json"):
                continue
            manifest = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
            if manifest.get("status") != "committed":
                continue
            published = datetime.fromisoformat(manifest["started_at"].replace("Z", "+00:00"))
            if published >= started_at:
                total += manifest.get("row_count", sum(item.get("row_count", 0) for item in manifest["files"]))
    return total


def count_tables(cursor, namespace: str, names: tuple[str, ...]) -> dict[str, int]:
    result = {}
    for name in names:
        cursor.execute(f"SELECT count(*) FROM {namespace}.{identifier(name)}")
        result[name] = cursor.fetchone()[0]
    return result


def finish(dag_id: str, run_id: str, status: str, failed_task: str | None) -> None:
    with connection() as db:
        with db.cursor() as cursor:
            table = ensure_table(cursor)
            cursor.execute(f"SELECT started_at FROM {table} WHERE dag_id = :dag_id AND run_id = :run_id "
                           "AND namespace = :namespace",
                           {"dag_id": dag_id, "run_id": run_id,
                            "namespace": pipeline_s3_prefix().rstrip("/") or "main"})
            row = cursor.fetchone()
            if row is None:
                raise ValueError(f"No started Ops run for {dag_id}/{run_id}")
            bronze = bronze_rows_since(row[0])
            silver = gold = marts = {}
            quarantine = None
            warning = False
            if status == "success":
                catalog = identifier(os.environ["DBT_CATALOG"])
                silver_namespace = f"{catalog}.{identifier(os.environ.get('DBT_SILVER_SCHEMA', 'silver'))}"
                gold_namespace = f"{catalog}.{identifier(os.environ.get('DBT_TARGET_SCHEMA', 'analytics_dev') + '_gold')}"
                silver = count_tables(cursor, silver_namespace, tuple(SILVER_NAMES.values()))
                gold = count_tables(cursor, gold_namespace, GOLD_FACTS)
                marts = count_tables(cursor, gold_namespace, BUSINESS_MARTS)
                cursor.execute(f"SELECT count(*) FROM {silver_namespace}.`silver_quarantine`")
                quarantine = cursor.fetchone()[0]
                cursor.execute(f"""SELECT silver_rows FROM {table}
                    WHERE status = 'success' AND dag_id = :dag_id AND run_id <> :run_id
                    AND namespace = :namespace ORDER BY started_at DESC LIMIT 7""",
                    {"dag_id": dag_id, "run_id": run_id,
                     "namespace": pipeline_s3_prefix().rstrip("/") or "main"})
                previous = [item[0] for item in cursor.fetchall()]
                warning = len(previous) == 7 and sum(silver.values()) < 0.8 * sum(previous) / 7
            cursor.execute(f"""UPDATE {table} SET finished_at = current_timestamp(),
                status = :status, failed_task = :failed_task, bronze_rows = :bronze,
                silver_rows = :silver_rows, gold_rows = :gold_rows, mart_rows = :mart_rows,
                quarantine_rows = :quarantine, silver_counts_json = :silver_json,
                gold_counts_json = :gold_json, mart_counts_json = :mart_json,
                volume_warning = :warning
                WHERE dag_id = :dag_id AND run_id = :run_id AND namespace = :namespace""", {
                    "status": status, "failed_task": failed_task, "bronze": bronze,
                    "silver_rows": sum(silver.values()) if silver else None,
                    "gold_rows": sum(gold.values()) if gold else None,
                    "mart_rows": sum(marts.values()) if marts else None,
                    "quarantine": quarantine,
                    "silver_json": json.dumps(silver), "gold_json": json.dumps(gold),
                    "mart_json": json.dumps(marts), "warning": warning,
                    "dag_id": dag_id, "run_id": run_id,
                    "namespace": pipeline_s3_prefix().rstrip("/") or "main",
                })
            print(f"{dag_id}/{run_id}: {status}; Bronze={bronze}, Silver={sum(silver.values())}, "
                  f"Gold={sum(gold.values())}, marts={sum(marts.values())}, volume_warning={warning}")
            if warning:
                url = os.environ.get("AIRFLOW_ALERT_WEBHOOK_URL")
                if url:
                    request = Request(url, data=json.dumps({"text":
                        f"E-commerce CDC Silver row count fell over 20% versus the previous seven successful runs: "
                        f"{dag_id}/{run_id}. Check ops_pipeline_runs."}).encode("utf-8"),
                        headers={"Content-Type": "application/json"}, method="POST")
                    try:
                        with urlopen(request, timeout=10):
                            pass
                    except Exception as exc:
                        print(f"Volume warning webhook failed: {type(exc).__name__}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("start", "finish"))
    parser.add_argument("--dag-id", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--status", choices=("success", "failed"))
    parser.add_argument("--failed-task")
    args = parser.parse_args()
    if args.action == "start":
        start(args.dag_id, args.run_id)
    else:
        if args.status is None:
            parser.error("finish requires --status")
        finish(args.dag_id, args.run_id, args.status, args.failed_task)


if __name__ == "__main__":
    main()
