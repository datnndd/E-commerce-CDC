"""Local Bronze/Silver orchestration followed by Databricks Gold and marts."""

import json
import logging
import os
import re
import subprocess
from datetime import datetime, timedelta, timezone
from urllib.request import Request, urlopen

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, get_current_context, task


ROOT = "/workspace"
UV = "uv run --frozen"
NAMESPACE = os.environ.get("PIPELINE_S3_PREFIX", "").strip("/") or "main"
if not re.fullmatch(r"[A-Za-z0-9_-]+(?:/[A-Za-z0-9_-]+)*", NAMESPACE):
    raise ValueError("PIPELINE_S3_PREFIX must contain only safe path segments")
BRONZE = f"{ROOT}/logs/airflow_bronze/{NAMESPACE}"
SILVER = f"{ROOT}/logs/airflow_silver/{NAMESPACE}"


def alert_failure(context) -> None:
    url = os.environ.get("AIRFLOW_ALERT_WEBHOOK_URL")
    if not url:
        logging.error("AIRFLOW_ALERT_WEBHOOK_URL is not configured")
        return
    task_instance = context["task_instance"]
    message = (f"E-commerce CDC failed: {task_instance.dag_id}/{task_instance.run_id} "
               f"task={task_instance.task_id}. Check the Airflow task log.")
    request = Request(url, data=json.dumps({"text": message}).encode("utf-8"),
                      headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urlopen(request, timeout=10) as response:
            if response.status >= 400:
                raise RuntimeError(f"Webhook returned HTTP {response.status}")
    except Exception:
        logging.exception("Failed to send Airflow failure alert")


def shell_task(task_id: str, command: str) -> BashOperator:
    return BashOperator(task_id=task_id, bash_command=f"{UV} {command}", cwd=ROOT)


with DAG(
    dag_id="ecommerce_cdc_daily",
    schedule="@daily",
    start_date=datetime(2026, 11, 2, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5),
                  "on_failure_callback": alert_failure},
    tags=["ecommerce", "cdc"],
) as dag:

    @task
    def runtime_check() -> None:
        required = (
            "S3_BUCKET", "AIRFLOW_S3_BUCKET", "DBT_DATABRICKS_HOST", "DBT_DATABRICKS_HTTP_PATH",
            "DBT_CATALOG", "DATABRICKS_TOKEN", "AIRFLOW_ALERT_WEBHOOK_URL",
        )
        missing = [name for name in required if not os.environ.get(name)]
        if missing:
            raise ValueError(f"Configure these Airflow runtime variables: {', '.join(missing)}")
        if NAMESPACE != "main" and (
            os.environ.get("DBT_SILVER_SCHEMA", "silver") == "silver"
            or os.environ.get("DBT_TARGET_SCHEMA", "analytics_dev") == "analytics_dev"
        ):
            raise ValueError("An isolated PIPELINE_S3_PREFIX requires separate DBT_SILVER_SCHEMA and DBT_TARGET_SCHEMA")
        if NAMESPACE != "main" and not os.environ.get("POSTGRES_DB", "").endswith("_1c_test"):
            raise ValueError("An isolated 1C pipeline requires a *_1c_test PostgreSQL database")

    started = BashOperator(
        task_id="record_started",
        bash_command=(f'{UV} python -m orchestration.ops_runs start '
                      '--dag-id "$PIPELINE_DAG_ID" --run-id "$PIPELINE_RUN_ID"'),
        cwd=ROOT,
        env={"PIPELINE_DAG_ID": "{{ dag.dag_id }}", "PIPELINE_RUN_ID": "{{ run_id }}"},
        append_env=True,
    )
    cdc = shell_task("cdc_extract", "python -m ingestion.cdc_extract")
    rates = shell_task(
        "ingest_exchange_rates",
        "python -m ingestion.ingest_api --source exchange_rate "
        "--url 'https://api.frankfurter.dev/v2/rates?base=BRL&quotes=USD'",
    )
    campaign = shell_task(
        "ingest_campaign_fixture",
        "python -m ingestion.ingest_api --source campaign "
        "--url file:///workspace/sources/api/mock_campaigns.json",
    )
    clickstream = shell_task("ingest_demo_clickstream", "python -m orchestration.ingest_demo_clickstream")
    sync = shell_task("sync_committed_bronze", f"python -m ingestion.sync_bronze --root {BRONZE}")
    silver = shell_task(
        "load_silver", f"python -m spark_jobs.silver_transform --bronze-root {BRONZE} --silver-root {SILVER}"
    )
    silver_keys = shell_task("verify_silver", f"python -m spark_jobs.verify_silver --silver-root {SILVER}")
    quality = shell_task(
        "silver_quality_gate", f"python -m orchestration.silver_quality --silver-root {SILVER}"
    )
    publish = shell_task("publish_silver", f"python -m orchestration.publish_silver --silver-root {SILVER}")
    build = shell_task(
        "dbt_build", "dbt build --project-dir /workspace/dbt_project "
        "--profiles-dir /workspace/airflow/dbt_profile",
    )

    def finish_run(status: str) -> None:
        context = get_current_context()
        command = ["uv", "run", "--frozen", "python", "-m", "orchestration.ops_runs", "finish",
                   "--dag-id", context["dag"].dag_id, "--run-id", context["run_id"],
                   "--status", status]
        if status == "failed":
            command.extend(("--failed-task", "see_airflow_task_logs"))
        subprocess.run(command, cwd=ROOT, check=True)

    @task
    def record_success() -> None:
        finish_run("success")

    @task(trigger_rule="one_failed")
    def record_failure() -> None:
        finish_run("failed")

    @task(retries=0)
    def mark_failed() -> None:
        raise RuntimeError("Pipeline failed; see the Airflow task log")

    preflight = runtime_check()
    preflight >> started >> [cdc, rates, campaign]
    cdc >> clickstream
    [rates, campaign, clickstream] >> sync >> silver
    silver >> [silver_keys, quality] >> publish >> build
    success, failure = record_success(), record_failure()
    build >> success
    for upstream in (preflight, started, cdc, rates, campaign, clickstream, sync,
                     silver, silver_keys, quality, publish, build):
        upstream >> failure
    failure >> mark_failed()
