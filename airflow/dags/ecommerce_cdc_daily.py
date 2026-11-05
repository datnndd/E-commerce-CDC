"""Local Bronze/Silver orchestration followed by Databricks Gold and marts."""

import json
import logging
import os
import re
import subprocess
from datetime import datetime, timedelta, timezone
from urllib.request import Request, urlopen

from airflow.providers.standard.operators.bash import BashOperator
try:
    from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
except ImportError:
    DatabricksRunNowOperator = None
from airflow.sdk import DAG, get_current_context, task


ROOT = "/workspace"
UV = "uv run --frozen"
NAMESPACE = os.environ.get("PIPELINE_S3_PREFIX", "").strip("/") or "main"
if not re.fullmatch(r"[A-Za-z0-9_-]+(?:/[A-Za-z0-9_-]+)*", NAMESPACE):
    raise ValueError("PIPELINE_S3_PREFIX must contain only safe path segments")
BASE_CURRENCY = os.environ.get("EXCHANGE_BASE_CURRENCY", "BRL").strip().upper() or "BRL"
QUOTE_CURRENCIES = os.environ.get("EXCHANGE_QUOTE_CURRENCIES", "USD").strip().upper() or "USD"
DATABRICKS_SILVER_JOB_ID = os.environ.get("DATABRICKS_SILVER_JOB_ID", "")


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
    params={
        "base_currency": BASE_CURRENCY,
        "quote_currencies": QUOTE_CURRENCIES,
        "databricks_silver_job_id": DATABRICKS_SILVER_JOB_ID,
    },
    tags=["ecommerce", "cdc", "databricks"],
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
        task_id="record_pipeline_start",
        bash_command=(f'{UV} python -m orchestration.ops_runs start '
                      '--dag-id "$PIPELINE_DAG_ID" --run-id "$PIPELINE_RUN_ID"'),
        cwd=ROOT,
        env={"PIPELINE_DAG_ID": "{{ dag.dag_id }}", "PIPELINE_RUN_ID": "{{ run_id }}"},
        append_env=True,
    )
    cdc = shell_task("extract_postgres_cdc_to_bronze", "python -m ingestion.cdc_extract")
    rates = BashOperator(
        task_id="ingest_exchange_rates_to_bronze",
        bash_command=(f'{UV} python -m ingestion.ingest_api --source exchange_rate '
                      '--base "$EXCHANGE_BASE_CURRENCY" --quotes "$EXCHANGE_QUOTE_CURRENCIES"'),
        cwd=ROOT,
        env={
            "EXCHANGE_BASE_CURRENCY": "{{ params.base_currency }}",
            "EXCHANGE_QUOTE_CURRENCIES": "{{ params.quote_currencies }}",
        },
        append_env=True,
    )
    campaign = shell_task(
        "ingest_campaign_fixture_to_bronze",
        "python -m ingestion.ingest_api --source campaign "
        "--url file:///workspace/sources/api/mock_campaigns.json",
    )
    clickstream = shell_task(
        "generate_demo_clickstream_to_bronze", "python -m orchestration.ingest_demo_clickstream"
    )

    if DatabricksRunNowOperator is not None:
        silver_job = DatabricksRunNowOperator(
            task_id="run_silver_transform_on_databricks",
            databricks_conn_id="databricks_default",
            job_id="{{ params.databricks_silver_job_id }}",
            notebook_params={
                "pipeline_s3_prefix": NAMESPACE,
                "target_schema": os.environ.get("DBT_SILVER_SCHEMA", "silver"),
            },
        )
    else:
        silver_job = shell_task(
            "run_silver_transform_on_databricks",
            "echo 'Databricks provider not installed; placeholder for DatabricksRunNowOperator'",
        )

    build = shell_task(
        "build_gold_and_marts_dbt",
        "dbt build --project-dir /workspace/dbt_project "
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
    def record_pipeline_success() -> None:
        finish_run("success")

    @task(trigger_rule="one_failed")
    def record_pipeline_failure() -> None:
        finish_run("failed")

    @task(retries=0)
    def fail_dag_run() -> None:
        raise RuntimeError("Pipeline failed; see the Airflow task log")

    preflight = runtime_check()
    preflight >> started >> [cdc, rates, campaign]
    cdc >> clickstream
    [rates, campaign, clickstream] >> silver_job >> build
    success, failure = record_pipeline_success(), record_pipeline_failure()
    build >> success
    for upstream in (preflight, started, cdc, rates, campaign, clickstream, silver_job, build):
        upstream >> failure
    failure >> fail_dag_run()
