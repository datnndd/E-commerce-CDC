"""Upload Bronze, run AWS Glue Silver, then build Gold and marts."""

import json
import logging
import os
import re
import subprocess
from datetime import datetime, timedelta, timezone
from urllib.request import Request, urlopen

from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, get_current_context, task


ROOT = "/workspace"
UV = "uv run --frozen"
S3_PREFIX = os.environ.get("PIPELINE_S3_PREFIX", "").strip("/")
if S3_PREFIX and not re.fullmatch(r"[A-Za-z0-9_-]+(?:/[A-Za-z0-9_-]+)*", S3_PREFIX):
    raise ValueError("PIPELINE_S3_PREFIX must contain only safe path segments")
BASE_CURRENCY = os.environ.get("EXCHANGE_BASE_CURRENCY", "BRL").strip().upper() or "BRL"
QUOTE_CURRENCIES = os.environ.get("EXCHANGE_QUOTE_CURRENCIES", "USD").strip().upper() or "USD"
GLUE_SILVER_JOB_NAME = os.environ.get("GLUE_SILVER_JOB_NAME", "ecommerce-silver-transform")


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
    },
    tags=["ecommerce", "cdc", "aws", "glue"],
) as dag:

    @task
    def runtime_check() -> None:
        bucket = os.environ.get("AIRFLOW_S3_BUCKET") or os.environ.get("S3_BUCKET")
        if not bucket:
            raise ValueError("Configure AIRFLOW_S3_BUCKET or S3_BUCKET runtime variable")
        if not re.fullmatch(r"[A-Za-z0-9.-]+", bucket):
            raise ValueError("S3 bucket must be a bucket name, not an S3 URI")

        job_name = os.environ.get("GLUE_SILVER_JOB_NAME", "ecommerce-silver-transform")
        if not re.fullmatch(r"[A-Za-z0-9_-]+", job_name):
            raise ValueError(f"GLUE_SILVER_JOB_NAME must contain only safe characters: {job_name!r}")

        required = ("GLUE_ROLE_ARN", "SILVER_S3_ROOT", "DBT_S3_LOCATION",
                    "DBT_SILVER_SCHEMA", "DBT_TARGET_SCHEMA", "BI_DATABASE",
                    "ATHENA_WORKGROUP", "ATHENA_RESULTS_S3")
        missing = [name for name in required if not os.environ.get(name)]
        if missing:
            raise ValueError(f"Configure these AWS pipeline variables: {', '.join(missing)}")
        if not re.fullmatch(r"arn:aws:iam::\d{12}:role/[A-Za-z0-9+=,.@_/-]+", os.environ["GLUE_ROLE_ARN"]):
            raise ValueError("GLUE_ROLE_ARN must be a valid IAM role ARN")
        for name in ("SILVER_S3_ROOT", "DBT_S3_LOCATION", "ATHENA_RESULTS_S3"):
            value = os.environ[name].rstrip("/")
            if not value.startswith(f"s3://{bucket}/") or "'" in value or ".." in value:
                raise ValueError(f"{name} must be a safe S3 path in the pipeline bucket")
        if os.environ["SILVER_S3_ROOT"].rstrip("/") == f"s3://{bucket}/silver/pipeline_live":
            raise ValueError("SILVER_S3_ROOT must use a new AWS Silver path")
        silver_path = os.environ["SILVER_S3_ROOT"].rstrip("/") + "/"
        gold_path = os.environ["DBT_S3_LOCATION"].rstrip("/") + "/"
        if silver_path.startswith(gold_path) or gold_path.startswith(silver_path):
            raise ValueError("Silver and dbt Gold S3 locations must be separate")
        for name in ("DBT_SILVER_SCHEMA", "DBT_TARGET_SCHEMA", "BI_DATABASE"):
            if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", os.environ[name]):
                raise ValueError(f"{name} must be a safe Glue database name")
        if os.environ["BI_DATABASE"] in {
            os.environ["DBT_SILVER_SCHEMA"], f"{os.environ['DBT_TARGET_SCHEMA']}_gold"
        }:
            raise ValueError("BI_DATABASE must be separate from Silver and Gold databases")

        if S3_PREFIX and (
            os.environ.get("DBT_SILVER_SCHEMA", "silver") == "silver"
            or os.environ.get("DBT_TARGET_SCHEMA", "analytics_dev") == "analytics_dev"
        ):
            raise ValueError("An isolated PIPELINE_S3_PREFIX requires separate DBT_SILVER_SCHEMA and DBT_TARGET_SCHEMA")
        if S3_PREFIX and not os.environ.get("POSTGRES_DB", "").endswith("_1c_test"):
            raise ValueError("An isolated 1C pipeline requires a *_1c_test PostgreSQL database")
        if S3_PREFIX and not os.environ["SILVER_S3_ROOT"].startswith(f"s3://{bucket}/{S3_PREFIX}/"):
            raise ValueError("Isolated SILVER_S3_ROOT must stay under PIPELINE_S3_PREFIX")

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

    bucket = os.environ.get("AIRFLOW_S3_BUCKET") or os.environ.get("S3_BUCKET", "")
    bronze_root = f"s3://{bucket}/{S3_PREFIX}" if S3_PREFIX else f"s3://{bucket}"
    silver_root = os.environ.get("SILVER_S3_ROOT", "")
    silver_job = GlueJobOperator(
        task_id="run_silver_transform_on_glue",
        job_name=GLUE_SILVER_JOB_NAME,
        aws_conn_id="aws_default",
        wait_for_completion=True,
        script_args={
            "--bronze_root": bronze_root,
            "--silver_root": silver_root,
            "--glue_database": os.environ.get("DBT_SILVER_SCHEMA", ""),
            "--pipeline_run_id": "{{ run_id }}",
        },
    )

    build = shell_task(
        "build_gold_and_marts_dbt",
        "dbt build --project-dir /workspace/dbt_project "
        "--profiles-dir /workspace/airflow/dbt_profile",
    )
    publish = shell_task("publish_athena_tables", "python -m orchestration.athena_publish")

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
    [rates, campaign, clickstream] >> silver_job >> build >> publish
    success, failure = record_pipeline_success(), record_pipeline_failure()
    publish >> success
    for upstream in (preflight, started, cdc, rates, campaign, clickstream, silver_job, build, publish, success):
        upstream >> failure
    failure >> fail_dag_run()
