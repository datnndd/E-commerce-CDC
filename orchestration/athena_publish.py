"""Publish dbt Delta marts and Ops records through Athena engine v3."""

import os
import re
import time
from urllib.parse import urlsplit

import boto3

GOLD_FACTS = (
    "fct_orders", "fct_order_items", "fct_payments", "fct_reviews",
    "fct_clickstream_events", "fct_sessions", "fct_exchange_rates",
)
BUSINESS_MARTS = (
    "mart_daily_sales", "mart_product_performance", "mart_customer_rfm",
    "mart_funnel_daily", "mart_payment_mix_daily", "mart_order_experience_daily",
)


def identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", value):
        raise ValueError(f"Unsafe Glue identifier: {value!r}")
    return value


def settings() -> tuple[str, str, str, str]:
    database = identifier(os.environ["BI_DATABASE"])
    gold_database = identifier(f"{os.environ['DBT_TARGET_SCHEMA']}_gold")
    workgroup = os.environ["ATHENA_WORKGROUP"]
    output = os.environ["ATHENA_RESULTS_S3"]
    parsed = urlsplit(output)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.strip("/") or parsed.query or parsed.fragment:
        raise ValueError("ATHENA_RESULTS_S3 must be an S3 URI")
    if not re.fullmatch(r"[A-Za-z0-9_-]+", workgroup):
        raise ValueError("ATHENA_WORKGROUP must be a valid workgroup name")
    return database, gold_database, workgroup, output.rstrip("/") + "/"


def run_query(query: str, database: str | None = None) -> list[dict]:
    bi_database, _, workgroup, output = settings()
    client = boto3.client("athena", region_name=os.environ.get("AWS_REGION", "ap-southeast-1"))
    request = {"QueryString": query, "WorkGroup": workgroup,
               "ResultConfiguration": {"OutputLocation": output}}
    if database:
        request["QueryExecutionContext"] = {"Database": database}
    query_id = client.start_query_execution(**request)["QueryExecutionId"]
    for _ in range(120):
        execution = client.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]
        state = execution["Status"]["State"]
        if state == "SUCCEEDED":
            break
        if state in {"FAILED", "CANCELLED"}:
            raise RuntimeError(f"Athena query {query_id} {state}: {execution['Status'].get('StateChangeReason', '')}")
        time.sleep(2)
    else:
        raise TimeoutError(f"Athena query {query_id} did not finish")
    rows = []
    token = None
    while True:
        page = client.get_query_results(QueryExecutionId=query_id, **({"NextToken": token} if token else {}))
        rows.extend(page["ResultSet"]["Rows"])
        token = page.get("NextToken")
        if not token:
            break
    return rows


def publish_tables() -> None:
    bi_database, gold_database, workgroup, _ = settings()
    athena = boto3.client("athena", region_name=os.environ.get("AWS_REGION", "ap-southeast-1"))
    engine = athena.get_work_group(WorkGroup=workgroup)["WorkGroup"]["Configuration"].get("EngineVersion", {})
    if engine.get("SelectedEngineVersion") != "Athena engine version 3" and engine.get("EffectiveEngineVersion") != "Athena engine version 3":
        raise ValueError(f"ATHENA_WORKGROUP {workgroup} must use Athena engine version 3")
    glue = boto3.client("glue", region_name=os.environ.get("AWS_REGION", "ap-southeast-1"))
    run_query(f"CREATE DATABASE IF NOT EXISTS `{bi_database}`")
    for name in (*GOLD_FACTS, *BUSINESS_MARTS):
        source = glue.get_table(DatabaseName=gold_database, Name=name)["Table"]
        location = source["StorageDescriptor"]["Location"].rstrip("/") + "/"
        if not re.fullmatch(r"s3://[A-Za-z0-9.-]+/[A-Za-z0-9._/-]+/", location):
            raise ValueError(f"Invalid S3 location for {gold_database}.{name}: {location}")
        try:
            existing = glue.get_table(DatabaseName=bi_database, Name=name)["Table"]
        except glue.exceptions.EntityNotFoundException:
            existing = None
        if existing:
            current = existing["StorageDescriptor"]["Location"].rstrip("/") + "/"
            if current != location:
                raise ValueError(f"{bi_database}.{name} points to {current}, expected {location}")
            params = existing.get("Parameters", {})
            if (params.get("table_type", "").upper() != "DELTA"
                    and params.get("spark.sql.sources.provider", "").lower() != "delta"):
                raise ValueError(f"{bi_database}.{name} exists but is not a Delta table")
        else:
            run_query(
                f"CREATE EXTERNAL TABLE `{bi_database}`.`{name}` LOCATION '{location}' "
                "TBLPROPERTIES ('table_type'='DELTA')"
            )
        run_query(f"SELECT count(*) FROM `{bi_database}`.`{name}`", bi_database)

    bucket = os.environ.get("AIRFLOW_S3_BUCKET") or os.environ["S3_BUCKET"]
    prefix = os.environ.get("PIPELINE_S3_PREFIX", "").strip("/")
    root = f"s3://{bucket}/{prefix + '/' if prefix else ''}ops/runs/"
    try:
        existing = glue.get_table(DatabaseName=bi_database, Name="ops_pipeline_runs")["Table"]
    except glue.exceptions.EntityNotFoundException:
        existing = None
    if existing:
        current = existing["StorageDescriptor"]["Location"].rstrip("/") + "/"
        if current != root:
            raise ValueError(f"{bi_database}.ops_pipeline_runs points to {current}, expected {root}")
    else:
        run_query(f"""CREATE EXTERNAL TABLE `{bi_database}`.`ops_pipeline_runs` (
            dag_id string, run_id string, execution_date string, namespace string,
            status string, started_at string, finished_at string, duration_seconds double,
            failed_task string, error_message string, bronze_rows bigint,
            silver_rows bigint, gold_rows bigint, mart_rows bigint,
            quarantine_rows bigint, silver_counts_json string, gold_counts_json string,
            mart_counts_json string, volume_warning boolean
        ) ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
        STORED AS TEXTFILE LOCATION '{root}'""")


def count_gold_tables() -> tuple[dict[str, int], dict[str, int]]:
    database, _, _, _ = settings()
    def count(names: tuple[str, ...]) -> dict[str, int]:
        result = {}
        for name in names:
            rows = run_query(f"SELECT count(*) FROM `{database}`.`{name}`", database)
            result[name] = int(rows[1]["Data"][0]["VarCharValue"])
        return result
    return count(GOLD_FACTS), count(BUSINESS_MARTS)


if __name__ == "__main__":
    publish_tables()
