# Handoff Report: Airflow DAG Migration from Databricks to AWS Glue

**Agent**: explorer_m3_1  
**Working Directory**: `c:\Users\ddat2\Downloads\Projects\E-commerce-CDC\.agents\teamwork\explorer_m3_1\`  
**Milestone**: M3 — Airflow Orchestration & Environment Decoupling  
**Date**: 2026-11-07  

---

## 1. Observation

### 1.1 Databricks Usages in `airflow/dags/ecommerce_cdc_daily.py`
Direct inspection of `airflow/dags/ecommerce_cdc_daily.py` revealed the following specific Databricks dependencies:

- **Line 1 (Docstring)**:
  ```python
  """Upload Bronze, run Databricks Silver, then build Gold and marts."""
  ```
- **Line 11 (Import)**:
  ```python
  from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
  ```
- **Line 23 (Job ID Configuration)**:
  ```python
  DATABRICKS_SILVER_JOB_ID = os.environ.get("DATABRICKS_SILVER_JOB_ID", "")
  ```
- **Line 60 (DAG Tags)**:
  ```python
  tags=["ecommerce", "cdc", "databricks"],
  ```
- **Lines 65–74 (Preflight Runtime Check / Environment Validation)**:
  ```python
  @task
  def runtime_check() -> None:
      required = (
          "S3_BUCKET", "AIRFLOW_S3_BUCKET", "DBT_DATABRICKS_HOST", "DBT_DATABRICKS_HTTP_PATH",
          "DBT_CATALOG", "DATABRICKS_TOKEN", "DATABRICKS_SILVER_JOB_ID",
          "AIRFLOW_ALERT_WEBHOOK_URL",
      )
      missing = [name for name in required if not os.environ.get(name)]
      if missing:
          raise ValueError(f"Configure these Airflow runtime variables: {', '.join(missing)}")
      if not os.environ["DATABRICKS_SILVER_JOB_ID"].isdigit():
          raise ValueError("DATABRICKS_SILVER_JOB_ID must be a numeric Databricks Job ID")
  ```
  Five required variables are Databricks-specific:
  1. `DBT_DATABRICKS_HOST`
  2. `DBT_DATABRICKS_HTTP_PATH`
  3. `DBT_CATALOG`
  4. `DATABRICKS_TOKEN`
  5. `DATABRICKS_SILVER_JOB_ID` (plus numeric validation `.isdigit()`)

- **Lines 112–125 (Silver Operator Task)**:
  ```python
  bucket = os.environ.get("AIRFLOW_S3_BUCKET", "")
  bronze_root = f"s3://{bucket}/{S3_PREFIX}" if S3_PREFIX else f"s3://{bucket}"
  silver_root = f"{bronze_root}/silver/pipeline_live"
  silver_job = DatabricksRunNowOperator(
      task_id="run_silver_transform_on_databricks",
      databricks_conn_id="databricks_default",
      job_id=DATABRICKS_SILVER_JOB_ID,
      python_params=[
          "--bronze-root", bronze_root,
          "--silver-root", silver_root,
          "--catalog", os.environ.get("DBT_CATALOG", ""),
          "--schema", os.environ.get("DBT_SILVER_SCHEMA", "silver"),
      ],
  )
  ```

- **Lines 154–162 (Task Graph Wiring)**:
  ```python
  preflight = runtime_check()
  preflight >> started >> [cdc, rates, campaign]
  cdc >> clickstream
  [rates, campaign, clickstream] >> silver_job >> build
  success, failure = record_pipeline_success(), record_pipeline_failure()
  build >> success
  for upstream in (preflight, started, cdc, rates, campaign, clickstream, silver_job, build):
      upstream >> failure
  failure >> fail_dag_run()
  ```

### 1.2 Glue Silver Job Parameter Specification (`spark_jobs/silver_transform.py`)
Direct inspection of `spark_jobs/silver_transform.py` lines 351–359 confirmed the supported options in AWS Glue runtime:
```python
GLUE_OPTION_SPEC: list[tuple[str, bool, str | None]] = [
    ("JOB_NAME", True, None),
    ("bronze-root", True, None),
    ("silver-root", True, None),
    ("dataset", False, None),
    ("glue-database", False, "silver"),
    ("catalog", False, None),
    ("schema", False, "silver"),
]
```
`parse_glue_args()` automatically extracts options passed via AWS Glue runtime flags (with hyphen or underscore forms). The Silver Glue script requires `--bronze-root` and `--silver-root`, and accepts `--glue-database` (defaulting to `"silver"`).

### 1.3 Airflow DAG Contract Tests (`tests/test_stage6_dag.py`)
Direct inspection of `tests/test_stage6_dag.py` lines 18–45 revealed the exact contract assertions:
```python
dag = bag.dags["ecommerce_cdc_daily"]
self.assertEqual(dag.max_active_runs, 1)
self.assertEqual(dag.task_dict["record_pipeline_failure"].trigger_rule, "one_failed")
self.assertEqual(
    dag.task_dict["record_pipeline_failure"].upstream_task_ids,
    set(dag.task_ids) - {"record_pipeline_success", "record_pipeline_failure", "fail_dag_run"},
)
self.assertEqual(dag.task_dict["fail_dag_run"].upstream_task_ids, {"record_pipeline_failure"})
self.assertEqual(dag.task_dict["fail_dag_run"].retries, 0)
self.assertEqual(dag.task_dict["record_pipeline_success"].upstream_task_ids, {"build_gold_and_marts_dbt"})
self.assertEqual(dag.task_dict["build_gold_and_marts_dbt"].upstream_task_ids, {"run_silver_transform_on_databricks"})
self.assertEqual(dag.task_dict["run_silver_transform_on_databricks"].upstream_task_ids, {
    "ingest_exchange_rates_to_bronze", "ingest_campaign_fixture_to_bronze", "generate_demo_clickstream_to_bronze",
})
self.assertEqual(dag.task_dict["generate_demo_clickstream_to_bronze"].upstream_task_ids, {"extract_postgres_cdc_to_bronze"})
self.assertEqual(dag.params["base_currency"], "BRL")
self.assertEqual(dag.params["quote_currencies"], "USD")
silver_task = dag.task_dict["run_silver_transform_on_databricks"]
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
self.assertIsInstance(silver_task, DatabricksRunNowOperator)
prefix = os.environ.get("PIPELINE_S3_PREFIX", "").strip("/")
root = f"s3://{os.environ.get('AIRFLOW_S3_BUCKET', '')}" + (f"/{prefix}" if prefix else "")
self.assertEqual(silver_task.python_params[:4], [
    "--bronze-root", root, "--silver-root", f"{root}/silver/pipeline_live",
])
```

### 1.4 Container and Environment References
- `docker/airflow.Dockerfile` line 12: `RUN pip install --no-cache-dir apache-airflow-providers-databricks`
- `compose.yaml` lines 36–43: defines `DBT_DATABRICKS_HOST`, `DBT_DATABRICKS_HTTP_PATH`, `DBT_CATALOG`, `DATABRICKS_TOKEN`, `DATABRICKS_SILVER_JOB_ID`, and `AIRFLOW_CONN_DATABRICKS_DEFAULT`.
- `.env.example` lines 31–41: documents Databricks configuration variables.

---

## 2. Logic Chain

### Step 2.1: Operator Replacement
1. **From Obs 1.1**: The DAG imports `DatabricksRunNowOperator` from `airflow.providers.databricks.operators.databricks` and instantiates it with `task_id="run_silver_transform_on_databricks"`.
2. **From Obs 1.2 & PROJECT.md (Interface Contracts)**: The project contract specifies `airflow.providers.amazon.aws.operators.glue.GlueJobOperator`, task ID `run_silver_transform_on_glue`, connection `aws_conn_id="aws_default"`, `job_name=os.environ.get("GLUE_SILVER_JOB_NAME", "ecommerce-silver-transform")`, and `wait_for_completion=True`.
3. **From Obs 1.2**: In AWS Glue runtime, job arguments are passed via `script_args` dict to the `StartJobRun` API. The keys must match the CLI argument flags:
   - `"--bronze-root"`: `bronze_root`
   - `"--silver-root"`: `silver_root`
   - `"--glue-database"`: `os.environ.get("DBT_SILVER_SCHEMA", "silver")`
4. Therefore, `GlueJobOperator` is a direct drop-in replacement that aligns with `silver_transform.py`'s `GLUE_OPTION_SPEC`.

### Step 2.2: Preflight Validation Decoupling
1. **From Obs 1.1**: `runtime_check()` asserts `DBT_DATABRICKS_HOST`, `DBT_DATABRICKS_HTTP_PATH`, `DBT_CATALOG`, `DATABRICKS_TOKEN`, and `DATABRICKS_SILVER_JOB_ID`. It also asserts `DATABRICKS_SILVER_JOB_ID.isdigit()`.
2. Because Databricks compute and Unity Catalog are eliminated (per R1 and R2), these 5 checks cause false failure if retained.
3. Glue jobs identify by name (alphanumeric, hyphens, underscores) rather than numeric cluster/job IDs.
4. Preflight checks must ensure valid S3 bucket configuration (`AIRFLOW_S3_BUCKET` or `S3_BUCKET`), valid Glue job name format (`[A-Za-z0-9_-]+`), and preserve the existing 1C isolated prefix and schema invariants (`DBT_SILVER_SCHEMA` and `DBT_TARGET_SCHEMA` separation when `PIPELINE_S3_PREFIX` is set, and `POSTGRES_DB` ending in `_1c_test`).

### Step 2.3: DAG Graph Integrity & Dependency Preservation
1. **From Obs 1.1 (lines 154–162)**:
   The DAG structure has 11 tasks in total:
   - Preflight: `preflight` (`runtime_check`)
   - Pipeline start: `started` (`record_pipeline_start`)
   - Parallel Bronze ingestion: `cdc`, `rates`, `campaign`, followed by `clickstream` (downstream of `cdc`)
   - Silver transformation: `silver_job` (downstream of `rates`, `campaign`, `clickstream`)
   - Gold/marts build: `build` (`build_gold_and_marts_dbt`, downstream of `silver_job`)
   - Success branch: `success` (`record_pipeline_success`, downstream of `build`)
   - Failure branch: `failure` (`record_pipeline_failure`, trigger_rule `one_failed`, downstream of all 8 pipeline tasks), followed by `fail_dag_run`.
2. By replacing `silver_job = DatabricksRunNowOperator(...)` with `silver_job = GlueJobOperator(...)` and setting `task_id="run_silver_transform_on_glue"`, all Python dependency operators (`>>`) continue to reference the `silver_job` variable without changing the topological execution order.
3. The failure handling mechanism (`for upstream in (preflight, started, cdc, rates, campaign, clickstream, silver_job, build): upstream >> failure`) correctly includes the Glue operator.

### Step 2.4: Test Suite Synchronization
1. **From Obs 1.3**: `tests/test_stage6_dag.py` asserts:
   - `dag.task_dict["build_gold_and_marts_dbt"].upstream_task_ids == {"run_silver_transform_on_databricks"}`
   - `dag.task_dict["run_silver_transform_on_databricks"].upstream_task_ids == {...}`
   - `isinstance(silver_task, DatabricksRunNowOperator)`
   - `silver_task.python_params` contains root paths.
2. In Milestone 4, `tests/test_stage6_dag.py` must be updated to assert:
   - `upstream_task_ids == {"run_silver_transform_on_glue"}`
   - `isinstance(silver_task, GlueJobOperator)`
   - `silver_task.job_name == os.environ.get("GLUE_SILVER_JOB_NAME", "ecommerce-silver-transform")`
   - `silver_task.aws_conn_id == "aws_default"`
   - `silver_task.wait_for_completion is True`
   - `silver_task.script_args` matches the expected root paths and `--glue-database`.

---

## 3. Caveats

1. **Airflow Runtime Environment**:
   `airflow.models` and `airflow.providers.amazon` are installed within the Airflow Docker image (`docker/airflow.Dockerfile`), not in the repository root local virtual environment (`pyproject.toml`). Local test executions via `python -m unittest tests/test_stage6_dag.py` correctly skip with `"run in the Airflow Docker image"`. For full offline validation outside Docker, AST/syntax checks (`ast.parse`) or unit tests with mock modules can be used.
2. **S3 Bucket Env Var Precedence**:
   In `ecommerce_cdc_daily.py`, `bucket` was extracted strictly from `os.environ.get("AIRFLOW_S3_BUCKET", "")`. In the proposed design, falling back to `os.environ.get("AIRFLOW_S3_BUCKET") or os.environ.get("S3_BUCKET", "")` makes the DAG resilient whether `AIRFLOW_S3_BUCKET` or `S3_BUCKET` is configured.
3. **Glue Script Arguments Key Format**:
   AWS Glue expects CLI argument keys passed via `script_args` to include the leading `--` prefix (e.g., `"--bronze-root"`). `GlueJobOperator` forwards these arguments directly to AWS Glue `StartJobRun`. `silver_transform.py`'s `parse_glue_args()` directly resolves options matching these flags.

---

## 4. Conclusion & Proposed Specification

### 4.1 Proposed Implementation for `airflow/dags/ecommerce_cdc_daily.py`

#### A. Imports & Module Variables
```python
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
```

#### B. DAG Definition
```python
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
```

#### C. Preflight Runtime Check (`runtime_check` / `validate_env`)
```python
    @task(task_id="runtime_check")
    def runtime_check() -> None:
        bucket = os.environ.get("AIRFLOW_S3_BUCKET") or os.environ.get("S3_BUCKET")
        if not bucket:
            raise ValueError("Configure AIRFLOW_S3_BUCKET or S3_BUCKET runtime variable")
        
        job_name = os.environ.get("GLUE_SILVER_JOB_NAME", "ecommerce-silver-transform")
        if not re.fullmatch(r"[A-Za-z0-9_-]+", job_name):
            raise ValueError(f"GLUE_SILVER_JOB_NAME must contain only safe characters: {job_name!r}")
            
        if S3_PREFIX and (
            os.environ.get("DBT_SILVER_SCHEMA", "silver") == "silver"
            or os.environ.get("DBT_TARGET_SCHEMA", "analytics_dev") == "analytics_dev"
        ):
            raise ValueError("An isolated PIPELINE_S3_PREFIX requires separate DBT_SILVER_SCHEMA and DBT_TARGET_SCHEMA")
        if S3_PREFIX and not os.environ.get("POSTGRES_DB", "").endswith("_1c_test"):
            raise ValueError("An isolated 1C pipeline requires a *_1c_test PostgreSQL database")
```

#### D. Silver Glue Operator Definition
```python
    bucket = os.environ.get("AIRFLOW_S3_BUCKET") or os.environ.get("S3_BUCKET", "")
    bronze_root = f"s3://{bucket}/{S3_PREFIX}" if S3_PREFIX else f"s3://{bucket}"
    silver_root = f"{bronze_root}/silver/pipeline_live"
    silver_job = GlueJobOperator(
        task_id="run_silver_transform_on_glue",
        job_name=GLUE_SILVER_JOB_NAME,
        aws_conn_id="aws_default",
        wait_for_completion=True,
        script_args={
            "--bronze-root": bronze_root,
            "--silver-root": silver_root,
            "--glue-database": os.environ.get("DBT_SILVER_SCHEMA", "silver"),
        },
    )
```

#### E. Task Dependencies (Unchanged Logic, Updated References)
```python
    preflight = runtime_check()
    preflight >> started >> [cdc, rates, campaign]
    cdc >> clickstream
    [rates, campaign, clickstream] >> silver_job >> build
    success, failure = record_pipeline_success(), record_pipeline_failure()
    build >> success
    for upstream in (preflight, started, cdc, rates, campaign, clickstream, silver_job, build):
        upstream >> failure
    failure >> fail_dag_run()
```

---

### 4.2 Proposed Contract Updates for `tests/test_stage6_dag.py`
In `tests/test_stage6_dag.py`:
```python
        # Upstream of build is now the Glue task
        self.assertEqual(dag.task_dict["build_gold_and_marts_dbt"].upstream_task_ids, {"run_silver_transform_on_glue"})
        
        # Upstream of Glue task are the Bronze ingestion tasks
        self.assertEqual(dag.task_dict["run_silver_transform_on_glue"].upstream_task_ids, {
            "ingest_exchange_rates_to_bronze", "ingest_campaign_fixture_to_bronze", "generate_demo_clickstream_to_bronze",
        })
        
        # Operator type and contract assertions
        silver_task = dag.task_dict["run_silver_transform_on_glue"]
        from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
        self.assertIsInstance(silver_task, GlueJobOperator)
        self.assertEqual(silver_task.job_name, os.environ.get("GLUE_SILVER_JOB_NAME", "ecommerce-silver-transform"))
        self.assertEqual(silver_task.aws_conn_id, "aws_default")
        self.assertTrue(silver_task.wait_for_completion)
        
        prefix = os.environ.get("PIPELINE_S3_PREFIX", "").strip("/")
        bucket = os.environ.get("AIRFLOW_S3_BUCKET") or os.environ.get("S3_BUCKET", "")
        root = f"s3://{bucket}" + (f"/{prefix}" if prefix else "")
        self.assertEqual(silver_task.script_args, {
            "--bronze-root": root,
            "--silver-root": f"{root}/silver/pipeline_live",
            "--glue-database": os.environ.get("DBT_SILVER_SCHEMA", "silver"),
        })
```

---

## 5. Verification Method

### 5.1 Syntax and AST Verification (Offline)
Run python compile and AST validation across DAG and test files:
```powershell
uv run python -m compileall airflow
uv run python -c "import ast; ast.parse(open('airflow/dags/ecommerce_cdc_daily.py').read()); print('AST verification passed')"
```

### 5.2 Unit Test Discovery (Offline)
Ensure existing offline unit test suite continues to pass with zero failures:
```powershell
uv run python -m unittest discover tests
```

### 5.3 In-Container Airflow DAG Verification (Docker)
Once Docker container is built (`docker build -f docker/airflow.Dockerfile -t ecommerce-airflow-local:stage6 .`):
```bash
docker run --rm -v $(pwd):/workspace ecommerce-airflow-local:stage6 python -m unittest tests/test_stage6_dag.py
```
Expected output:
```
Ran 2 tests in ...s
OK
```

### 5.4 Invalidation Conditions
- If `GlueJobOperator` receives positional arguments rather than keyword arguments for `job_name` or `script_args`.
- If `script_args` keys omit the `--` prefix required by AWS Glue `StartJobRun`.
- If `DBT_DATABRICKS_*` or `DATABRICKS_*` environment variables are referenced in `runtime_check()`.
- If `build_gold_and_marts_dbt` is triggered before `run_silver_transform_on_glue` finishes.
