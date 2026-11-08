# Comprehensive Survey: Airflow Orchestration, Environment Configurations, and Test Suites (R3 & R4)

**Agent**: `explorer_survey_3`  
**Date**: 2026-11-07  
**Scope**: Airflow DAGs, Docker Compose, environment configurations, and offline test suite verification (Requirements R3 and R4).

---

## Executive Summary

This survey provides a thorough, code-level catalog and analysis of all Databricks dependencies present in the Airflow orchestration layer, Docker/environment configurations, and repository test suites. It details the required replacements with AWS-native services (specifically AWS Glue for Silver processing and AWS-compatible adapters for Gold/dbt), the offline execution behavior of the test suites, and an actionable migration plan to satisfy Acceptance Criteria R3 and R4 without requiring live AWS credentials.

Key survey findings:
1. **Airflow DAG (`airflow/dags/ecommerce_cdc_daily.py`)**: Uses `DatabricksRunNowOperator` from `airflow.providers.databricks.operators.databricks`, a hardcoded `databricks_default` connection ID, and explicit runtime checks for 5 Databricks environment variables (`DBT_DATABRICKS_HOST`, `DBT_DATABRICKS_HTTP_PATH`, `DBT_CATALOG`, `DATABRICKS_TOKEN`, `DATABRICKS_SILVER_JOB_ID`).
2. **Orchestration Metadata (`orchestration/ops_runs.py`)**: Directly imports `from databricks import sql` and establishes a live connection to the Databricks SQL Warehouse at pipeline start and finish. Decoupling this is required to prevent runtime crashes when Databricks credentials are removed.
3. **Docker and Environment Files**: `docker/airflow.Dockerfile` installs `apache-airflow-providers-databricks`. `compose.yaml`, `.env.example`, and `.env` wire Databricks tokens and host endpoints into the Airflow container.
4. **Repository Test Suite**: 34 unit tests exist across 10 test files. 21 tests pass offline immediately; 13 are skipped due to external environment flags (`SILVER_TEST_DOCKER`, `CDC_TEST_PG_PORT`, `airflow.models`). `tests/test_stage6_dag.py` asserts exact `DatabricksRunNowOperator` types and parameters, requiring an update to verify `GlueJobOperator` contracts.
5. **Offline Test Integrity**: Airflow's DAG parsing (`DagBag`) parses DAG structure and operator parameter contracts 100% in-memory without calling AWS endpoints. Adding mock-aware testing allows DAG integrity tests to run completely offline on both developer hosts and inside Docker.

---

## 1. Airflow Orchestration Deep Dive

### 1.1 Existing Airflow DAG Architecture
- **Location**: `airflow/dags/ecommerce_cdc_daily.py` (163 lines)
- **Airflow Version**: Built on Airflow 3 (`apache/airflow:3.3.2-python3.13`), utilizing `airflow.sdk` (`DAG`, `task`, `get_current_context`) and `airflow.providers.standard.operators.bash.BashOperator`.
- **Schedule and Behavior**: `@daily`, `start_date=datetime(2026, 11, 2, tzinfo=timezone.utc)`, `catchup=False`, `max_active_runs=1`.

### 1.2 Databricks Dependencies in `ecommerce_cdc_daily.py`
| Line Numbers | Item | Current Code | Migration Requirement |
|---|---|---|---|
| Line 11 | Operator import | `from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator` | Replace with `from airflow.providers.amazon.aws.operators.glue import GlueJobOperator` |
| Line 23 | Environment variable | `DATABRICKS_SILVER_JOB_ID = os.environ.get("DATABRICKS_SILVER_JOB_ID", "")` | Replace with `GLUE_SILVER_JOB_NAME = os.environ.get("GLUE_SILVER_JOB_NAME", "ecommerce_silver_transform")` |
| Line 60 | DAG tags | `tags=["ecommerce", "cdc", "databricks"]` | Update to `tags=["ecommerce", "cdc", "aws", "glue"]` |
| Lines 65–74 | Preflight check `runtime_check` | Validates `DBT_DATABRICKS_HOST`, `DBT_DATABRICKS_HTTP_PATH`, `DBT_CATALOG`, `DATABRICKS_TOKEN`, `DATABRICKS_SILVER_JOB_ID`, and `DATABRICKS_SILVER_JOB_ID.isdigit()` | Remove all 5 Databricks variable checks and digit validation; add validation for S3 and Glue variables |
| Lines 115–125 | Silver task `silver_job` | `DatabricksRunNowOperator(task_id="run_silver_transform_on_databricks", databricks_conn_id="databricks_default", job_id=DATABRICKS_SILVER_JOB_ID, python_params=[...])` | Replace with `GlueJobOperator(task_id="run_silver_transform_on_glue", job_name=GLUE_SILVER_JOB_NAME, script_args={...}, aws_conn_id="aws_default", region_name=..., wait_for_completion=True)` |

### 1.3 Task Graph and Dependencies
The existing task graph in `ecommerce_cdc_daily.py`:
```
preflight (runtime_check)
   │
   ▼
started (record_pipeline_start)
   │
   ├──────────────────────────┬─────────────────────────────┐
   ▼                          ▼                             ▼
cdc (extract_postgres_cdc)  rates (ingest_exchange_rates) campaign (ingest_campaign_fixture)
   │                          │                             │
   ▼                          │                             │
clickstream (demo)            │                             │
   │                          │                             │
   └──────────────────────────┼─────────────────────────────┘
                              ▼
           silver_job (run_silver_transform_on_databricks)
                              ▼
           build (build_gold_and_marts_dbt)
                              │
              ┌───────────────┴───────────────┐
              ▼                               ▼
  success (record_pipeline_success)       failure (record_pipeline_failure) [trigger_rule="one_failed"]
                                              ▼
                                         fail_dag_run
```
All tasks (`preflight`, `started`, `cdc`, `rates`, `campaign`, `clickstream`, `silver_job`, `build`) link directly to `failure` on error.
This dependency topology must be preserved exactly, replacing only the `silver_job` node (`run_silver_transform_on_databricks` -> `run_silver_transform_on_glue`).

### 1.4 Orchestration Metadata (`orchestration/ops_runs.py`)
- **Issue**: `record_pipeline_start`, `record_pipeline_success`, and `record_pipeline_failure` execute `python -m orchestration.ops_runs start/finish`.
- `ops_runs.py` contains:
  - Line 11: `from databricks import sql`
  - Lines 33–39: `def connection()` raises `ValueError("Set DATABRICKS_TOKEN, DBT_DATABRICKS_HOST and DBT_DATABRICKS_HTTP_PATH")` if Databricks credentials are absent.
  - Lines 42–57: Creates/updates `ops_pipeline_runs` table in Databricks SQL Warehouse.
- **Impact**: Removing Databricks credentials from Airflow environment causes `record_pipeline_start` to fail immediately.
- **Required Action**: Decouple `ops_runs.py` from Databricks SQL connector. Provide an AWS-compatible backend (Athena, S3 Delta table, PostgreSQL, or local SQLite/mock fallback) so the DAG tasks execute cleanly without Databricks tokens.

---

## 2. Docker Compose, Dockerfiles, and Environment Configurations

### 2.1 File Inventory and Required Changes

| Configuration File | Existing Databricks Configurations | Required AWS Replacement |
|---|---|---|
| `docker/airflow.Dockerfile` | `RUN pip install --no-cache-dir apache-airflow-providers-databricks` (Line 12) | Replace with `RUN pip install --no-cache-dir apache-airflow-providers-amazon` |
| `docker/silver.Dockerfile` | Configures local PySpark Delta image with OpenJDK 17 | Retain as reproducible local execution and unit-test harness for PySpark/Delta |
| `compose.yaml` (Lines 36–43) | `DBT_DATABRICKS_HOST`, `DBT_DATABRICKS_HTTP_PATH`, `DBT_CATALOG`, `DATABRICKS_TOKEN`, `DATABRICKS_SILVER_JOB_ID`, `AIRFLOW_CONN_DATABRICKS_DEFAULT` | Remove all Databricks vars. Add `AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `GLUE_SILVER_JOB_NAME`, `GLUE_SILVER_DATABASE`, and `AIRFLOW_CONN_AWS_DEFAULT` |
| `airflow/dbt_profile/profiles.yml` | `ecommerce_databricks` target with `type: databricks`, `auth_type: pat`, `host`, `http_path`, `token`, `database` | Update profile to match AWS Gold adapter (e.g., `type: athena` with `s3_staging_dir`, `s3_data_dir`, `database`, `schema`) |
| `.env.example` (Lines 31–41) | Section "Databricks Silver Job and dbt Gold" listing Databricks host, token, catalog, and job ID | Replace with AWS Glue / Athena section; clean up documentation and comments |
| `.env` | Live values for `DBT_DATABRICKS_HOST`, `DBT_DATABRICKS_HTTP_PATH`, `DATABRICKS_TOKEN`, `DATABRICKS_SILVER_JOB_ID`, etc. | Remove all Databricks entries |
| `pyproject.toml` (Line 9) | `"dbt-databricks>=1.10,<1.12"` | Replace with target AWS dbt adapter (e.g., `dbt-athena-community`) |

### 2.2 Connection Specification: AWS Glue in Airflow
In Airflow, the Amazon provider connects via `aws_default` connection or environment credentials.
- **Connection Environment Variable**:
  `AIRFLOW_CONN_AWS_DEFAULT='{"conn_type":"aws","extra":{"region_name":"ap-southeast-1"}}'`
- **GlueJobOperator Specification**:
  ```python
  silver_job = GlueJobOperator(
      task_id="run_silver_transform_on_glue",
      job_name=os.environ.get("GLUE_SILVER_JOB_NAME", "ecommerce_silver_transform"),
      script_args={
          "--bronze-root": bronze_root,
          "--silver-root": silver_root,
          "--glue-database": os.environ.get("GLUE_SILVER_DATABASE", "silver"),
      },
      aws_conn_id="aws_default",
      region_name=os.environ.get("AWS_REGION", "ap-southeast-1"),
      wait_for_completion=True,
  )
  ```

---

## 3. Test Suites and Offline Verification Analysis

### 3.1 Repository Test Inventory
Executing `uv run python -m unittest discover -s tests -p "test_*.py" -v` produces:
**34 total tests, 21 passed, 13 skipped, 0 failures**.

| Test File | Test Count | Status on Host | Reason / Dependencies |
|---|---|---|---|
| `tests/test_cdc_extract.py` | 11 tests (6 passed, 5 skipped) | CdcCredentialsTest: 6 PASSED<br>CdcExtractTest: 5 SKIPPED | Credentials tests run offline with `unittest.mock`. Extract tests skip unless `CDC_TEST_PG_PORT` is set for disposable PostgreSQL. |
| `tests/test_generate_from_bronze.py` | 2 tests (2 passed) | 2 PASSED | Pure in-memory deterministic PRNG and event simulation; 100% offline. |
| `tests/test_gold_scd2_local.py` | 1 test (1 skipped) | 1 SKIPPED | Tests SCD2 fact joins via `spark_jobs.verify_gold.render()`. Skips unless `SILVER_TEST_DOCKER=1`. |
| `tests/test_ingest_api.py` | 5 tests (5 passed) | 5 PASSED | Mocks HTTP responses and S3 uploads; 100% offline. |
| `tests/test_ingest_logs.py` | 2 tests (2 passed) | 2 PASSED | Gzip and manifest ordering tests using `FakeS3`; 100% offline. |
| `tests/test_silver_local.py` | 3 tests (3 skipped) | 3 SKIPPED | Local Delta table merge and quarantine tests. Skips unless `SILVER_TEST_DOCKER=1`. |
| `tests/test_silver_manifest.py` | 6 tests (6 passed) | 6 PASSED | Mocks PySpark binary reads, manifest validation, Delta write modes; 100% offline. |
| `tests/test_silver_postgres_e2e.py` | 1 test (1 skipped) | 1 SKIPPED | E2E integration test. Skips unless `SILVER_PG_E2E=1`. |
| `tests/test_stage6_dag.py` | 2 tests (2 skipped) | 2 SKIPPED | DAG graph and webhook tests. Skips unless `importlib.util.find_spec("airflow.models")` is True. |
| `tests/test_stage6_quality.py` | 1 test (1 skipped) | 1 SKIPPED | Great Expectations checkpoint validation. Skips unless `SILVER_TEST_DOCKER` is set. |

### 3.2 DAG Integrity Test Analysis (`tests/test_stage6_dag.py`)
Current test logic in `Stage6DagTest`:
1. `test_quality_precedes_gold_and_failure_branch_sees_all_tasks`:
   - Loads `DagBag(dag_folder=str(folder))`
   - Verifies `bag.import_errors` is empty
   - Verifies max active runs == 1
   - Verifies failure branch sees all upstream tasks with `trigger_rule="one_failed"`
   - Verifies `build_gold_and_marts_dbt` depends on `run_silver_transform_on_databricks`
   - Verifies `run_silver_transform_on_databricks` depends on 3 ingestion tasks
   - Checks `isinstance(silver_task, DatabricksRunNowOperator)`
   - Checks `silver_task.python_params[:4] == ["--bronze-root", root, "--silver-root", f"{root}/silver/pipeline_live"]`
   - Checks exchange rates task environment mapping
2. `test_webhook_alert_contains_task_context_without_secret`:
   - Dynamically loads module `ecommerce_cdc_daily.py` via `importlib.util.spec_from_file_location`
   - Mocks `urlopen` and tests `alert_failure({"task_instance": ti})`

### 3.3 Root Cause of Test Skipping on Host
1. **PySpark / Delta Tests (`SILVER_TEST_DOCKER=1`)**:
   On Windows host with modern JDK, PySpark raises `java.lang.UnsupportedOperationException: getSubject is not supported` because Hadoop native code and JDK 24+ security managers conflict. These tests are deliberately containerized inside `docker/silver.Dockerfile` (Linux Bookworm + OpenJDK 17).
2. **Airflow DAG Tests (`find_spec("airflow.models")`)**:
   `apache-airflow` is installed in `docker/airflow.Dockerfile`, not in local host `.venv`. When running on the host, importing `airflow` only finds the repository subfolder `airflow/` (as an empty namespace package), lacking `airflow.models`.

### 3.4 Strategy for 100% Offline DAG & Test Passing
To satisfy Requirement R4 ("DAG integrity and graph tests pass, verifying task dependencies and operator parameter contracts without requiring live AWS cloud credentials"):
- **Inside Airflow Docker Container**: Running `docker compose exec airflow python -m unittest discover tests` executes `test_stage6_dag.py` against real Airflow and `GlueJobOperator` classes without network requests.
- **On Developer Host**: In `test_stage6_dag.py`, when `airflow.models` is absent from `.venv`, provide a lightweight mock/shim layer that loads the DAG and verifies task IDs, dependencies, operator types, and parameter dictionaries directly. This guarantees `python -m unittest` passes completely on host without skipping!
- **Zero AWS Credential Requirement**:
  - `GlueJobOperator.__init__` does not make network calls or contact AWS STS.
  - Setting standard dummy credentials (`AWS_ACCESS_KEY_ID=test`, `AWS_SECRET_ACCESS_KEY=test`, `AWS_REGION=ap-southeast-1`) prevents boto3 from attempting remote instance metadata calls.
- **Manifest Tests (`test_silver_manifest.py`)**:
  - Update `test_spark_python_entrypoint_without_file_global`: replace namespace `databricks_spark_python_task` with the AWS Glue entrypoint namespace.

---

## 4. Code Syntax, Compilation, and Repository Hygiene Checks

The following validation checks were performed directly against the repository:

1. **Python Compilation**:
   ```powershell
   uv run python -m compileall airflow spark_jobs ingestion orchestration tests
   ```
   **Result**: Code 0. Zero syntax errors across all modules.

2. **Package Lockfile Integrity**:
   ```powershell
   uv lock --check
   ```
   **Result**: Code 0. All 105 resolved packages match `uv.lock`.

3. **Docker Compose Configuration**:
   ```powershell
   docker compose config --quiet
   ```
   **Result**: Code 0. Configuration parses cleanly with current `.env`.

4. **Git Formatting / Whitespace**:
   ```powershell
   git diff --check
   ```
   **Result**: Code 0. Clean working tree with no whitespace errors.

---

## 5. End-to-End Migration Plan for R3 and R4

### Step 1: Environment and Container Configuration (R3)
1. **Update `docker/airflow.Dockerfile`**:
   - Remove `apache-airflow-providers-databricks`.
   - Add `apache-airflow-providers-amazon`.
2. **Update `compose.yaml`**:
   - Remove Databricks environment variables (`DBT_DATABRICKS_HOST`, `DBT_DATABRICKS_HTTP_PATH`, `DBT_CATALOG`, `DATABRICKS_TOKEN`, `DATABRICKS_SILVER_JOB_ID`, `AIRFLOW_CONN_DATABRICKS_DEFAULT`).
   - Add `AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `GLUE_SILVER_JOB_NAME`, `GLUE_SILVER_DATABASE`, and `AIRFLOW_CONN_AWS_DEFAULT`.
3. **Update `.env.example` and `.env`**:
   - Replace Databricks section with AWS Glue configuration.
   - Clean up comments and obsolete external location notes.

### Step 2: Airflow DAG Migration (`airflow/dags/ecommerce_cdc_daily.py`) (R3)
1. **Imports**:
   - Replace `DatabricksRunNowOperator` with `GlueJobOperator`.
2. **Variables & Runtime Checks**:
   - Replace `DATABRICKS_SILVER_JOB_ID` with `GLUE_SILVER_JOB_NAME`.
   - In `runtime_check`: Remove Databricks variables and numeric check; add `GLUE_SILVER_JOB_NAME` and `AWS_REGION`.
3. **Silver Operator**:
   - Replace `run_silver_transform_on_databricks` with:
     ```python
     silver_job = GlueJobOperator(
         task_id="run_silver_transform_on_glue",
         job_name=os.environ.get("GLUE_SILVER_JOB_NAME", "ecommerce_silver_transform"),
         script_args={
             "--bronze-root": bronze_root,
             "--silver-root": silver_root,
             "--glue-database": os.environ.get("GLUE_SILVER_DATABASE", "silver"),
         },
         aws_conn_id="aws_default",
         region_name=os.environ.get("AWS_REGION", "ap-southeast-1"),
         wait_for_completion=True,
     )
     ```
4. **Update Tags**:
   - Change `tags=["ecommerce", "cdc", "databricks"]` to `tags=["ecommerce", "cdc", "aws", "glue"]`.
5. **Decouple `orchestration/ops_runs.py`**:
   - Remove `from databricks import sql`.
   - Implement an AWS/offline compatible storage mechanism for `ops_pipeline_runs`.

### Step 3: Test Suite Updates & Offline Verification (R4)
1. **Update `tests/test_stage6_dag.py`**:
   - Assert `dag.task_dict["build_gold_and_marts_dbt"].upstream_task_ids == {"run_silver_transform_on_glue"}`.
   - Assert `dag.task_dict["run_silver_transform_on_glue"].upstream_task_ids == {"ingest_exchange_rates_to_bronze", "ingest_campaign_fixture_to_bronze", "generate_demo_clickstream_to_bronze"}`.
   - Assert `isinstance(silver_task, GlueJobOperator)`.
   - Assert `silver_task.job_name == "ecommerce_silver_transform"` and verify `script_args`.
   - Add offline shim/mock capability so the test can execute both in Docker and on the local host without skipping.
2. **Update `tests/test_silver_manifest.py`**:
   - Update entrypoint test `test_spark_python_entrypoint_without_file_global` to test the AWS Glue entrypoint.
3. **Verify Full Offline Test Suite**:
   - Run `python -m unittest discover tests` with dummy AWS credentials and ensure zero errors.
   - Verify `python -m compileall airflow spark_jobs ingestion orchestration tests` passes with zero errors.
