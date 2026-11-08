# Project: Lakehouse Data Pipeline Migration from Databricks to AWS Glue and AWS-Native Services

## Architecture
- **Silver Processing Layer**: Standalone AWS Glue Spark job (`spark_jobs/silver_transform.py`) reading Bronze data from S3, applying cleaning, deduplication, quarantine, Delta CDC MERGE, and quality checks, writing directly to S3 Silver Delta paths without Databricks runtime or Unity Catalog dependencies.
- **Gold Transformation Layer**: dbt project (`dbt_project/`) migrated to AWS-compatible adapter (`ecommerce_aws` profile), maintaining 35 models, 2 snapshots, and 19 custom data tests in Spark SQL dialect, compiling and executing offline without Databricks host/token/warehouse.
- **Airflow Orchestration**: Airflow DAG (`airflow/dags/ecommerce_cdc_daily.py`) decoupled from Databricks, triggering the Silver job via `GlueJobOperator` and downstream dbt models via bash/dbt runner. Decoupled operational run logging (`orchestration/ops_runs.py`) and updated Docker Compose / environment configurations.
- **Verification Layer**: Offline-executable test suite verifying DAG graph integrity, Glue operator contracts, Bronze-to-Silver manifest validation, Gold SQL compilation, and full Python syntax checks without live AWS credentials.

## Feature Inventory
| # | Feature | Description | Milestone | Source |
|---|---------|-------------|-----------|--------|
| 1 | Glue Spark Silver Dual-Mode Entrypoint | Refactor CLI argument parsing to support AWS Glue `getResolvedOptions` when on Glue, and `argparse` when local/testing. | M1 | Survey (Explorer 1) |
| 2 | Decouple SparkSession from Databricks | Remove `DATABRICKS_RUNTIME_VERSION` checks and Databricks runtime assumptions in `get_spark()`. | M1 | Survey (Explorer 1) |
| 3 | Remove Unity Catalog Table Registration | Remove `register_unity_catalog()` SQL statements (`CREATE TABLE ... USING DELTA LOCATION`). | M1 | Survey (Explorer 1) |
| 4 | S3 Direct Delta I/O and Manifest Verification | Read Bronze manifests and write Silver Delta tables directly to S3 paths. | M1 | Survey (Explorer 1) |
| 5 | Silver Data Transformations Preservation | Preserve normalization, 15-min overlap deduplication, and orphan clickstream quarantine across all 12 tables. | M1 | Survey (Explorer 1) |
| 6 | Silver Delta CDC MERGE & Quality Gates | Preserve deterministic CDC MERGE precedence condition, negative value / future timestamp stops, and GX suites. | M1 | Survey (Explorer 1) |
| 7 | dbt Project Configuration Migration | Update `dbt_project.yml` profile reference from `ecommerce_databricks` to `ecommerce_aws`. | M2 | Survey (Explorer 2) |
| 8 | AWS dbt Profiles Configuration | Update `dbt_project/profiles.yml` and `profiles.yml.example` to AWS profile (`type: glue`) with safe defaults. | M2 | Survey (Explorer 2) |
| 9 | Airflow dbt Profiles Configuration | Update `airflow/dbt_profile/profiles.yml` to match the AWS adapter profile without Databricks tokens. | M2 | Survey (Explorer 2) |
| 10 | Decouple sources.yml from Unity Catalog | Remove `database: "{{ env_var('DBT_CATALOG') }}"` from `models/staging/sources.yml`. | M2 | Survey (Explorer 2) |
| 11 | Replace dbt-databricks Dependency | Replace `dbt-databricks` in `pyproject.toml` with AWS-compatible adapter (`dbt-glue`). | M2 | Survey (Explorer 2) |
| 12 | Offline Compilation of dbt Models | Verify `dbt parse` and `dbt compile` resolve all 35 models, 2 snapshots, and 19 tests offline without network calls. | M2 | Survey (Explorer 2) |
| 13 | Airflow DAG Operator Migration | Replace `DatabricksRunNowOperator` with `GlueJobOperator` (`run_silver_transform_on_glue`) in `ecommerce_cdc_daily.py`. | M3 | Survey (Explorer 3) |
| 14 | Airflow Preflight Validation Decoupling | Remove Databricks environment variable assertions from `validate_env()` in `ecommerce_cdc_daily.py`. | M3 | Survey (Explorer 3) |
| 15 | Orchestration Run Logger Decoupling | Decouple `orchestration/ops_runs.py` from `databricks.sql`, adding local/file or S3 metadata fallback. | M3 | Survey (Explorer 3) |
| 16 | Airflow Dockerfile Provider Migration | Replace `apache-airflow-providers-databricks` with `apache-airflow-providers-amazon` in `docker/airflow.Dockerfile`. | M3 | Survey (Explorer 3) |
| 17 | Compose & Environment Files Cleanup | Remove all Databricks env vars from `compose.yaml` and `.env.example`, adding AWS Glue/S3 configurations. | M3 | Survey (Explorer 3) |
| 18 | Airflow DAG Integrity & Graph Tests | Update `tests/test_stage6_dag.py` to assert `GlueJobOperator` contracts, task graph, and parameter mappings. | M4 | Survey (Explorer 3) |
| 19 | Offline Glue Job Invocation Tests | Add offline unit tests for Glue parameter parsing, S3 path contracts, and exit codes. | M4 | Survey (Explorer 1, 3) |
| 20 | Repository Offline Test Suite Pass | Ensure all offline tests in `tests/` pass with returncode 0 without live AWS credentials. | M4 | Survey (Explorer 3) |
| 21 | Syntax & Compilation Checks | Ensure `python -m compileall` passes cleanly across all project directories. | M4 | Survey (Explorer 3) |

## Milestones
| # | Name | Scope | Dependencies | Status |
|---|------|-------|-------------|--------|
| M1 | AWS Glue Spark Silver Job Migration | Features 1, 2, 3, 4, 5, 6 | None | DONE |
| M2 | Gold Layer & dbt Migration to AWS | Features 7, 8, 9, 10, 11, 12 | M1 (Interface contract: S3 Silver Delta paths) | DONE |
| M3 | Airflow Orchestration & Environment Decoupling | Features 13, 14, 15, 16, 17 | M1, M2 | DONE |
| M4 | Offline Verification, Test Suite Integrity & Acceptance | Features 18, 19, 20, 21 | M1, M2, M3 | DONE |

## Interface Contracts
### Silver Layer ↔ Gold Layer (dbt)
- **Path / Location**: Silver Delta tables written to `s3://<bucket>/silver/pipeline_live/<table_name>` (or local path during offline verification).
- **Table Names (12 tables)**: `slv_customers`, `slv_orders`, `slv_order_items`, `slv_payments`, `slv_reviews`, `slv_products`, `slv_sellers`, `slv_geolocation`, `slv_category_translation`, `slv_clickstream_events`, `slv_exchange_rates`, `slv_campaigns`.
- **Format**: Delta Lake format (`file_format: delta`).
- **dbt Source Reference**: `{{ source('silver', '<table_name>') }}` configured via `models/staging/sources.yml` with `schema: "{{ env_var('DBT_SILVER_SCHEMA', 'silver') }}"` (without Unity Catalog `database` prefix).

### Airflow Orchestration ↔ AWS Glue Silver Job
- **Operator**: `airflow.providers.amazon.aws.operators.glue.GlueJobOperator`.
- **Task ID**: `run_silver_transform_on_glue`.
- **Job Name**: `os.environ.get("GLUE_SILVER_JOB_NAME", "ecommerce-silver-transform")`.
- **Connection**: `aws_conn_id="aws_default"`.
- **Script Args**:
  - `--bronze-root`: `f"s3://{s3_bucket}/bronze"`
  - `--silver-root`: `f"s3://{s3_bucket}/silver/pipeline_live"`
  - `--glue-database`: `os.environ.get("DBT_SILVER_SCHEMA", "silver")`
- **Wait for completion**: `wait_for_completion=True`.

### Airflow Orchestration ↔ Ops Run Logger (`ops_runs.py`)
- CLI command: `python -m orchestration.ops_runs start --dag-id ... --run-id ...` and `finish --dag-id ... --run-id ... --status ...`.
- Backend: Decoupled from `databricks.sql`. Must log runs to local JSON/SQLite or S3 metadata without throwing connection errors if Databricks credentials are absent.

## Code Layout
- `spark_jobs/silver_transform.py`: AWS Glue Spark Silver ETL script.
- `spark_jobs/silver_schema.py`: Silver schemas, keys, and table definitions.
- `dbt_project/dbt_project.yml`: dbt project configuration.
- `dbt_project/profiles.yml`: Local / CI dbt profile for AWS.
- `dbt_project/models/staging/sources.yml`: Silver source tables configuration.
- `airflow/dags/ecommerce_cdc_daily.py`: Airflow production orchestration DAG.
- `airflow/dbt_profile/profiles.yml`: Airflow container dbt profile for AWS.
- `orchestration/ops_runs.py`: Pipeline execution logging utility.
- `docker/airflow.Dockerfile`: Airflow container definition.
- `compose.yaml`: Local container orchestration configuration.
- `pyproject.toml`: Project dependencies.
- `.env.example`: Template for environment variables.
- `tests/test_stage6_dag.py`: DAG integrity and graph tests.
- `tests/test_silver_manifest.py`: Silver manifest & S3 reading tests.
- `tests/test_silver_glue_offline.py`: Offline Glue argument parsing and parameter contract tests.
- `tests/test_silver_glue_adversarial.py`: Adversarial parameter resolution tests.
- `tests/test_silver_adversarial.py`: Adversarial CDC merge precedence and quarantine hashing tests.
