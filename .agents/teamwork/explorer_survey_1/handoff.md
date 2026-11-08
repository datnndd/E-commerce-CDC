# Handoff Report: Spark Silver Layer Survey for AWS Glue Migration (Requirement R1)

## 1. Observation

### 1.1 Existing Silver Implementation Files
- **`spark_jobs/silver_transform.py`** (379 lines):
  - Line 20-33: `get_spark()` initializes SparkSession with Databricks check `if "DATABRICKS_RUNTIME_VERSION" in os.environ:` or configures local Delta session via `configure_spark_with_delta_pip(builder)`.
  - Line 52-65: `load_manifests_from_s3(spark, s3_root, dataset)` reads S3 manifests using `spark.read.format("binaryFile").option("recursiveFileLookup", "true").option("pathGlobFilter", "_manifest.json").option("ignoredPathSegmentRegex", "")`.
  - Line 67-79: `verify_manifest_file(path, file, spark)` verifies file size and SHA-256 digest against `file["size_bytes"]` and `file["sha256"]`.
  - Line 81-135: `committed_frames(spark, root, dataset)` loads all committed runs, asserts row count equality, and attaches `bronze_run_id` and `ingested_at`.
  - Line 138-145: `quarantine_rows(dataset, frame)` formats quarantine records with deterministic `record_id` (SHA256 of `dataset|bronze_run_id|record_json`).
  - Line 147-206: `normalize(dataset, frame)` applies string trimming, type casting, lowercase/uppercase transformations, field validation checks, and batch deduplication via `Window.partitionBy(*KEYS[dataset]).orderBy(F.col("updated_at").desc(), F.col("ingested_at").desc(), F.col("row_hash").desc())`.
  - Line 209-230: `quarantine_orphan_clickstream(spark, silver_root, frame)` performs referential integrity joins against `slv_customers`, `slv_orders`, and `slv_order_items`, routing orphan events to quarantine.
  - Line 232-251: `merge_delta(spark, frame, path, keys, quarantine=False)` executes CDC MERGE with update condition:
    `source.row_hash <> target.row_hash AND (source.updated_at > target.updated_at OR (source.updated_at = target.updated_at AND (source.ingested_at > target.ingested_at OR (source.ingested_at = target.ingested_at AND source.row_hash > target.row_hash))))`.
  - Line 254-267: `run_dataset(spark, bronze_root, silver_root, dataset)` enforces hard quality stops: negative payment values (`good.filter(F.col("payment_value") < 0)`) and future timestamps (`good.filter(F.col("event_timestamp") > F.current_timestamp())`).
  - Line 286-296: `validate_frames(frames)` runs Great Expectations expectations on `slv_orders`, `slv_payments`, and `slv_clickstream_events`.
  - Line 298-333: `verify_silver_quality(spark, silver_root)` validates key uniqueness, join completeness, and GX suites across all 12 Silver tables.
  - Line 335-346: `register_unity_catalog(spark, silver_root, catalog, schema)` issues Databricks-specific SQL `CREATE SCHEMA IF NOT EXISTS`, `CREATE TABLE ... USING DELTA LOCATION`, and `ALTER TABLE ... SET LOCATION`.
  - Line 348-375: `main()` parses CLI arguments `--bronze-root`, `--silver-root`, `--dataset`, `--catalog`, `--schema`.
- **`spark_jobs/silver_schema.py`** (30 lines):
  - Defines `OLTP_NAMES` (9 tables), `SILVER_NAMES` (12 tables), and primary/merge `KEYS` dictionary.
- **`tests/test_silver_manifest.py`** (78 lines):
  - Contains 6 unit tests covering initial Delta append mode, manifest discovery, payload SHA-256 verification, and corruption rejection.
  - Tested directly with `uv run --frozen python -m unittest tests/test_silver_manifest.py`: passed 6 tests in 0.015s.
- **`airflow/dags/ecommerce_cdc_daily.py`** (163 lines):
  - Line 11: `from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator`.
  - Line 115-125: Task `run_silver_transform_on_databricks` triggers Databricks job `DATABRICKS_SILVER_JOB_ID` with `python_params`.
  - Line 63-82: Preflight check verifies Databricks host, token, catalog, and job ID.

### 1.2 Databricks Incident Context
- **`docs/plans/active/databricks-silver-orchestration.md`** lines 71-73:
  - Full isolated Databricks Job `922817689219073` and `1171339633060` succeeded on Delta MERGE and GX quality checks.
  - Subsequent Airflow DAG execution failed during Silver execution: `RESOURCE_EXHAUSTED: ... hit your free daily limit`.
  - This motivated the complete migration to AWS Glue and AWS-native services.

---

## 2. Logic Chain

1. **Portable Core Engine**:
   - Observations 1.1 demonstrate that the core data processing in `spark_jobs/silver_transform.py` (reading Bronze manifests from S3, checksum verification, row hashing, deduplication via window functions, orphan quarantine, and Delta MERGE) relies strictly on standard Apache Spark (`pyspark.sql`) and standard Delta Lake (`delta.tables.DeltaTable`).
   - None of this core transformation logic requires Databricks proprietary runtime features.

2. **Decoupling Databricks Dependencies**:
   - Observation 1.1 reveals that Databricks coupling is isolated to:
     a) `DATABRICKS_RUNTIME_VERSION` check in `get_spark()`.
     b) `register_unity_catalog()` which issues Databricks-specific SQL to register external tables into Unity Catalog.
     c) Databricks job entrypoint argument handling.
   - Requirement R1 explicitly commands: *"The job must read Bronze data from S3, perform all Silver transformations and validations, and write clean Silver tables directly to S3 without calling Databricks APIs or Unity Catalog endpoints."*
   - Therefore, removing `register_unity_catalog()`, eliminating `DATABRICKS_RUNTIME_VERSION`, and reading/writing directly to S3 paths completely removes the Databricks and Unity Catalog dependencies.

3. **AWS Glue Compatibility & Dual-Mode Execution**:
   - In AWS Glue 4.0/5.0, Delta Lake is natively enabled via the job parameter `--datalake-formats delta`.
   - AWS Glue passes parameters as CLI arguments parsed via `awsglue.utils.getResolvedOptions`.
   - However, in local CI/CD environments and offline unit tests, `awsglue` is not installed (as observed in `pyproject.toml`).
   - By structuring the argument resolution and Spark initialization to dynamically detect AWS Glue (checking `awsglue` availability and `--JOB_NAME` in `sys.argv`), the script can run inside AWS Glue ETL jobs while retaining 100% offline testability via `argparse` and local `SparkSession`.

4. **S3 Access & Security Simplification**:
   - In AWS Glue, standard IAM role execution policies grant permissions to the target S3 bucket.
   - S3 paths (`s3://...`) are read and written using standard Hadoop S3A/S3 connectors already embedded in the AWS Glue Spark runtime, removing the need for Unity Catalog external storage credentials or separate `boto3` tokens.

---

## 3. Caveats

1. **No Live AWS Environment during Survey**:
   - The survey was conducted in read-only local mode without live AWS Glue execution credentials. Live cloud submission and AWS Glue job creation will be tested during downstream milestone validation if live credentials are provided.
2. **Downstream Gold & dbt Contract**:
   - While Requirement R1 focuses on the Silver Spark job, downstream dbt models currently query Silver tables using `{{ source('silver', '<table_name>') }}`. When migrating Gold (Requirement R2), the dbt adapter (e.g., `dbt-athena` or `dbt-glue`) must point to the Silver tables either via AWS Glue Data Catalog external tables or direct S3 location views.
3. **Great Expectations Library in AWS Glue**:
   - Great Expectations is installed in the local environment (`great-expectations[spark]>=1.19,<2`). In AWS Glue, it can be provided via `--additional-python-modules great-expectations==1.24.1`. The quality gate also includes native PySpark checks (`groupBy`, `count`, `filter`, `left_anti`) which are guaranteed to execute without external dependencies.

---

## 4. Conclusion

The Silver processing layer is well-isolated and ready for migration to AWS Glue:
1. `spark_jobs/silver_transform.py` contains the authoritative cleaning, deduplication, quarantine, and Delta MERGE logic for all 12 tables and can be migrated directly into an AWS Glue ETL script.
2. The migration requires refactoring argument parsing to support AWS Glue's `getResolvedOptions` while maintaining `argparse` fallback for offline tests, replacing the Databricks session builder with `GlueContext`, and removing Unity Catalog registration.
3. The offline unit test suite (`tests/test_silver_manifest.py`) currently passes and should be extended to validate Glue parameter contracts offline.

Detailed findings, entity-by-entity schemas, quarantine rules, and code blueprints are documented in:
`c:\Users\ddat2\Downloads\Projects\E-commerce-CDC\.agents\teamwork\explorer_survey_1\analysis.md`

---

## 5. Verification Method

### 5.1 Offline Unit Test Verification
Run the existing Silver manifest and S3 reading integrity tests:
```powershell
uv run --frozen python -m unittest tests/test_silver_manifest.py
```
**Expected outcome**: All 6 tests pass with returncode 0 (`OK`).

### 5.2 Python Syntax & Compilation Verification
Verify that all Silver scripts compile cleanly without syntax errors:
```powershell
python -m compileall spark_jobs/ tests/test_silver_manifest.py
```
**Expected outcome**: All `.py` files compile with returncode 0.

### 5.3 Local Integration Proof (when Docker available)
```powershell
docker run --rm -v "%CD%:/workspace" -e SILVER_TEST_DOCKER=1 ecommerce-silver-local uv run --frozen python -m unittest tests/test_silver_local.py
```
**Expected outcome**: All 12 tables replay, overlap deduplication, late updates, and quarantine tests pass.

### 5.4 Invalidation Conditions
- Any removal of the 15-minute overlap deduplication rule (`ROW_NUMBER() OVER (...)`).
- Any modification that bypasses `_manifest.json` SHA-256 and size checksum verification.
- Any regression that breaks offline unit tests or requires live cloud credentials to run tests.
