# Technical Analysis: Spark Silver Processing Layer Survey & AWS Glue Migration Blueprint

## 1. Executive Summary

This survey provides a comprehensive architectural and technical assessment of the Spark Silver processing layer within the E-commerce CDC Lakehouse project. The survey is conducted to support **Requirement R1**: migrating the Silver layer from Databricks to an AWS Glue Spark job, eliminating Databricks free edition compute quota limits (`RESOURCE_EXHAUSTED`), removing Databricks-specific runtime APIs and Unity Catalog dependencies, while strictly preserving all data transformations, CDC deduplication, quarantine policies, and quality validation gates.

The codebase currently implements a robust, idempotent Bronze-to-Delta Silver pipeline across 12 distinct datasets (9 PostgreSQL OLTP tables, 1 Clickstream event stream, 2 external REST API sources) plus a centralized `silver_quarantine` table. The existing PySpark code is cleanly structured in `spark_jobs/silver_transform.py` and `spark_jobs/silver_schema.py`. It is highly portable, with only minor Databricks-specific touchpoints:
1. Databricks runtime detection (`DATABRICKS_RUNTIME_VERSION`) in SparkSession initialization.
2. Unity Catalog SQL registration commands (`CREATE SCHEMA`, `CREATE TABLE ... USING DELTA LOCATION`, `ALTER TABLE ... SET LOCATION`).
3. Databricks-specific Airflow operator (`DatabricksRunNowOperator`) in `airflow/dags/ecommerce_cdc_daily.py`.
4. Databricks SQL connection dependencies in operational tracking (`orchestration/ops_runs.py`).

Migrating to an AWS Glue Spark job (`AWS Glue 4.0` or `5.0`) is architecturally straightforward because AWS Glue natively supports Delta Lake via the `--datalake-formats delta` parameter, uses standard S3 path URIs, and inherits IAM role permissions for direct S3 read/write operations without requiring external location credential vending.

---

## 2. Current Silver Layer Codebase Inventory

| File Path | Lines | Purpose / Role | Key Dependencies |
|-----------|-------|----------------|------------------|
| `spark_jobs/silver_transform.py` | 379 | Core Silver transformation engine: Bronze manifest loading, checksum validation, row deduplication, normalization, quarantine routing, CDC Delta MERGE, referential integrity joins, and Great Expectations checks | `pyspark`, `delta`, `great_expectations`, `spark_jobs.silver_schema` |
| `spark_jobs/silver_schema.py` | 30 | Master schema metadata: definitions of `OLTP_NAMES` (9 tables), `SILVER_NAMES` (12 tables), and primary/merge `KEYS` dictionary | Standard Python |
| `spark_jobs/verify_silver.py` | 51 | CLI verification utility for Delta Silver tables: asserts table existence, row counts, key uniqueness, non-null keys, and clickstream purchase joins | `pyspark`, `delta`, `spark_jobs.silver_transform` |
| `orchestration/silver_quality.py` | 36 | Standalone runner for Great Expectations quality checks on Silver Delta tables | `spark_jobs.silver_transform` |
| `airflow/dags/ecommerce_cdc_daily.py` | 163 | Airflow orchestration DAG: triggers ingestion tasks, submits Databricks Silver job via `DatabricksRunNowOperator`, gates downstream dbt Gold execution | `airflow`, `airflow.providers.databricks` |
| `orchestration/ops_runs.py` | 188 | Operational run auditor: records run metadata, table counts, and volume drop alerts into Delta table via Databricks SQL Warehouse | `databricks.sql`, `psycopg`, `boto3` |
| `tests/test_silver_manifest.py` | 78 | Fast, offline unit test suite: verifies S3 manifest listing, checksum validation, append mode writes, and entrypoint execution without live AWS/Databricks | `unittest`, `unittest.mock` |
| `tests/test_silver_local.py` | 186 | Full integration test suite: verifies replay idempotency, 15-minute overlap deduplication, late-arriving updates, quarantine routing, and quality gate triggers | `unittest`, `pyspark`, `delta`, `pyarrow` |
| `tests/test_silver_postgres_e2e.py` | 92 | E2E integration test: verifies price updates propagating from PostgreSQL through Bronze Parquet to Delta Silver | `unittest`, `psycopg`, `docker` |
| `tests/test_stage6_dag.py` | 71 | Airflow DAG structure test: verifies task dependencies, operator types, parameter passing, and alert callbacks | `unittest`, `airflow` |
| `docker/silver.Dockerfile` | 15 | Standalone container definition for running local Spark/Delta Silver jobs under Java 17 and Python 3.13 | `python:3.13-slim-bookworm`, `uv` |

---

## 3. Databricks-Specific Dependencies & APIs

### 3.1 Runtime Detection & Session Creation
In `spark_jobs/silver_transform.py`:
```python
def get_spark() -> SparkSession:
    active = SparkSession.getActiveSession()
    if active is not None:
        return active
    if "DATABRICKS_RUNTIME_VERSION" in os.environ:
        return SparkSession.builder.getOrCreate()
    builder = (SparkSession.builder.master("local[2]").appName("ecommerce-silver")
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.ui.showConsoleProgress", "false")
               .config("spark.sql.shuffle.partitions", "2"))
    return configure_spark_with_delta_pip(builder).getOrCreate()
```
- **Databricks coupling**: Checks for `"DATABRICKS_RUNTIME_VERSION"` environment variable. In Databricks, Delta Lake and S3 connectors are pre-installed in the runtime.
- **Glue migration impact**: In AWS Glue, `GlueContext` wraps `SparkSession`. Glue automatically configures Delta Lake when the job parameter `--datalake-formats delta` is provided. The script must initialize `GlueContext(SparkContext.getOrCreate())` when running on AWS Glue, while falling back cleanly to standard/local `SparkSession` when running offline or in unit tests.

### 3.2 Unity Catalog Table Registration
In `spark_jobs/silver_transform.py`:
```python
def register_unity_catalog(spark: SparkSession, silver_root: str, catalog: str, schema: str) -> None:
    for value in (catalog, schema):
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", value):
            raise ValueError(f"Unsafe Unity Catalog identifier: {value!r}")
    root_str = str(silver_root).rstrip("/")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
    for name in list(SILVER_NAMES.values()) + ["silver_quarantine"]:
        location = f"{root_str}/{name}/"
        spark.sql(f"CREATE TABLE IF NOT EXISTS `{catalog}`.`{schema}`.`{name}` USING DELTA LOCATION '{location}'")
        spark.sql(f"ALTER TABLE `{catalog}`.`{schema}`.`{name}` SET LOCATION '{location}'")
        print(f"Registered Unity Catalog table `{catalog}`.`{schema}`.`{name}` at {location}")
```
- **Databricks coupling**: Uses Unity Catalog 3-level namespace (`<catalog>.<schema>.<table>`) and executes Databricks Delta table registration SQL statements.
- **Glue migration impact**: Requirement R1 explicitly specifies: *"write clean Silver tables directly to S3 without calling Databricks APIs or Unity Catalog endpoints"*. This entire function should be eliminated or decoupled. If AWS Glue Data Catalog registration is desired for Athena access, it should use AWS Glue Data Catalog 2-level namespace (`<database>.<table>`) or Glue Catalog APIs, completely free of Unity Catalog.

### 3.3 Command-Line Interface & Parameter Passing
In `spark_jobs/silver_transform.py`:
- `main()` parses `--bronze-root`, `--silver-root`, `--dataset`, `--catalog`, `--schema`.
- Airflow passes these as Databricks Python task parameters (`python_params`).
- **Glue migration impact**: AWS Glue jobs receive parameters as command-line arguments prefixed with `--` and parsed using `awsglue.utils.getResolvedOptions`. A Glue-compatible script must support `getResolvedOptions` while remaining backward-compatible with standard `argparse` for offline tests.

### 3.4 Orchestration & Connection Couplings
In `airflow/dags/ecommerce_cdc_daily.py`:
- Line 11: `from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator`
- Line 23: `DATABRICKS_SILVER_JOB_ID = os.environ.get("DATABRICKS_SILVER_JOB_ID", "")`
- Lines 115-125: `DatabricksRunNowOperator` calls Databricks job `DATABRICKS_SILVER_JOB_ID`.
- Lines 66-74: Preflight checks require `DBT_DATABRICKS_HOST`, `DBT_DATABRICKS_HTTP_PATH`, `DBT_CATALOG`, `DATABRICKS_TOKEN`, `DATABRICKS_SILVER_JOB_ID`.
- In `compose.yaml`: `AIRFLOW_CONN_DATABRICKS_DEFAULT` is configured.
- In `docker/airflow.Dockerfile`: `apache-airflow-providers-databricks` is installed.

---

## 4. Bronze S3 Reading, Validation, & Silver Writing

### 4.1 S3 Layout & Bronze Paths

| Source Category | Datasets | Bronze S3 Path Pattern | File Format & Options |
|-----------------|----------|------------------------|-----------------------|
| **PostgreSQL OLTP** (9 tables) | `customers`, `geolocation`, `product_category_name_translation`, `products`, `sellers`, `orders`, `order_items`, `order_payments`, `order_reviews` | `s3://<bucket>/[<prefix>/]bronze/postgres_oltp/<dataset>/load_date=YYYY-MM-DD/run_id=YYYYMMDDTHHMMSSffffff/part-0000.parquet` | Snappy Parquet (batches of up to 500k rows). Contains `ingested_at` and `source_table` columns. |
| **Clickstream** | `clickstream` | `s3://<bucket>/[<prefix>/]bronze/clickstream/dt=YYYY-MM-DD/hour=HH/run_id=YYYYMMDDTHHMMSSffffff/events.json.gz` | Gzipped JSONL (`multiLine=False`). Event fields: `event_id`, `event_type`, `timestamp`, `session_id`, `customer_id`, `product_id`, `device`, `utm_source`, optional `order_id`. |
| **API: Rates** | `exchange_rate` | `s3://<bucket>/[<prefix>/]bronze/api/exchange_rate/dt=YYYY-MM-DD/run_id=YYYYMMDDTHHMMSSffffff/response.json` | Multiline JSON array of objects: `[{"date": "YYYY-MM-DD", "base": "BRL", "quote": "USD", "rate": 0.20}, ...]`. |
| **API: Campaign** | `campaign` | `s3://<bucket>/[<prefix>/]bronze/api/campaign/dt=YYYY-MM-DD/run_id=YYYYMMDDTHHMMSSffffff/response.json` | Multiline JSON object: `{"synthetic": true, "campaigns": [{"campaign_id": "...", "utm_source": "...", "discount_pct": ...}]}`. |

### 4.2 Manifest Discovery & Parsing Contract
In `load_manifests_from_s3`:
1. Discovers `_manifest.json` under `s3_root/bronze/...` using Spark's `binaryFile` data source:
   ```python
   spark.read.format("binaryFile")
       .option("recursiveFileLookup", "true")
       .option("pathGlobFilter", "_manifest.json")
       .option("ignoredPathSegmentRegex", "")
       .load(path)
   ```
   *Critical Spark behavior*: Spark's default path filter excludes any file starting with `_` or `.`. Setting `.option("ignoredPathSegmentRegex", "")` is mandatory for Spark to discover `_manifest.json`.
2. Decodes manifest content as UTF-8 JSON.
3. Filters for `manifest.get("status") == "committed"` (uncommitted runs in status `"writing"` are ignored).
4. Verifies `source_system` and `dataset` match the directory structure.
5. Sorts committed manifests chronologically by `started_at` to ensure deterministic replay ordering.

### 4.3 Checksum and File Integrity Verification
In `verify_manifest_file`:
- For each file entry in `manifest["files"]`:
  - Path safety: validates file name contains no directory traversal (`Path(name).name == name` and `name not in (".", "..")`).
  - Integrity check: reads file using Spark `binaryFile` and calculates size (`F.length("content")`) and SHA-256 digest (`F.sha2("content", 256)`).
  - Asserts calculated size and digest match `file["size_bytes"]` and `file["sha256"]`.
  - Asserts Spark DataFrame row count matches `file["row_count"]` (if provided).
  - Asserts combined run DataFrame row count matches `manifest["row_count"]`.
  - Any mismatch raises `ValueError` before data is processed.

### 4.4 Metadata Column Enrichment
- Injects `bronze_run_id = F.lit(manifest["run_id"])`.
- For non-OLTP datasets (Clickstream, API), injects `ingested_at = F.try_to_timestamp(F.lit(manifest["started_at"]))`.
- Unions all discovered committed runs across history using `unionByName(..., allowMissingColumns=True)`.

### 4.5 Silver Target S3 Writing & Delta MERGE
In `merge_delta`:
- Target path: `s3://<bucket>/[<prefix>/]silver/pipeline_live/<slv_table_name>`.
- Initial write:
  - If `not DeltaTable.isDeltaTable(spark, path_str)`: writes via `frame.write.format("delta").mode("append").save(path_str)`.
  - *Design note*: In Spark Connect/Serverless, `errorifexists` is unsupported; append mode is proven safe for initial creation.
- Subsequent CDC writes:
  - Loads target via `DeltaTable.forPath(spark, path_str)`.
  - Condition: equality on primary key columns (`target.key = source.key`).
  - Quarantine table merge: uses `whenNotMatchedInsertAll()` keyed on `record_id`.
  - Silver table merge update condition:
    ```sql
    source.row_hash <> target.row_hash AND (
        source.updated_at > target.updated_at OR (
            source.updated_at = target.updated_at AND (
                source.ingested_at > target.ingested_at OR (
                    source.ingested_at = target.ingested_at AND source.row_hash > target.row_hash
                )
            )
        )
    )
    ```
  - Matched action: `whenMatchedUpdateAll(condition=newer)`.
  - Not matched action: `whenNotMatchedInsertAll()`.
  - *Guarantees*: Idempotent re-runs; late-arriving records with older `updated_at` never overwrite newer state; identical records generate zero Delta version churn.

---

## 5. Detailed Schema Contracts, Deduplication Rules, & Transformations

### 5.1 Primary Keys & Target Table Names

| Entity / Source | Silver Table Name | Primary / Merge Key(s) | Key Data Type(s) |
|---|---|---|---|
| `customers` | `slv_customers` | `["customer_id"]` | `StringType` |
| `geolocation` | `slv_geolocation` | `["geolocation_id"]` | `LongType` (BigInteger) |
| `product_category_name_translation` | `slv_category_translation` | `["product_category_name"]` | `StringType` |
| `products` | `slv_products` | `["product_id"]` | `StringType` |
| `sellers` | `slv_sellers` | `["seller_id"]` | `StringType` |
| `orders` | `slv_orders` | `["order_id"]` | `StringType` |
| `order_items` | `slv_order_items` | `["order_id", "order_item_id"]` | `StringType`, `IntegerType` |
| `order_payments` | `slv_payments` | `["order_id", "payment_sequential"]` | `StringType`, `IntegerType` |
| `order_reviews` | `slv_reviews` | `["review_row_id"]` | `LongType` (BigInteger) |
| `clickstream` | `slv_clickstream_events` | `["event_id"]` | `StringType` |
| `exchange_rate` | `slv_exchange_rates` | `["date", "base", "quote"]` | `DateType`, `StringType`, `StringType` |
| `campaign` | `slv_campaigns` | `["campaign_id"]` | `StringType` |
| `quarantine` | `silver_quarantine` | `["record_id"]` | `StringType` (SHA-256) |

### 5.2 Common Normalization Pipeline
Executed across all datasets:
1. **Timestamp Casting**:
   - `updated_at`: parsed via `F.try_to_timestamp("updated_at")`.
   - `ingested_at`: parsed via `F.try_to_timestamp("ingested_at")`.
2. **String Trimming**: All string fields are trimmed: `F.trim(F.col(field.name))`.
3. **Dataset-Specific Normalizations**:
   - `orders`: `order_status` converted to lowercase (`F.lower("order_status")`).
   - `exchange_rate`: `date` cast to `date`; `rate` cast to `decimal(18,8)`; `base` and `quote` converted to uppercase (`F.upper`); `updated_at` set to `ingested_at`.
   - `campaign`: JSON unnesting via `select("synthetic", F.explode("campaigns").alias("campaign")).select("synthetic", "campaign.*")`; `discount_pct` cast to `int`; `updated_at` set to `ingested_at`; `utm_source` converted to lowercase (`F.lower`).
   - `clickstream`: `timestamp` parsed to `event_timestamp` (`F.try_to_timestamp("timestamp")`); original `timestamp` dropped; `updated_at` set to `event_timestamp`; `utm_source` converted to lowercase (`F.lower`).
4. **Row Hash Computation**:
   - Calculates content column list: all columns excluding `{"bronze_run_id", "ingested_at", "quarantine_reason", "row_hash"}`.
   - Computes `row_hash = F.sha2(F.to_json(F.struct(*content_columns)), 256)`.

### 5.3 Quarantine Rules by Entity

| Entity | Quarantine Condition | Assigned Quarantine Reason | Action |
|---|---|---|---|
| **All entities** | Any primary key is NULL or empty string `""` | `f"missing {key}"` | Row routed to `silver_quarantine` |
| **All entities** | `updated_at` is NULL | `"missing or invalid updated_at"` | Row routed to `silver_quarantine` |
| **All entities** | `ingested_at` is NULL | `"missing or invalid ingested_at"` | Row routed to `silver_quarantine` |
| `customers` | `customer_id` is NULL or empty string `""` | `"missing customer_id"` | Row routed to `silver_quarantine` |
| `orders` | `customer_id` is NULL or empty string `""` | `"missing customer_id"` | Row routed to `silver_quarantine` |
| `order_items` | `price < 0` | `"negative price"` | Row routed to `silver_quarantine` |
| `products` | `unit_price < 0` (if column exists) | `"negative unit_price"` | Row routed to `silver_quarantine` |
| `products` | `quantity <= 0` (if column exists) | `"nonpositive quantity"` | Row routed to `silver_quarantine` |
| `exchange_rate` | `rate` is NULL or non-castable | `"missing or invalid rate"` | Row routed to `silver_quarantine` |
| `campaign` | `discount_pct` is NULL or non-castable | `"missing discount_pct"` | Row routed to `silver_quarantine` |
| `clickstream` | `product_id` is NULL or empty string `""` | `"missing product_id"` | Row routed to `silver_quarantine` |
| `clickstream` | `customer_id` is NULL or empty string `""` | `"missing customer_id"` | Row routed to `silver_quarantine` |
| `clickstream` | `event_type == 'purchase'` and `order_id` is NULL | `"purchase missing order_id"` | Row routed to `silver_quarantine` |
| `clickstream` | `customer_id` not found in `slv_customers` | `"customer not found in Silver"` | Row routed to `silver_quarantine` |
| `clickstream` | `event_type == 'purchase'` and `(order_id, customer_id)` not found in `slv_orders` | `"purchase order/customer not found in Silver"` | Row routed to `silver_quarantine` |
| `clickstream` | `event_type == 'purchase'` and `(order_id, product_id)` not found in `slv_order_items` | `"purchase order/item product not found in Silver"` | Row routed to `silver_quarantine` |

### 5.4 Hard Quality Gates (Job Stoppage)
Unlike rows that are diverted to `silver_quarantine`, certain critical anomalies trigger a hard pipeline failure before any Delta write:
1. **Negative Payment Gate**:
   ```python
   if dataset == "order_payments" and good.filter(F.col("payment_value") < 0).limit(1).count():
       raise ValueError("Quality gate failed: slv_payments has negative payment values")
   ```
2. **Future Event Timestamp Gate**:
   ```python
   if dataset == "clickstream" and good.filter(F.col("event_timestamp") > F.current_timestamp()).limit(1).count():
       raise ValueError("Quality gate failed: slv_clickstream_events has future event timestamps")
   ```
3. **Post-Merge Integrity Gate (`verify_silver_quality`)**:
   - Asserts all 12 Delta tables exist.
   - Asserts 0 duplicate primary keys (`groupBy(*KEYS).count().filter("count > 1")`).
   - Asserts 0 missing/null primary keys.
   - Asserts 0 orphaned purchase events against `slv_orders` and `slv_order_items` via `left_anti` joins.
   - Asserts Great Expectations suite passes on all 3 target frames.

### 5.5 Great Expectations Quality Contract
Validated via ephemeral GX context (`gx.get_context(mode="ephemeral")`):
- `slv_orders`: `ExpectColumnValuesToNotBeNull(column="customer_id")`
- `slv_payments`: `ExpectColumnValuesToBeBetween(column="payment_value", min_value=0)`
- `slv_clickstream_events`: `ExpectColumnValuesToBeBetween(column="event_timestamp", max_value=datetime.now(timezone.utc))`

---

## 6. Architectural Blueprint for AWS Glue Spark Silver Job

### 6.1 AWS Glue Runtime Specifications
- **Glue Version**: AWS Glue 4.0 (Apache Spark 3.3.0, Python 3.10) or AWS Glue 5.0 (Apache Spark 3.5.0, Python 3.11).
- **Compute Type**: Standard G.1X or G.2X worker type (e.g. 2 to 4 workers for demo/scale).
- **Native Delta Lake Enablement**:
  Add job parameter:
  ```
  --datalake-formats delta
  ```
  AWS Glue automatically injects Delta Spark extensions:
  - `spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension`
  - `spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog`
- **Additional Python Modules**:
  ```
  --additional-python-modules great-expectations==1.24.1
  ```
- **Job Parameter Interface**:
  Parameters passed by Airflow or CLI:
  - `--JOB_NAME`: standard Glue job name identifier
  - `--bronze-root`: S3 URI (e.g. `s3://<bucket>/bronze` or `s3://<bucket>/<prefix>/bronze`)
  - `--silver-root`: S3 URI (e.g. `s3://<bucket>/silver/pipeline_live` or `s3://<bucket>/<prefix>/silver/pipeline_live`)
  - `--dataset`: (optional) specific dataset to process (defaults to all 12)

### 6.2 Dual-Mode Script Architecture (Glue + Offline Compatible)
To satisfy Requirement R4 (offline verification without live cloud credentials), the migrated Silver job script must run seamlessly in both environments:
1. Inside AWS Glue runtime (using `GlueContext`, `Job.init()`, `Job.commit()`, and `getResolvedOptions`).
2. Locally or in CI/CD test runners (using standard `argparse` and standalone `SparkSession`).

```python
"""AWS Glue Spark Job for Silver Layer CDC, Deduplication, and Quality Gates."""

import sys
import argparse
from pyspark.sql import SparkSession

# Detect AWS Glue runtime
IS_GLUE = False
try:
    from awsglue.utils import getResolvedOptions
    from awsglue.context import GlueContext
    from awsglue.job import Job
    # Check if executed inside AWS Glue environment
    if any(arg.startswith("--JOB_NAME") for arg in sys.argv):
        IS_GLUE = True
except ImportError:
    pass

def init_environment():
    if IS_GLUE:
        args = getResolvedOptions(sys.argv, [
            "JOB_NAME",
            "bronze-root",
            "silver-root",
        ])
        # Optional parameters with defaults
        dataset = None
        for arg in sys.argv:
            if arg.startswith("--dataset="):
                dataset = arg.split("=", 1)[1]
        from pyspark.context import SparkContext
        sc = SparkContext.getOrCreate()
        glueContext = GlueContext(sc)
        spark = glueContext.spark_session
        job = Job(glueContext)
        job.init(args["JOB_NAME"], args)
        return spark, args["bronze-root"], args["silver-root"], dataset, job
    else:
        parser = argparse.ArgumentParser(description="AWS Glue Silver Transform (Offline / Local Mode)")
        parser.add_argument("--bronze-root", required=True)
        parser.add_argument("--silver-root", required=True)
        parser.add_argument("--dataset", action="append", choices=SILVER_NAMES)
        parsed, _ = parser.parse_known_args()
        spark = get_spark() # configured with delta extensions
        return spark, parsed.bronze_root, parsed.silver_root, parsed.dataset, None
```

### 6.3 Complete Decoupling from Databricks
1. **Drop `register_unity_catalog`**:
   The Databricks Unity Catalog registration step is deleted. Silver Delta tables exist purely as S3 Delta tables under `--silver-root/<slv_table_name>`.
2. **Zero Databricks APIs**:
   Remove all references to `DATABRICKS_RUNTIME_VERSION`, `dbutils`, and `databricks.sql`.
3. **S3 Security & Access**:
   AWS Glue runs with an attached AWS IAM Role that has S3 `GetObject`, `PutObject`, `ListBucket` permissions on the project S3 bucket. No access keys, tokens, or external location grants need to be wired into Spark code.
4. **Airflow Orchestration Replacement**:
   In `airflow/dags/ecommerce_cdc_daily.py`:
   Replace `DatabricksRunNowOperator` with `GlueJobOperator` from `airflow.providers.amazon.aws.operators.glue`:
   ```python
   silver_job = GlueJobOperator(
       task_id="run_silver_transform_on_aws_glue",
       job_name=os.environ.get("GLUE_SILVER_JOB_NAME", "ecommerce-silver-transform"),
       script_args={
           "--bronze-root": bronze_root,
           "--silver-root": silver_root,
       },
       aws_conn_id="aws_default",
       wait_for_completion=True,
   )
   ```

---

## 7. Migration Work Breakdown for Implementation

To implement Requirement R1, the implementation phase should execute the following concrete work items:

1. **Refactor `spark_jobs/silver_transform.py`**:
   - Add dual-mode argument parsing (`getResolvedOptions` when on Glue, `argparse` when standalone/test).
   - Support `GlueContext` / `SparkSession` initialization.
   - Remove `DATABRICKS_RUNTIME_VERSION` checks.
   - Remove `register_unity_catalog` function and CLI arguments (`--catalog`, `--schema`).
   - Retain 100% of the cleaning, typing, normalization, hashing, batch deduplication, orphan clickstream quarantine, Delta MERGE, and quality gate logic.
2. **Update `spark_jobs/verify_silver.py`**:
   - Ensure verification script continues to verify Delta tables on S3 or local paths without Unity Catalog.
3. **Update Test Suites**:
   - Verify `tests/test_silver_manifest.py` passes offline.
   - Add/update unit tests to verify Glue parameter parsing and execution under mock environments.
   - Ensure `tests/test_stage6_dag.py` reflects the updated operator and parameter contracts.
4. **Update Airflow and Environment Configurations**:
   - Replace Databricks environment variables in `.env.example` and `compose.yaml` with AWS Glue variables (`GLUE_SILVER_JOB_NAME`, `AWS_DEFAULT_REGION`).
   - Replace `apache-airflow-providers-databricks` with `apache-airflow-providers-amazon` in `docker/airflow.Dockerfile`.
