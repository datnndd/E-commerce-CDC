# Comprehensive Technical Analysis: SparkSession Initialization, Delta Lake Setup, S3 I/O, and Unity Catalog Decoupling

**Agent**: `explorer_m1_2`  
**Milestone**: M1 (AWS Glue Spark Silver Job Migration)  
**Deliverable**: Technical Architecture, Delta Lake Configuration, Direct S3 I/O Verification, and Unity Catalog Removal  
**Working Directory**: `c:\Users\ddat2\Downloads\Projects\E-commerce-CDC\.agents\teamwork\explorer_m1_2\`  
**Target Code**: `spark_jobs/silver_transform.py`  
**Date**: 2026-11-07  

---

## 1. Executive Summary

This investigation explores and defines the technical requirements for decoupling `spark_jobs/silver_transform.py` from Databricks and Unity Catalog, enabling it to run natively as an AWS Glue Spark ETL job while maintaining 100% offline local testability.

### Key Conclusions:
1. **SparkSession Initialization**: AWS Glue provides `awsglue.context.GlueContext(SparkContext.getOrCreate()).spark_session`. By catching `ImportError` when `awsglue` is absent, `get_spark()` cleanly falls back to a standalone local SparkSession configured with `io.delta.sql.DeltaSparkSessionExtension`, `DeltaCatalog`, and `configure_spark_with_delta_pip()`.
2. **Delta Lake in AWS Glue**: AWS Glue natively integrates Delta Lake via the job execution parameter `--datalake-formats delta`. This automatically adds Delta Lake JARs to the classpath and configures Delta catalog extensions. `configure_spark_with_delta_pip()` must NOT be invoked inside Glue because it attempts to resolve Maven packages over the public internet, which fails in isolated AWS Glue VPCs.
3. **Unity Catalog Removal**: `register_unity_catalog()` contains the only `spark.sql` statements in `silver_transform.py` (`CREATE SCHEMA IF NOT EXISTS`, `CREATE TABLE ... USING DELTA LOCATION`, `ALTER TABLE ... SET LOCATION`). Completely deleting this function removes all Databricks and Unity Catalog coupling. Unused `import re` can also be eliminated.
4. **Direct S3 Delta I/O**: The Silver pipeline reads Bronze manifests and data from S3, executes CDC MERGEs, writes quarantine records, and runs quality checks using **pure directory paths** (`s3://<bucket>/silver/pipeline_live/<dataset>`). Delta Lake is self-describing via `_delta_log/` directories on S3; zero external metastore or catalog interactions are required during the Silver transformation.
5. **Caller Compatibility**: All 7 existing repository callers of `local_spark()` (`verify_silver.py`, `verify_gold.py`, `silver_quality.py`, `test_silver_local.py`, `test_stage6_quality.py`, `test_gold_scd2_local.py`, `test_silver_postgres_e2e.py`) remain completely unbroken because `local_spark() -> SparkSession: return get_spark()` is preserved.

---

## 2. Examination of `get_spark()` and `register_unity_catalog()`

### 2.1 Current Implementation in `spark_jobs/silver_transform.py`

#### `get_spark()` (Lines 20–34):
```python
def get_spark() -> SparkSession:
    """Return active Databricks SparkSession, or configure a local standalone one."""
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

**Issues Identified**:
- Explicitly checks `"DATABRICKS_RUNTIME_VERSION" in os.environ`. On AWS Glue, this environment variable is never set.
- Lacks any detection of AWS Glue runtime (`awsglue.context.GlueContext`).
- Calling `configure_spark_with_delta_pip` on AWS Glue is hazardous because it sets `spark.jars.packages` and attempts Maven Central downloads, failing in private VPC subnets.
- Docstring references Databricks.

#### `register_unity_catalog()` (Lines 335–346):
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

**Issues Identified**:
- Issues Databricks-specific SQL:
  - `CREATE SCHEMA IF NOT EXISTS \`{catalog}\`.\`{schema}\``
  - `CREATE TABLE IF NOT EXISTS \`{catalog}\`.\`{schema}\`.\`{name}\` USING DELTA LOCATION '{location}'`
  - `ALTER TABLE \`{catalog}\`.\`{schema}\`.\`{name}\` SET LOCATION '{location}'`
- Standard Apache Spark and AWS Glue do not have a 3-tier catalog router named backtick catalog (`\`catalog\`.\`schema\``) by default; executing this on AWS Glue throws `ParseException` or `AnalysisException`.
- The regular expression validation `re.fullmatch` was added solely for Unity Catalog identifier sanitization; `import re` on line 8 is not used anywhere else in `silver_transform.py`.
- In `main()` lines 353–357 and 370–371:
  - CLI flags `--catalog` and `--schema` exist solely to feed `register_unity_catalog()`.
  - Line 356 raises a CLI error: `if args.dataset and args.catalog: parser.error("--dataset cannot be combined with --catalog; register only after the full quality gate")`.
  - Line 371 calls `register_unity_catalog(spark, args.silver_root, args.catalog, args.schema)`.

---

## 3. SparkSession & Delta Lake Initialization: AWS Glue vs Local Fallback

### 3.1 AWS Glue Runtime Architecture
In AWS Glue 3.0 / 4.0 / 5.0:
- The execution environment is launched with PySpark pre-configured.
- The standard AWS Glue driver initializes via:
  ```python
  from awsglue.context import GlueContext
  from pyspark.context import SparkContext
  
  sc = SparkContext.getOrCreate()
  glue_context = GlueContext(sc)
  spark = glue_context.spark_session
  ```
- **Delta Lake Integration**: When configuring the Glue Job via Airflow (`GlueJobOperator`) or AWS CLI, passing the parameter:
  `--datalake-formats delta`
  instructs the AWS Glue service to:
  1. Add Delta Lake JAR files (`delta-spark`, `delta-storage`) to the Spark JVM classpath.
  2. Set `spark.sql.extensions = io.delta.sql.DeltaSparkSessionExtension`.
  3. Set `spark.sql.catalog.spark_catalog = org.apache.spark.sql.delta.catalog.DeltaCatalog`.
- As a result, `glue_context.spark_session` already has Delta Lake support fully enabled and ready for use.
- **Job Lifecycle Management**:
  ```python
  from awsglue.job import Job
  job = Job(glue_context)
  job.init(job_name, args_dict)
  ...
  job.commit()
  ```
  `job.init()` sets up job bookmarks and CloudWatch metric collection. `job.commit()` commits the job state upon successful completion.

### 3.2 Local Fallback for Offline Development & Tests
In local development, Docker containers, and offline unit test environments:
- `awsglue` is NOT installed (it is not in `pyproject.toml` and is an AWS-proprietary runtime library).
- Any attempt to `import awsglue` raises `ModuleNotFoundError` / `ImportError`.
- By wrapping the AWS Glue initialization in a `try...except ImportError` block:
  ```python
  try:
      from awsglue.context import GlueContext
      from pyspark.context import SparkContext

      sc = SparkContext.getOrCreate()
      glue_context = GlueContext(sc)
      return glue_context.spark_session
  except ImportError:
      pass
  ```
  The code falls through safely to the local `SparkSession.builder`.
- The local builder configures:
  - `master("local[2]")`
  - `appName("ecommerce-silver")`
  - `config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")`
  - `config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")`
  - `config("spark.sql.session.timeZone", "UTC")`
  - `config("spark.ui.showConsoleProgress", "false")`
  - `config("spark.sql.shuffle.partitions", "2")`
  - `configure_spark_with_delta_pip(builder).getOrCreate()`
- Note: `from delta import configure_spark_with_delta_pip` is moved inside the local fallback block so that Glue environments lacking the PIP helper don't encounter import friction.

### 3.3 Active Session Check
Before attempting either Glue or local builder initialization, the function checks:
```python
active = SparkSession.getActiveSession()
if active is not None:
    return active
```
This guarantees that in unit test fixtures (e.g. `tests/test_silver_local.py` which creates `cls.spark = local_spark()`), subsequent calls to `get_spark()` reuse the running test session immediately.

### 3.4 Preservation of `local_spark()`
Seven project files import `local_spark`:
1. `orchestration/silver_quality.py`: `from spark_jobs.silver_transform import local_spark, validate_frames`
2. `spark_jobs/verify_silver.py`: `from spark_jobs.silver_transform import KEYS, SILVER_NAMES, local_spark`
3. `spark_jobs/verify_gold.py`: `from spark_jobs.silver_transform import SILVER_NAMES, local_spark`
4. `tests/test_gold_scd2_local.py`: `from spark_jobs.silver_transform import local_spark`
5. `tests/test_silver_local.py`: `from spark_jobs.silver_transform import KEYS, OLTP_NAMES, SILVER_NAMES, local_spark, run_dataset`
6. `tests/test_stage6_quality.py`: `from spark_jobs.silver_transform import local_spark`
7. `tests/test_silver_postgres_e2e.py`: `from spark_jobs.silver_transform import local_spark`

Retaining:
```python
def local_spark() -> SparkSession:
    return get_spark()
```
guarantees 100% backward compatibility with all existing verification scripts and test suites without requiring edits across those 7 files.

---

## 4. Removal of Unity Catalog and Databricks Dependencies

### 4.1 Deletions Required
1. **Delete `register_unity_catalog()`**: Remove lines 335–346 entirely.
2. **Remove `DATABRICKS_RUNTIME_VERSION`**:
   - Remove line 25: `if "DATABRICKS_RUNTIME_VERSION" in os.environ:`
   - Remove line 360: `"DATABRICKS_RUNTIME_VERSION" not in os.environ`
3. **Remove `import re`**: Line 8 (`import re`) was only referenced on line 337. Remove from module imports.
4. **Update Module Docstrings**:
   - Line 1: Change `"(local or Databricks)"` to `"(local or AWS Glue)"`.
   - Line 21: Change `"Return active Databricks SparkSession..."` to `"Return active SparkSession, AWS Glue SparkSession, or configure a local standalone one."`.
5. **Decouple CLI Arguments**:
   - In `main()`: Delete the registration branch:
     ```python
     if args.catalog:
         register_unity_catalog(spark, args.silver_root, args.catalog, args.schema)
     ```
   - In argument parsing: Accept `--catalog` and `--schema` as optional, deprecated arguments (or inert flags) to avoid breaking callers or scripts that still pass them during transition, but never execute any table registration.
   - Remove line 356 constraint `if args.dataset and args.catalog: parser.error(...)`.

---

## 5. Direct S3 Delta I/O Verification

### 5.1 Storage Architecture
The Silver layer writes Delta Lake tables to direct S3 locations without registering external tables in any metastore during execution:

| Target Table | S3 Storage Path | Format | Partitioning / Identity |
|--------------|-----------------|--------|-------------------------|
| `slv_customers` | `s3://<bucket>/silver/pipeline_live/slv_customers` | Delta Lake | `customer_id` |
| `slv_geolocation` | `s3://<bucket>/silver/pipeline_live/slv_geolocation` | Delta Lake | `geolocation_id` |
| `slv_category_translation` | `s3://<bucket>/silver/pipeline_live/slv_category_translation` | Delta Lake | `product_category_name` |
| `slv_products` | `s3://<bucket>/silver/pipeline_live/slv_products` | Delta Lake | `product_id` |
| `slv_sellers` | `s3://<bucket>/silver/pipeline_live/slv_sellers` | Delta Lake | `seller_id` |
| `slv_orders` | `s3://<bucket>/silver/pipeline_live/slv_orders` | Delta Lake | `order_id` |
| `slv_order_items` | `s3://<bucket>/silver/pipeline_live/slv_order_items` | Delta Lake | `order_id`, `order_item_id` |
| `slv_payments` | `s3://<bucket>/silver/pipeline_live/slv_payments` | Delta Lake | `order_id`, `payment_sequential` |
| `slv_reviews` | `s3://<bucket>/silver/pipeline_live/slv_reviews` | Delta Lake | `review_row_id` |
| `slv_clickstream_events` | `s3://<bucket>/silver/pipeline_live/slv_clickstream_events` | Delta Lake | `event_id` |
| `slv_exchange_rates` | `s3://<bucket>/silver/pipeline_live/slv_exchange_rates` | Delta Lake | `date`, `base`, `quote` |
| `slv_campaigns` | `s3://<bucket>/silver/pipeline_live/slv_campaigns` | Delta Lake | `campaign_id` |
| `silver_quarantine` | `s3://<bucket>/silver/pipeline_live/silver_quarantine` | Delta Lake | `record_id` |

### 5.2 Verification of Path-Based Operations

1. **Table Existence Check**:
   `DeltaTable.isDeltaTable(spark, path_str)` directly checks the filesystem/S3 prefix for the presence of `_delta_log/`. It makes no calls to any catalog.
2. **Initial Table Creation**:
   `frame.write.format("delta").mode("append").save(path_str)`
   Writes Parquet files directly under `path_str` and writes the initial transaction log entry `_delta_log/00000000000000000000.json`.
3. **Subsequent MERGE**:
   `target = DeltaTable.forPath(spark, path_str)` loads the table state directly from S3 transaction logs.
   `target.alias("target").merge(frame.alias("source"), condition)...execute()` writes new Parquet data files and logs the next transaction commit to S3.
4. **Referential Integrity Reads (`quarantine_orphan_clickstream`)**:
   `customers = spark.read.format("delta").load(f"{root}/slv_customers")`
   Loads Silver tables directly from their S3 URI paths without catalog lookups.
5. **Quality Verification (`verify_silver_quality`)**:
   `frame = spark.read.format("delta").load(table_path)` loads each table directly from S3 to verify key uniqueness, joins, and Great Expectations suites.
6. **S3 Authentication & Permissions**:
   Under AWS Glue, Spark transparently authenticates to S3 via the Glue Job's IAM execution role (`AWSGlueServiceRole`). No AWS credentials, tokens, or external location grants are required in code.

---

## 6. Implementation Recommendations and Exact Code Changes

### 6.1 Unified `get_spark()` Code

```python
def get_spark() -> SparkSession:
    """Return active SparkSession, AWS Glue SparkSession, or configure a local standalone one."""
    active = SparkSession.getActiveSession()
    if active is not None:
        return active

    try:
        from awsglue.context import GlueContext
        from pyspark.context import SparkContext

        sc = SparkContext.getOrCreate()
        glue_context = GlueContext(sc)
        return glue_context.spark_session
    except ImportError:
        pass

    builder = (
        SparkSession.builder.master("local[2]")
        .appName("ecommerce-silver")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.shuffle.partitions", "2")
    )
    from delta import configure_spark_with_delta_pip

    return configure_spark_with_delta_pip(builder).getOrCreate()
```

### 6.2 AWS Glue Job Lifecycle and Driver Shutdown in `main()`

```python
def main() -> None:
    args = parse_arguments()  # Dual-mode parsed Namespace from explorer_m1_1

    spark = get_spark()

    # Initialize AWS Glue Job tracking if running inside Glue runtime
    glue_job = None
    if "awsglue" in sys.modules and getattr(args, "job_name", None):
        try:
            from awsglue.context import GlueContext
            from awsglue.job import Job
            from pyspark.context import SparkContext

            glue_context = GlueContext(SparkContext.getOrCreate())
            glue_job = Job(glue_context)
            glue_job.init(args.job_name, vars(args) if hasattr(args, "__dict__") else {})
        except Exception as exc:
            print(f"Warning: Glue Job initialization skipped: {exc}")

    is_standalone_local = not is_s3_uri(args.silver_root) and "awsglue" not in sys.modules
    try:
        datasets = args.dataset or SILVER_NAMES
        for dataset in datasets:
            valid, quarantined = run_dataset(spark, args.bronze_root, args.silver_root, dataset)
            print(f"{dataset}: {valid} latest valid rows, {quarantined} quarantined")

        if not args.dataset:
            verify_silver_quality(spark, args.silver_root)

        if glue_job is not None:
            glue_job.commit()
    finally:
        if is_standalone_local:
            spark.stop()
```

### 6.3 Code Replacement Summary for Implementer

| Section | Lines in Current Code | Proposed Action |
|---------|-----------------------|-----------------|
| Docstring | 1 | Update to reflect AWS Glue support |
| Imports | 8, 12 | Remove `import re`, move `configure_spark_with_delta_pip` into `get_spark` fallback |
| `get_spark()` | 20–34 | Replace Databricks check with GlueContext detection + local builder fallback |
| `register_unity_catalog()` | 335–346 | Completely delete function |
| CLI `--catalog` / `--schema` | 353–357 | Handled by dual-mode parser (`explorer_m1_1`), removing error check and registration calls |
| Registration invocation | 370–371 | Completely delete `if args.catalog: register_unity_catalog(...)` |
| `is_standalone_local` | 360 | Update to `not is_s3_uri(args.silver_root) and "awsglue" not in sys.modules` |
| Glue Job commit | new | Call `glue_job.commit()` when `glue_job` is active |

---

## 7. Verification Method

### 7.1 Existing Offline Regression
Run the unit tests covering manifest reading and S3 Delta merge mode:
```powershell
uv run --frozen python -m unittest tests/test_silver_manifest.py
```
Expected: 6 tests pass in < 0.05s.

### 7.2 Full Offline Test Suite
Run all repository unit tests:
```powershell
uv run --frozen python -m unittest discover tests -p "test_*.py"
```
Expected: All non-Docker unit tests pass cleanly without errors.

### 7.3 Python Static Syntax & Compilation
Verify all Python files compile with zero syntax errors:
```powershell
python -m compileall spark_jobs/ tests/
```
Expected: Returncode 0.
