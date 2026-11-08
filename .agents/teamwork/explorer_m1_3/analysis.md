# Comprehensive Analysis: Silver Layer Data Quality, Normalization, CDC Deduplication, and Offline Verification for AWS Glue Migration

## 1. Executive Summary
The core transformation, deduplication, and quality gate logic in `spark_jobs/silver_transform.py` relies exclusively on standard PySpark DataFrame and Delta Lake OSS APIs, with zero coupling to Databricks runtime internals. Migration to AWS Glue Spark requires only refactoring the entrypoint/argument parser and decoupling SparkSession/table registration; all 12 dataset transformations, window deduplications, CDC MERGE precedence conditions, quarantine mechanics, and Great Expectations checks are fully portable and must be preserved verbatim.

---

## 2. Examination of Transformation & Cleaning Logic (`normalize`)

The `normalize(dataset: str, frame: DataFrame)` function in `spark_jobs/silver_transform.py` (lines 147–206) executes five sequential phases across incoming Bronze batches:

### 2.1 Timestamp & DataType Casting
- **`clickstream`**:
  - `event_timestamp = F.try_to_timestamp("timestamp")` (original `timestamp` column dropped).
  - `updated_at = F.col("event_timestamp")`.
- **`exchange_rate`**:
  - `date = F.expr("try_cast(date AS date)")`.
  - `rate = F.expr("try_cast(rate AS decimal(18,8))")`.
  - `updated_at = F.col("ingested_at")`.
- **`campaign`**:
  - `discount_pct = F.expr("try_cast(discount_pct AS int)")`.
  - `updated_at = F.col("ingested_at")`.
- **All OLTP datasets** (`customers`, `orders`, `order_items`, `order_payments`, `order_reviews`, `products`, `sellers`, `geolocation`, `product_category_name_translation`):
  - `updated_at = F.try_to_timestamp("updated_at")`.
- **Global**:
  - `ingested_at = F.try_to_timestamp("ingested_at")` across all datasets.

### 2.2 String Trimming & Casing Normalization
- **Global String Trimming**:
  ```python
  for field in frame.schema.fields:
      if isinstance(field.dataType, StringType):
          frame = frame.withColumn(field.name, F.trim(F.col(field.name)))
  ```
  Every StringType column in every dataset is stripped of leading and trailing whitespace.
- **Specific Casing Rules**:
  - `orders`: `order_status = F.lower("order_status")`.
  - `exchange_rate`: `base = F.upper("base")`, `quote = F.upper("quote")`.
  - `clickstream` & `campaign`: `utm_source = F.lower("utm_source")` (if column exists).

### 2.3 Required Silver Columns & Primary Key Quarantine
- Validates that `KEYS[dataset] + ["updated_at", "ingested_at"]` are present in `frame.columns`. If any are missing, raises `ValueError`.
- Builds a unified `quarantine_reason` expression via `F.when(...)`:
  1. For each `key in KEYS[dataset]`:
     `F.col(key).isNull() | (F.col(key) == "")` -> `"missing {key}"`
  2. `F.col("updated_at").isNull()` -> `"missing or invalid updated_at"`
  3. `F.col("ingested_at").isNull()` -> `"missing or invalid ingested_at"`
  4. Dataset-specific quarantine checks:
     - Any dataset with `customer_id`: null or empty -> `"missing customer_id"`.
     - `order_items`: `price < 0` -> `"negative price"`.
     - `order_items`: `quantity <= 0` -> `"nonpositive quantity"`; `unit_price < 0` -> `"negative unit_price"`.
     - `clickstream`: null/empty `product_id` -> `"missing product_id"`; `event_type == 'purchase'` and `order_id` is null -> `"purchase missing order_id"`.
     - `exchange_rate`: `rate.isNull()` -> `"missing or invalid rate"`.
     - `campaign`: `discount_pct.isNull()` -> `"missing discount_pct"`.

### 2.4 Row Hashing
- Computes SHA-256 hash over all content columns (excluding `bronze_run_id`, `ingested_at`, `quarantine_reason`, `row_hash`):
  ```python
  content_columns = sorted(set(frame.columns) - {"bronze_run_id", "ingested_at", "quarantine_reason", "row_hash"})
  frame = frame.withColumn("row_hash", F.sha2(F.to_json(F.struct(*[F.col(name) for name in content_columns])), 256))
  ```

---

## 3. Examination of CDC Deduplication & Window Precedence

### 3.1 In-Batch Window Deduplication (lines 204–206)
When Bronze files contain overlapping 15-minute replay windows, multiple versions of the same entity may exist in the incoming batch:
```python
window = Window.partitionBy(*KEYS[dataset]).orderBy(
    F.col("updated_at").desc(),
    F.col("ingested_at").desc(),
    F.col("row_hash").desc()
)
good = good.withColumn("_rank", F.row_number().over(window)).filter("_rank = 1").drop("_rank")
```
- **Ordering Hierarchy**:
  1. `updated_at.desc()`: Latest source transaction state wins.
  2. `ingested_at.desc()`: In case source update timestamp is identical, latest Bronze ingestion wins.
  3. `row_hash.desc()`: Invariant deterministic tie-breaker for identical timestamps.
- **Guarantee**: Produces exactly one row per primary key per execution batch, completely eliminating duplicate keys within the batch before passing to `merge_delta()`.

### 3.2 Delta Lake CDC MERGE Condition (`merge_delta`, lines 232–251)
```python
condition = " AND ".join(f"target.{key} = source.{key}" for key in keys)
merge = target.alias("target").merge(frame.alias("source"), condition)
newer = ("source.row_hash <> target.row_hash AND (source.updated_at > target.updated_at OR "
         "(source.updated_at = target.updated_at AND "
         "(source.ingested_at > target.ingested_at OR "
         "(source.ingested_at = target.ingested_at AND source.row_hash > target.row_hash))))")
merge.whenMatchedUpdateAll(condition=newer).whenNotMatchedInsertAll().execute()
```
- **Idempotence & Efficiency**: `source.row_hash <> target.row_hash` avoids redundant Parquet file rewrites if incoming data is unchanged.
- **Out-of-Order CDC Protection**: If delayed or out-of-order Bronze batches arrive, records with older `updated_at` cannot overwrite newer records already stored in Silver Delta.
- **Tie-Breaking**: Replays with identical `updated_at` and `ingested_at` use `source.row_hash > target.row_hash`.

---

## 4. Quarantine Mechanics & Clickstream Referential Integrity

### 4.1 Quarantine Row Formatting (`quarantine_rows`, lines 138–145)
- Encapsulates bad records into a uniform schema:
  - `record_id`: `sha2(concat_ws("|", dataset, bronze_run_id, record_json), 256)`
  - `dataset`: string name of source dataset.
  - `quarantine_reason`: reason string assigned during validation.
  - `record_json`: JSON string serialization of record fields.
  - `bronze_run_id`: ingestion batch identifier.
  - `ingested_at`: timestamp of Bronze ingestion.
- Deduplicated via `.dropDuplicates(["record_id"])`.
- Merged into `silver_quarantine` table with `whenNotMatchedInsertAll()` (append-only, immutable).

### 4.2 Orphan Clickstream Quarantine (`quarantine_orphan_clickstream`, lines 209–230)
- Executes referential integrity checks against existing Silver Delta tables:
  - Join against `slv_customers` on `customer_id`.
  - Join against `slv_orders` on `order_id` and `customer_id`.
  - Join against `slv_order_items` on `order_id` and `product_id`.
- Quarantines events where:
  - Customer does not exist in `slv_customers` -> `"customer not found in Silver"`.
  - Purchase event whose order/customer does not exist in `slv_orders` -> `"purchase order/customer not found in Silver"`.
  - Purchase event whose order/item does not exist in `slv_order_items` -> `"purchase order/item product not found in Silver"`.
- Clean events continue to `slv_clickstream_events`; orphaned events route to `silver_quarantine`.

---

## 5. Quality Gates & Great Expectations Validation

### 5.1 In-Flight Native PySpark Circuit Breakers (`run_dataset`, lines 260–263)
Before `merge_delta` is executed, hard-stop validations occur:
```python
if dataset == "order_payments" and good.filter(F.col("payment_value") < 0).limit(1).count():
    raise ValueError("Quality gate failed: slv_payments has negative payment values")
if dataset == "clickstream" and good.filter(F.col("event_timestamp") > F.current_timestamp()).limit(1).count():
    raise ValueError("Quality gate failed: slv_clickstream_events has future event timestamps")
```
- **Characteristics**: Fast fail with `.limit(1).count()` avoiding full dataset scans.
- **Fail-Safe**: If any record fails, the entire job terminates with `ValueError` *before* any rows are written to the target Delta table.

### 5.2 Post-Merge Quality Gate (`verify_silver_quality`, lines 298–334)
Executed when the full pipeline runs (all 12 datasets):
1. **Delta Existence**: Confirms all 12 Silver Delta tables exist.
2. **Key Uniqueness & Non-nullness**:
   - `duplicate_keys = frame.groupBy(*KEYS[dataset]).count().filter(F.col("count") > 1).count()`
   - `missing_keys = frame.filter(" OR ".join(f"{key} IS NULL" for key in KEYS[dataset])).count()`
   - If either > 0, raises `ValueError`.
3. **Clickstream Referential Integrity**:
   - Anti-joins purchase events with `slv_orders` and `slv_order_items`.
   - If missing orders or products > 0, raises `ValueError`.
4. **Great Expectations Suite (`validate_frames`, lines 286–295)**:
   - Uses `gx.get_context(mode="ephemeral")` (in-memory, requires no local disk stores).
   - Rules:
     - `slv_orders`: `ExpectColumnValuesToNotBeNull(column="customer_id")`
     - `slv_payments`: `ExpectColumnValuesToBeBetween(column="payment_value", min_value=0)`
     - `slv_clickstream_events`: `ExpectColumnValuesToBeBetween(column="event_timestamp", max_value=datetime.now(timezone.utc))`
   - Fails with `ValueError(f"Silver Great Expectations quality gate failed: {results}")` if any check fails.

### 5.3 Reliability on AWS Glue & Offline Testing
- **AWS Glue Runtime**:
  - AWS Glue 4.0 (Spark 3.3+) supports Great Expectations via `--additional-python-modules great-expectations==1.24.1`.
  - Because `mode="ephemeral"` is used, Great Expectations runs purely in Spark memory without requiring local disk access or DB connections.
- **Offline / Windows Testing**:
  - Live PySpark sessions fail on Windows outside Docker due to missing Hadoop/winutils native libraries (`java.lang.UnsupportedOperationException: getSubject is not supported` in Java 24 / Windows).
  - Therefore, full Spark end-to-end tests are properly isolated under `SILVER_TEST_DOCKER=1` in `tests/test_silver_local.py` and `tests/test_stage6_quality.py`.
  - Offline tests executed during default test runs (e.g. `python -m unittest discover tests`) MUST mock Spark / AST and test parameter parsing, entrypoint contracts, S3 path handling, and quality gate definitions without invoking JVM subprocesses.

---

## 6. Offline Unit Test Strategy

### 6.1 Test Inventory & File Structure
1. **Update `tests/test_silver_manifest.py`**:
   - Update docstring from Databricks to AWS Glue.
   - Replace Databricks-specific entrypoint test (`test_spark_python_entrypoint_without_file_global`) with AWS Glue execution contract tests.
   - Retain Delta append mode test, manifest verification test, S3 listing mock test, and corruption failure test.
2. **Create New `tests/test_silver_glue_offline.py`**:
   Dedicated offline unit test suite verifying:
   - **Glue Parameter Parsing (`getResolvedOptions`)**: Verifies required and optional parameter extraction.
   - **CLI Fallback (`argparse`)**: Verifies local development and test execution flags.
   - **Airflow `GlueJobOperator` Contract**: Verifies that `--JOB_NAME`, `--bronze-root`, `--silver-root`, `--glue-database` map 1:1 to script arguments.
   - **Absence of Databricks Dependencies**: AST/text assertion that `DATABRICKS_RUNTIME_VERSION`, `register_unity_catalog`, and Databricks SQL queries are completely absent.
   - **CDC MERGE Condition Structure**: Confirms precedence condition string matches deterministic ordering.
   - **Quality Gate Expectations Contract**: Confirms Great Expectations checks and native circuit breakers are declared properly.
   - **Quarantine Hashing Determinism**: Proves `record_id` generation is reproducible and deterministic.

---

## 7. Concrete Implementation Specifications

### 7.1 Proposed Updates to `spark_jobs/silver_transform.py`
1. **Dual-Mode Argument Parsing**:
   ```python
   def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
       argv = sys.argv[1:] if argv is None else argv
       if any(arg.startswith("--JOB_NAME") for arg in sys.argv):
           # Running in AWS Glue runtime
           from awsglue.utils import getResolvedOptions
           known_options = ["JOB_NAME", "bronze-root", "silver-root"]
           if any(arg == "--dataset" or arg.startswith("--dataset=") for arg in sys.argv):
               known_options.append("dataset")
           if any(arg == "--glue-database" or arg.startswith("--glue-database=") for arg in sys.argv):
               known_options.append("glue-database")
           resolved = getResolvedOptions(sys.argv, known_options)
           dataset_arg = resolved.get("dataset")
           datasets = [dataset_arg] if dataset_arg else None
           return argparse.Namespace(
               bronze_root=resolved["bronze-root"],
               silver_root=resolved["silver-root"],
               dataset=datasets,
               glue_database=resolved.get("glue-database", "silver"),
               job_name=resolved.get("JOB_NAME"),
           )
       # Fallback to argparse for local execution / tests
       parser = argparse.ArgumentParser(description=__doc__)
       parser.add_argument("--bronze-root", required=True, help="Bronze root (local directory path or s3:// URI)")
       parser.add_argument("--silver-root", required=True, help="Silver Delta root (local directory path or s3:// URI)")
       parser.add_argument("--dataset", action="append", choices=SILVER_NAMES, help="Process one dataset; repeat or omit for all")
       parser.add_argument("--glue-database", default="silver", help="AWS Glue database for Silver tables")
       return parser.parse_args(argv)
   ```
2. **SparkSession Decoupling**:
   ```python
   def get_spark() -> SparkSession:
       active = SparkSession.getActiveSession()
       if active is not None:
           return active
       try:
           from awsglue.context import GlueContext
           from pyspark.context import SparkContext
           return GlueContext(SparkContext.getOrCreate()).spark_session
       except ImportError:
           pass
       builder = (SparkSession.builder.master("local[2]").appName("ecommerce-silver")
                  .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                  .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                  .config("spark.sql.session.timeZone", "UTC")
                  .config("spark.ui.showConsoleProgress", "false")
                  .config("spark.sql.shuffle.partitions", "2"))
       return configure_spark_with_delta_pip(builder).getOrCreate()
   ```
3. **Elimination of Unity Catalog Registration**:
   Completely remove `register_unity_catalog()` and all `--catalog` / `--schema` Databricks arguments.

### 7.2 Proposed `tests/test_silver_glue_offline.py` Implementation Spec
The complete test file structure is detailed in `handoff.md` and covers:
1. `test_glue_argument_parsing_required_and_optional`
2. `test_cli_argument_parsing_fallback`
3. `test_airflow_glue_operator_script_args_contract`
4. `test_absence_of_databricks_references`
5. `test_cdc_merge_precedence_condition`
6. `test_quarantine_hashing_determinism`
7. `test_quality_gate_expectation_definitions`
