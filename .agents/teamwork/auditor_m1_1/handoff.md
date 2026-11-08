# Forensic Audit Report: Milestone M1 (AWS Glue Spark Silver Job Migration)

**Auditor**: `auditor_m1_1`  
**Milestone**: M1 (AWS Glue Spark Silver Job Migration)  
**Profile**: General Project (Forensic Integrity)  
**Integrity Mode**: Benchmark Mode (from `ORIGINAL_REQUEST.md`)  
**Verdict**: **CLEAN**  
**Date**: 2026-11-07  

---

## 1. Observation

### 1.1 Scope of Audited Files
The following work products were audited:
- `spark_jobs/silver_transform.py` (541 lines)
- `tests/test_silver_manifest.py` (79 lines)
- `tests/test_silver_glue_offline.py` (300 lines)

### 1.2 Static Analysis & Code Integrity
1. **Dual-Mode Parameter Resolution**:
   - `spark_jobs/silver_transform.py` lines 351–359 define the Glue option specification:
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
   - Lines 362–375 define `is_glue_runtime(argv: list[str]) -> bool`, inspecting whether `awsglue` modules are present and checking `--JOB_NAME` in `argv`/`sys.argv`.
   - Lines 377–450 define `parse_glue_args()`: it filters options passed to AWS Glue's `getResolvedOptions` so optional parameters (`--dataset`, `--glue-database`, `--catalog`, `--schema`) are only requested if actually passed, preventing AWS Glue `GlueArgumentError` exceptions.
   - Lines 453–487 define `parse_cli_args()` using standard library `argparse.ArgumentParser`.
   - Lines 490–496 define `parse_args()`, providing a unified resolver dispatching to Glue or CLI mode.
   - Neither implementation returns hardcoded constants or dummy mocks.

2. **SparkSession Initialization**:
   - Lines 22–50 define `get_spark()`:
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
   - Lines 52–53 maintain `local_spark() -> SparkSession: return get_spark()` for backward compatibility across the repository.

3. **Core Transformation & Quality Validation Logic**:
   - `normalize()` (lines 163–222): Implements timestamp standardization, string trimming, field casing, schema key validations from `KEYS`, row-level quarantine marking, row hashing (`sha2(..., 256)`), and window-based deduplication (`row_number() over (partitionBy KEYS order by updated_at desc, ingested_at desc, row_hash desc) == 1`).
   - `quarantine_orphan_clickstream()` (lines 225–245): Performs left joins against `slv_customers`, `slv_orders`, and `slv_order_items` Delta tables to quarantine orphaned clickstream events.
   - `merge_delta()` (lines 248–268): Uses Delta Lake `DeltaTable.forPath(spark, path_str).merge()`.
     CDC MERGE precedence condition (lines 263–266):
     ```python
     newer = ("source.row_hash <> target.row_hash AND (source.updated_at > target.updated_at OR "
              "(source.updated_at = target.updated_at AND "
              "(source.ingested_at > target.ingested_at OR "
              "(source.ingested_at = target.ingested_at AND source.row_hash > target.row_hash))))")
     ```
   - Circuit breakers in `run_dataset()` (lines 276–279):
     - `order_payments` with `payment_value < 0` raises `ValueError("Quality gate failed: slv_payments has negative payment values")`.
     - `clickstream` with `event_timestamp > F.current_timestamp()` raises `ValueError("Quality gate failed: slv_clickstream_events has future event timestamps")`.
   - Ephemeral Great Expectations in `validate_frames()` (lines 302–312): Validates non-null `customer_id`, non-negative `payment_value`, and non-future `event_timestamp`.

### 1.3 Databricks API & Unity Catalog Decoupling
- Grep search for `DATABRICKS_RUNTIME_VERSION`: Zero occurrences in `spark_jobs/`. Only present in `tests/test_silver_manifest.py:29` and `tests/test_silver_glue_offline.py:223` asserting its absence.
- Grep search for `register_unity_catalog`: Zero occurrences in `spark_jobs/`. Only present in `tests/test_silver_glue_offline.py:224` asserting its absence.
- Grep search for `USING DELTA`: Zero occurrences in `spark_jobs/`.
- Grep search for `CREATE TABLE`: Zero occurrences in `spark_jobs/`.
- Grep search for `CREATE SCHEMA`: Zero occurrences in `spark_jobs/`.
- S3 Delta table I/O: Delta tables are written directly to S3 paths (`s3://...`) via Delta Lake API without Databricks REST API or Unity Catalog endpoints.

### 1.4 Test Suite & Assertion Integrity
- `tests/test_silver_manifest.py`:
  - `test_aws_glue_entrypoint_without_file_global`: Compiles and executes `spark_jobs/silver_transform.py` in an `aws_glue_job` namespace, asserting presence of `KEYS` and absence of `DATABRICKS_RUNTIME_VERSION`.
- `tests/test_silver_glue_offline.py`:
  - Contains 14 distinct offline unit tests:
    - `test_glue_runtime_argument_parsing_with_resolved_options`
    - `test_glue_runtime_argument_parsing_with_optional_dataset`
    - `test_glue_runtime_comma_separated_datasets`
    - `test_glue_runtime_invalid_dataset_raises_error`
    - `test_glue_runtime_dataset_and_catalog_conflict`
    - `test_local_cli_argument_parsing_fallback`
    - `test_local_cli_missing_required_args_exits`
    - `test_airflow_glue_operator_contract_compatibility`
    - `test_absence_of_databricks_references_in_silver_transform`
    - `test_cdc_merge_precedence_sql_condition`
    - `test_quarantine_delta_merge_mode`
    - `test_quarantine_record_id_deterministic_hash`
    - `test_validate_frames_expectations_structure`
    - `test_local_spark_alias`
  - Every test contains concrete assertions checking return values, raised exceptions, or call arguments on mocks. Zero tautological or always-true checks were found.

### 1.5 Independent Test Execution Results
All test commands were executed directly by this auditor:
1. `python -m compileall spark_jobs tests`:
   ```
   Listing 'spark_jobs'...
   Listing 'tests'...
   ```
   Exit code: 0.

2. `python -m unittest tests/test_silver_manifest.py`:
   ```
   Ran 6 tests in 0.018s
   OK
   ```
   Exit code: 0.

3. `python -m unittest tests/test_silver_glue_offline.py`:
   ```
   Ran 14 tests in 0.020s
   OK
   ```
   Exit code: 0.

4. `python -m unittest discover -s tests -p "test_*.py"`:
   ```
   Ran 48 tests in 0.094s
   OK (skipped=13)
   ```
   Exit code: 0. (All 13 skipped tests are docker/live integration tests conditioned on environment variables such as `SILVER_TEST_DOCKER` or `SILVER_PG_E2E`).

---

## 2. Logic Chain

1. **Static Analysis & Absence of Facades**:
   - Observations in Section 1.2 demonstrate that parameter parsing and SparkSession instantiation perform real runtime checks, introspection of arguments, and initialization of actual objects (`GlueContext` or `configure_spark_with_delta_pip`).
   - No mock return constants, dummy stubs, or bypasses exist in `spark_jobs/silver_transform.py`.

2. **Decoupling from Databricks**:
   - Observations in Section 1.3 prove that all Databricks environment variable checks (`DATABRICKS_RUNTIME_VERSION`), Unity Catalog table registration SQL statements (`CREATE TABLE ... USING DELTA LOCATION`), and schema creation statements were completely deleted.
   - S3 writes utilize Delta Lake directly against S3 URIs without intermediary Databricks services.

3. **Test Suite Integrity**:
   - Observations in Section 1.4 confirm that test cases in `tests/test_silver_glue_offline.py` and `tests/test_silver_manifest.py` test genuine operational behavior, contract validation, and error boundaries.
   - No test asserts a tautology (e.g. `self.assertTrue(True)`).

4. **Benchmark Mode Compliance**:
   - No third-party packages were added to implement core logic. All code relies on standard library, existing project dependencies, and standard AWS Glue APIs (`awsglue.context.GlueContext`, `awsglue.job.Job`, `awsglue.utils.getResolvedOptions`).
   - No external scripts or delegating wrappers were introduced.

5. **Empirical Independent Verification**:
   - Observations in Section 1.5 show all 4 test suites compiled and executed cleanly under independent auditor execution with exit code 0.

---

## 3. Caveats

1. **Live AWS Glue Runtime**:
   - Tests were run in an offline environment using mocks for `awsglue` modules, per the constraints of R4 ("without requiring live AWS cloud credentials").
   - Delta Lake support on AWS Glue in production requires passing `--datalake-formats delta` to the AWS Glue job configuration, which is handled at the Airflow operator / Glue job definition level in subsequent milestones (M3).

---

## 4. Conclusion & Forensic Verdict

### Forensic Audit Summary Table

| Forensic Check | Requirement | Result |
|---|---|:---:|
| **Hardcoded Output Detection** | No hardcoded test responses or return constants | **PASS** |
| **Facade Detection** | Genuine business logic and Glue/local adapters | **PASS** |
| **Pre-populated Artifacts** | No falsified or predated test logs in PR | **PASS** |
| **Databricks Decoupling** | Zero Databricks runtime or Unity Catalog dependencies | **PASS** |
| **S3 Delta I/O** | Direct S3 Delta table writes without Databricks APIs | **PASS** |
| **Parameter Resolution** | Dual-mode Glue `getResolvedOptions` and CLI `argparse` | **PASS** |
| **Transformation Integrity** | Preservation of all normalization, deduplication & quarantine rules | **PASS** |
| **CDC MERGE Invariant** | Deterministic total-ordering precedence condition preserved | **PASS** |
| **Circuit Breakers & Quality Gates** | In-flight value checks and Great Expectations preserved | **PASS** |
| **Test Suite Authenticity** | Non-tautological, contract-enforcing unit tests | **PASS** |
| **Independent Build & Test** | `compileall` and all unittest commands exit code 0 | **PASS** |

### Verdict
**CLEAN**

Milestone M1 satisfies all forensic integrity criteria. The work product is authentic, correct, and fully decoupled from Databricks.

---

## 5. Verification Method

To independently reproduce this forensic audit:

1. **Check for Databricks / Unity Catalog references**:
   ```powershell
   git grep -i "DATABRICKS_RUNTIME_VERSION" spark_jobs/
   git grep -i "register_unity_catalog" spark_jobs/
   git grep -i "USING DELTA" spark_jobs/
   ```
   Expected output: Empty (zero occurrences).

2. **Run compilation check**:
   ```powershell
   .venv\Scripts\python -m compileall spark_jobs tests
   ```
   Expected output: Exit code 0, 0 syntax errors.

3. **Run Silver manifest tests**:
   ```powershell
   .venv\Scripts\python -m unittest tests/test_silver_manifest.py
   ```
   Expected output: 6 tests pass.

4. **Run Silver Glue offline tests**:
   ```powershell
   .venv\Scripts\python -m unittest tests/test_silver_glue_offline.py
   ```
   Expected output: 14 tests pass.

5. **Run full offline repository discovery**:
   ```powershell
   .venv\Scripts\python -m unittest discover -s tests -p "test_*.py"
   ```
   Expected output: 48 tests ran (35 pass, 13 skipped, 0 fail).

6. **Invalidation Conditions**:
   - Any recurrence of `DATABRICKS_RUNTIME_VERSION` or `register_unity_catalog` in `spark_jobs/`.
   - Any hardcoded return values in `parse_args` or `get_spark`.
   - Failure of any unit test in `tests/test_silver_glue_offline.py` or `tests/test_silver_manifest.py`.
