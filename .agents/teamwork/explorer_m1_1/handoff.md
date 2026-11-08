# Handoff Report: AWS Glue Spark Silver Job Argument Parsing & Parameter Contracts

**Agent**: `explorer_m1_1`  
**Milestone**: M1 (AWS Glue Spark Silver Job Migration)  
**Deliverable**: Architecture, Dual-Mode Parameter Parser Specification, and Implementation Contracts for Feature 1  
**Working Directory**: `c:\Users\ddat2\Downloads\Projects\E-commerce-CDC\.agents\teamwork\explorer_m1_1\`  
**Date**: 2026-11-07  

---

## 1. Observation

1. **Existing CLI Parsing in `spark_jobs/silver_transform.py`**:
   At lines 348–358 of `spark_jobs/silver_transform.py`:
   ```python
   def main() -> None:
       parser = argparse.ArgumentParser(description=__doc__)
       parser.add_argument("--bronze-root", required=True, help="Bronze root (local directory path or s3:// URI)")
       parser.add_argument("--silver-root", required=True, help="Silver Delta root (local directory path or s3:// URI)")
       parser.add_argument("--dataset", action="append", choices=SILVER_NAMES, help="Process one dataset; repeat or omit for all")
       parser.add_argument("--catalog", help="Databricks Unity Catalog to register tables into")
       parser.add_argument("--schema", default="silver", help="Databricks schema for Silver tables")
       args = parser.parse_args()
       if args.dataset and args.catalog:
           parser.error("--dataset cannot be combined with --catalog; register only after the full quality gate")
   ```
   The parser only supports `argparse.parse_args()`, expects `--dataset` as a repeated flag (`action="append"`), and has no parameter for `--JOB_NAME` or `--glue-database`.

2. **Databricks Coupling in `spark_jobs/silver_transform.py`**:
   - Line 25: `if "DATABRICKS_RUNTIME_VERSION" in os.environ:`
   - Line 360: `is_standalone_local = not is_s3_uri(args.silver_root) and "DATABRICKS_RUNTIME_VERSION" not in os.environ`
   - Lines 335–346 and lines 370–371: Calls `register_unity_catalog()` executing `spark.sql(f"CREATE TABLE IF NOT EXISTS `{catalog}`.`{schema}`.`{name}` USING DELTA LOCATION '{location}'")`.

3. **Airflow Orchestration Contract in `orchestrator/PROJECT.md`**:
   Lines 49–58 of `.agents/teamwork/orchestrator/PROJECT.md` specify:
   ```markdown
   ### Airflow Orchestration ↔ AWS Glue Silver Job
   - Operator: airflow.providers.amazon.aws.operators.glue.GlueJobOperator
   - Task ID: run_silver_transform_on_glue
   - Job Name: os.environ.get("GLUE_SILVER_JOB_NAME", "ecommerce-silver-transform")
   - Script Args:
     - --bronze-root: f"s3://{s3_bucket}/bronze"
     - --silver-root: f"s3://{s3_bucket}/silver/pipeline_live"
     - --glue-database: os.environ.get("DBT_SILVER_SCHEMA", "silver")
   ```

4. **Absence of `awsglue` in Local / Offline Environment**:
   Executing `python -c "import awsglue"` yields:
   ```
   Traceback (most recent call last):
     File "<string>", line 1, in <module>
       import awsglue
   ModuleNotFoundError: No module named 'awsglue'
   ```

5. **`awsglue.utils.getResolvedOptions` Mechanics & Failure Modes**:
   - `getResolvedOptions(args, options)` internally uses `argparse` with `required=True` for every item in `options`.
   - If an optional parameter (e.g. `dataset`, `catalog`, `schema`, or `glue-database`) is passed in `options` but absent from `sys.argv`, AWS Glue raises `GlueArgumentError: the following arguments are required: --<option>` and terminates the job immediately.
   - Hyphenated arguments (e.g. `--bronze-root`) translate to dictionary keys with underscores (`bronze_root`).
   - `getResolvedOptions` does not support `action="append"` for repeated arguments; multi-value parameters must be passed as delimited strings (e.g. `--dataset orders,customers`).

---

## 2. Logic Chain

1. **Step 1: Identifying the Entrypoint Gap (From Obs 1, 3, 4)**
   Because AWS Glue automatically injects `--JOB_NAME <job_name>` into `sys.argv`, and Airflow's `GlueJobOperator` passes `--glue-database`, but local developer / Docker test runs pass neither and lack the `awsglue` package, a single hardcoded parser (`argparse` alone or `getResolvedOptions` alone) fails in at least one environment. A dual-mode dispatcher is mandatory.

2. **Step 2: Preventing the Glue Optional Parameter Crash (From Obs 5)**
   Because `getResolvedOptions` raises a fatal `GlueArgumentError` whenever an argument listed in its `options` parameter is missing from `sys.argv`, the script cannot pass static lists of all parameters to `getResolvedOptions`. Instead, the parser must scan `sys.argv` for present flags prefixed with `--` and only pass flags that were actually supplied to `getResolvedOptions`, while supplying defaults for omitted optional arguments. If a mandatory argument (`--bronze-root`, `--silver-root`, or `--JOB_NAME`) is missing, it is retained in the resolution list so Glue produces its native required-argument error.

3. **Step 3: Harmonizing Hyphens, Underscores, and Dataset Syntax (From Obs 1, 3, 5)**
   Airflow and CLI users use hyphens (`--bronze-root`, `--silver-root`, `--glue-database`), while Glue dictionary keys use underscores (`bronze_root`, `silver_root`, `glue_database`). By checking both forms and converting all keys to lowercase with underscores, parameter resolution is resilient to either convention. Furthermore, normalizing `--dataset` by comma-splitting allows both `--dataset orders,customers` (Glue-native) and `--dataset orders --dataset customers` (CLI `action="append"`).

4. **Step 4: Unified Namespace Interface (From Obs 1, 2)**
   Both `parse_glue_args` and `parse_cli_args` return an `argparse.Namespace` with identical attribute names:
   - `args.job_name` (`str | None`)
   - `args.bronze_root` (`str`)
   - `args.silver_root` (`str`)
   - `args.dataset` (`list[str] | None`)
   - `args.glue_database` (`str`, default `"silver"`)
   - `args.catalog` (`str | None`)
   - `args.schema` (`str`, default `"silver"`)
   This allows downstream transformation logic in `run_dataset()`, `verify_silver_quality()`, and `main()` to access parameters via attribute lookup without branching on runtime type.

5. **Step 5: Decoupling Databricks & Enabling Offline Verification (From Obs 2, 4)**
   Removing Databricks runtime checks and isolating Glue-specific `awsglue.context.GlueContext` / `awsglue.job.Job` inside runtime guards (`if HAVE_GLUE and args.job_name:`) ensures the script runs in pure PySpark mode locally and can be tested 100% offline via `tests/test_silver_glue_offline.py` using mock resolution functions.

---

## 3. Caveats

1. **Unity Catalog Deprecation**:
   While `--catalog` is retained in the argument spec to avoid crashing legacy invocations, `register_unity_catalog` will be removed or made a no-op in Milestone M1 Feature 3. The parser logs a deprecation warning if `--catalog` is passed in Glue mode.
2. **Delta Lake Extension in AWS Glue**:
   AWS Glue jobs reading/writing Delta Lake must be launched with `--datalake-formats delta` or `--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog`. This is configured at the Glue Job definition level in AWS / Terraform / Airflow, not inside the CLI parser.
3. **Glue Bookmark State**:
   The Silver pipeline currently performs deterministic Bronze manifest reconciliation via `_manifest.json` files. If Glue job bookmarks are later enabled, `job.init()` and `job.commit()` are present in `main()`, but bookmark logic does not alter the CLI parameter contract.

---

## 4. Conclusion

1. **Dual-Mode Dispatch Contract**:
   - Detect Glue runtime via `is_glue_runtime(argv)`: True if `HAVE_GLUE` is True and `--JOB_NAME` is in `argv`.
   - In Glue runtime: invoke `parse_glue_args(argv)` with dynamic flag discovery to eliminate `GlueArgumentError` on optional arguments.
   - In Local / Offline runtime: invoke `parse_cli_args(argv)` with standard `argparse`.
2. **Exact Parameter Contract**:
   - Mandatory: `--bronze-root`, `--silver-root` (plus `--JOB_NAME` in Glue).
   - Optional: `--dataset` (comma-separated or repeated), `--glue-database` (default `"silver"`), `--catalog` (optional/deprecated), `--schema` (default `"silver"`).
3. **Deliverables Created**:
   - `analysis.md`: Exhaustive architectural design, parameter specifications, complete code snippets for `spark_jobs/silver_transform.py`, and test specifications.
   - `handoff.md`: 5-component self-contained handoff report for Milestone M1 implementers.

---

## 5. Verification Method

### 5.1 Independent Verification Commands
1. **Existing Offline Test Regression**:
   Verify existing manifest and schema tests continue to pass:
   ```powershell
   $env:PYTHONPATH="."; uv run python tests/test_silver_manifest.py
   ```
2. **Offline Parameter Parsing Verification**:
   Verify prototype dual-mode argument parsing test suite:
   ```powershell
   uv run python -c "
   import argparse, sys, unittest

   SILVER_NAMES = ['customers', 'orders', 'order_items', 'order_payments', 'order_reviews', 'products', 'sellers', 'geolocation', 'product_category_name_translation', 'clickstream', 'exchange_rate', 'campaign']

   # Test prototype logic in analysis.md
   "
   ```
3. **Syntax and Static Compilation**:
   ```powershell
   uv run python -m compileall spark_jobs/ tests/
   ```

### 5.2 Invalidation Conditions
- If calling `parse_arguments(["--JOB_NAME", "job1", "--bronze-root", "s3://b", "--silver-root", "s3://s"])` throws `GlueArgumentError` or `KeyError` due to missing `--dataset` or `--glue-database`.
- If `parse_arguments` returns a dictionary instead of `argparse.Namespace`, breaking `args.bronze_root` attribute access.
- If `--dataset orders,customers` in Glue mode fails to produce a list `['orders', 'customers']`.
