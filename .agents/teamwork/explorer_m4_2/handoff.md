# Offline AWS Glue Job Invocation Investigation & Test Gap Analysis (Feature 19)

## 1. Observation

Direct inspection of `spark_jobs/silver_transform.py` and existing test suites (`tests/test_silver_glue_offline.py`, `tests/test_silver_glue_adversarial.py`, `tests/test_silver_manifest.py`, `tests/test_silver_adversarial.py`, `tests/test_adversarial_m3.py`, `airflow/dags/ecommerce_cdc_daily.py`) revealed the following facts:

### 1.1 Parameter Parsing & Runtime Heuristics
- In `spark_jobs/silver_transform.py:351-359`, `GLUE_OPTION_SPEC` defines 7 options:
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
  `--execution-date` (and `--execution_date`) is entirely missing from `GLUE_OPTION_SPEC`.
- In `spark_jobs/silver_transform.py:453-471`, `parse_cli_args()` similarly declares arguments for `--JOB_NAME`, `--bronze-root`, `--silver-root`, `--dataset`, `--glue-database`, `--catalog`, and `--schema`. Neither `--execution-date` nor `--execution_date` is declared.
- In `spark_jobs/silver_transform.py:362-374`, `is_glue_runtime(argv)` checks:
  ```python
  has_glue = "awsglue" in sys.modules or "awsglue.utils" in sys.modules
  ...
  all_args = list(argv) + list(sys.argv)
  return any(a == "--JOB_NAME" or a.startswith("--JOB_NAME=") for a in all_args)
  ```
- In `spark_jobs/silver_transform.py:490-496`, `parse_args(argv, force_mode)` supports `force_mode="glue"` and `force_mode="local"`, but `force_mode` logic is untested in existing suites. Additionally, `parse_args` does not forward a `resolve_fn` argument to `parse_glue_args(argv, resolve_fn)`.

### 1.2 Path and Contract Validation
- In `spark_jobs/silver_transform.py:56-57`, `is_s3_uri(path: str)` evaluates `str(path).startswith("s3://") or str(path).startswith("s3a://")`.
- Neither `parse_glue_args()` nor `parse_cli_args()` performs S3 contract validation:
  - If a local path (e.g. `/tmp/bronze`), an empty string `""`, or a malformed URI (e.g. `s3://`, `s3:/bad`) is passed in Glue runtime, it is accepted by `parse_args()` without validation (as demonstrated in `tests/test_silver_glue_adversarial.py:270-282`).
  - Neither parser validates `--glue-database` (e.g., ensuring it is a valid database identifier conforming to `^[a-zA-Z0-9_]{1,255}$` rather than spaces or SQL injection fragments).
  - No date format validation exists for execution dates.

### 1.3 Exit Codes & CLI Execution
- In `spark_jobs/silver_transform.py:471`, `parser.parse_args(argv)` calls `parser.error()`, which raises `SystemExit(2)`.
- In `spark_jobs/silver_transform.py:426`, `fn(glue_argv, options_to_resolve)` raises `GlueArgumentError` on missing required options when resolving via AWS Glue.
- In `spark_jobs/silver_transform.py:442,448`, invalid dataset or dataset+catalog conflict in Glue mode raises `ValueError`.
- In `spark_jobs/silver_transform.py:501-537`, `main()` does not wrap `parse_args()` in a try/except, so unhandled exceptions bubble up.
- In `tests/test_silver_glue_offline.py` and `tests/test_silver_glue_adversarial.py`, assertions are made on `assertRaises(SystemExit)` and `assertRaises(ValueError)` at the `parse_args` unit level. However, **zero** tests verify subprocess execution or process return codes (`returncode == 0`, `returncode == 2` on CLI error, `returncode == 1` on unhandled exception).

### 1.4 Quality Gate Simulation & Stop Conditions in `main()`
- In `spark_jobs/silver_transform.py:276-279`, `run_dataset()` aborts with `ValueError` on negative payment values or future event timestamps before calling `merge_delta()`.
- In `spark_jobs/silver_transform.py:314-344`, `verify_silver_quality()` aborts with `ValueError` on:
  - Missing Delta table
  - Duplicate or NULL primary keys
  - Referential integrity join failure (Clickstream purchases without matching order or product)
  - Great Expectations checkpoint validation failure
- In `spark_jobs/silver_transform.py:521-534`, `main()` executes:
  ```python
  datasets = args.dataset or list(SILVER_NAMES.keys())
  for dataset in datasets:
      valid, quarantined = run_dataset(spark, args.bronze_root, args.silver_root, dataset)
  if not args.dataset:
      verify_silver_quality(spark, args.silver_root)
  if glue_job is not None:
      glue_job.commit()
  ```
- In `tests/test_silver_glue_adversarial.py:504-543`, `test_main_fails_fast_on_dataset_error_without_glue_commit` tests `main()` when `run_dataset()` raises a `ValueError`.
- **Gaps in existing tests**:
  - No test simulates `verify_silver_quality()` failing inside `main()` (e.g. GX checkpoint failure or key dups) to assert that `glue_job.commit()` is NOT called.
  - No test verifies that specifying `--dataset orders` deliberately bypasses `verify_silver_quality()`, while omitting `--dataset` runs `verify_silver_quality()` and commits.
  - No test validates end-to-end quality gate stop conditions against synthetic bad data in an offline Glue invocation harness.

---

## 2. Logic Chain

1. **Contract Incompleteness for Airflow / Orchestration (Observation 1.1 & 1.2)**:
   Airflow DAGs and batch orchestration systems frequently pass `--execution-date` (or `--execution_date`) along with `--bronze-root`, `--silver-root`, and `--glue-database`. Because `--execution-date` is missing from `GLUE_OPTION_SPEC` and `parse_cli_args()`, any invocation supplying an execution date either fails with `SystemExit(2)` in CLI mode or silently drops the parameter in Glue mode. Adding `--execution-date` / `--execution_date` with ISO date validation closes this orchestration gap.

2. **Validation Gap on Cloud S3 Path Contracts (Observation 1.2)**:
   In AWS Glue runtime, processing expects direct S3 Delta I/O (`s3://<bucket>/...`). If an operator or test mistakenly passes a relative path, local path, or malformed URI to a Glue job, the job should fail fast during argument resolution rather than failing deep during Spark manifest reads. Adding contract checks for S3 URIs in Glue mode and identifier checks for `--glue-database` strengthens job safety.

3. **Verification Gap on Process Exit Codes (Observation 1.3)**:
   The AWS Glue runtime monitors the Python script execution return code. An exit code of 0 marks `SUCCEEDED`, while any non-zero exit code marks `FAILED`. Unit tests that only catch `SystemExit` inside `parse_args()` do not verify that running `python -m spark_jobs.silver_transform` or calling `main()` under invalid arguments, missing required parameters, or quality gate aborts produces expected non-zero process exit codes. Testing CLI entrypoint invocation completes Feature 19 requirements.

4. **Incomplete Coverage of Quality Gate Stop Conditions in Job Lifecycle (Observation 1.4)**:
   `verify_silver_quality()` is the definitive quality gate before committing the Glue job. If Great Expectations expectations fail or referential integrity is violated, `glue_job.commit()` MUST NOT be called. While `run_dataset` failure was tested, the post-merge gate (`verify_silver_quality`) failure was never tested in the `main()` lifecycle. Demonstrating that GX failures and referential join breaks prevent `glue_job.commit()` provides full assurance of pipeline integrity.

---

## 3. Caveats

- PySpark and Delta Lake runtime operations in full integration mode require a Java/Spark environment. Offline unit tests must use lightweight mocking (or ephemeral Spark context where available) to test `main()` lifecycle without requiring a live Spark cluster or live AWS connection.
- AWS Glue's native `awsglue.utils.getResolvedOptions` is written in C-Python in AWS Glue containers. Offline testing relies on mock implementations simulating `getResolvedOptions` behavior, including `GlueArgumentError` on missing required options.
- The repository uses `uv run python -m unittest` rather than pytest (pytest is not installed in the workspace .venv). All recommended tests must run cleanly under `unittest`.

---

## 4. Conclusion & Recommendations for Worker M4

To achieve complete offline test coverage for Feature 19 (Offline Glue Job Invocation Tests), worker_m4 should execute the following bounded enhancements:

### Recommendation A: Parameter Specification & Validation Enhancements (`spark_jobs/silver_transform.py`)
1. **Support `--execution-date` / `--execution_date`**:
   - Add `("execution-date", False, None)` to `GLUE_OPTION_SPEC`.
   - Add `--execution-date` / `--execution_date` argument to `parse_cli_args()`.
   - Validate execution date format (valid ISO date `YYYY-MM-DD` or timestamp `YYYY-MM-DDTHH:MM:SSZ`), raising `ValueError` (Glue mode) or calling `parser.error` (CLI mode) on invalid formats.
2. **S3 Path & Database Contract Validation**:
   - In `parse_args` (or `parse_glue_args` when in Glue runtime), validate that `bronze_root` and `silver_root` are valid S3 URIs (`s3://` or `s3a://` with non-empty bucket name).
   - Validate that `glue_database` is a valid identifier (alphanumeric and underscores, no illegal chars or spaces).
3. **Parameter Resolver Injection**:
   - Update `parse_args()` signature to `parse_args(argv=None, force_mode=None, resolve_fn=None)` and forward `resolve_fn` to `parse_glue_args()`.

### Recommendation B: Dedicated Offline Glue Invocation Test Suite (`tests/test_silver_glue_offline.py` or new `tests/test_silver_glue_invocation.py`)
Add test cases covering the 4 gap areas:
1. **CLI vs Glue Parameter Parsing Gaps**:
   - Test `parse_args(argv=None)` reading from `sys.argv` in both Glue and CLI modes.
   - Test fallback to CLI when `awsglue` is present in `sys.modules` but `--JOB_NAME` is absent.
   - Test `force_mode="glue"` and `force_mode="local"` overrides.
   - Test `force_mode="glue"` raising `RuntimeError` when `awsglue.utils` is unavailable.
2. **S3 Path & Execution Date Contract Tests**:
   - Test valid S3 path acceptance (`s3://bucket/bronze`, `s3a://bucket/silver`).
   - Test rejection of non-S3 paths (e.g. `/tmp/local`, `file:///tmp`, `s3://`) in Glue mode.
   - Test valid `--execution-date` (`2026-11-07`, `2026-11-07T00:00:00Z`).
   - Test malformed `--execution-date` (`bad-date`, `2026/13/45`) raises error.
   - Test valid vs invalid `--glue-database` names.
3. **Exit Code & Entrypoint Tests**:
   - Test subprocess execution of `python -m spark_jobs.silver_transform --help` -> returncode 0.
   - Test subprocess execution with missing required arguments -> returncode 2 (`SystemExit: 2`).
   - Test subprocess execution with unknown arguments -> returncode 2.
   - Test execution under Glue mock when required args are missing -> non-zero exit / `GlueArgumentError`.
4. **Quality Gate Failure Simulation & Stop Conditions in `main()`**:
   - Test `main()` with full dataset run: `verify_silver_quality` succeeds -> `glue_job.commit()` called.
   - Test `main()` when `verify_silver_quality` raises `ValueError` (GX failure or duplicate keys) -> `glue_job.commit()` NOT called.
   - Test `main()` with `--dataset orders`: `run_dataset` succeeds, `verify_silver_quality` skipped, `glue_job.commit()` called.
   - Test `main()` with `--dataset order_payments` where `run_dataset` fails on negative payment -> `glue_job.commit()` NOT called.

---

## 5. Verification Method

To verify these findings and validate future worker implementations independently:

1. **Run Current Test Suites**:
   ```bash
   uv run python -m unittest tests/test_silver_glue_offline.py tests/test_silver_glue_adversarial.py tests/test_silver_manifest.py tests/test_silver_adversarial.py
   ```
2. **Run Full Test Suite Offline**:
   ```bash
   uv run python -m unittest discover tests
   ```
3. **Verify Absence of Pytest Dependency**:
   Ensure all tests execute with standard library `unittest` under `uv run python -m unittest`.
4. **Invalidation Conditions**:
   - Any test requiring network calls or live AWS credentials invalidates offline compliance.
   - Any change altering existing valid CLI options or breaking `local_spark()` backward compatibility invalidates the interface contract.
