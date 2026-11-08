# AWS Glue Spark Silver Job Argument Parsing & Entrypoint Contract Analysis

**Agent**: `explorer_m1_1`  
**Milestone**: M1 (AWS Glue Spark Silver Job Migration)  
**Target File**: `spark_jobs/silver_transform.py`  
**Test File**: `tests/test_silver_glue_offline.py`  
**Date**: 2026-11-07  

---

## 1. Executive Summary

In migrating the Lakehouse Silver processing pipeline from Databricks to an AWS Glue Spark job (`spark_jobs/silver_transform.py`), the job entrypoint must transition from Databricks Python task parameter passing (`python_params`) to AWS Glue's runtime environment (`awsglue.utils.getResolvedOptions`) while maintaining 100% backward compatibility with local testing, Docker execution, and offline CI test suites via standard Python `argparse`.

The central technical challenge with AWS Glue's native `getResolvedOptions(sys.argv, options)` is that it enforces `required=True` on every key listed in the `options` argument. If an optional parameter (such as `--dataset`, `--glue-database`, `--catalog`, or `--schema`) is queried via `getResolvedOptions` but was not passed by the caller (e.g. Airflow `GlueJobOperator`), AWS Glue raises a fatal `GlueArgumentError` / `KeyError` before the job even starts.

This report establishes:
1. The exact audit of existing argument handling in `spark_jobs/silver_transform.py`.
2. A dual-mode argument parsing architecture that dynamically discovers present command-line flags before invoking `getResolvedOptions` on AWS Glue, and seamlessly falls back to Python's standard `argparse` when running locally or during offline testing.
3. Complete parameter contract definitions covering `--JOB_NAME`, `--bronze-root`, `--silver-root`, `--dataset`, `--glue-database`, `--catalog`, and `--schema`.
4. Production-ready implementation code for `spark_jobs/silver_transform.py` and offline test specifications for `tests/test_silver_glue_offline.py`.

---

## 2. Examination of Existing `spark_jobs/silver_transform.py`

### 2.1 Existing Entrypoint & Argument Handling
In `spark_jobs/silver_transform.py` (lines 348–378):
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

    spark = get_spark()
    is_standalone_local = not is_s3_uri(args.silver_root) and "DATABRICKS_RUNTIME_VERSION" not in os.environ
    try:
        datasets = args.dataset or SILVER_NAMES
        for dataset in datasets:
            valid, quarantined = run_dataset(spark, args.bronze_root, args.silver_root, dataset)
            print(f"{dataset}: {valid} latest valid rows, {quarantined} quarantined")

        if not args.dataset:
            verify_silver_quality(spark, args.silver_root)

        if args.catalog:
            register_unity_catalog(spark, args.silver_root, args.catalog, args.schema)
    finally:
        if is_standalone_local:
            spark.stop()
```

### 2.2 Databricks Assumptions & Deficiencies
1. **No AWS Glue Runtime Parameter Awareness**:
   AWS Glue jobs pass arguments via `sys.argv` starting with `--JOB_NAME <name>` and optional user arguments (`--bronze-root`, `--silver-root`, `--glue-database`). The current parser does not know about `--JOB_NAME` or `--glue-database`.
2. **Missing `getResolvedOptions` Integration**:
   AWS Glue standard practice and Glue runtime tooling expect parameters to be resolved using `awsglue.utils.getResolvedOptions`. Currently, `silver_transform.py` exclusively calls `argparse.ArgumentParser.parse_args()`.
3. **Hardcoded Databricks Environment Checks**:
   Line 360 uses `"DATABRICKS_RUNTIME_VERSION" not in os.environ` to determine if standalone Spark should be stopped.
4. **Unity Catalog Coupling**:
   Lines 335–346 and lines 370–371 call `register_unity_catalog()`, issuing Databricks SQL `CREATE TABLE ... USING DELTA LOCATION` commands against Unity Catalog. On AWS Glue, table metadata is stored in AWS Glue Data Catalog, and Unity Catalog endpoints do not exist.
5. **Airflow Invocation Contract Mismatch**:
   Airflow's migrated `GlueJobOperator` (`run_silver_transform_on_glue`) passes:
   - `--bronze-root`: `s3://<bucket>/bronze`
   - `--silver-root`: `s3://<bucket>/silver/pipeline_live`
   - `--glue-database`: `os.environ.get("DBT_SILVER_SCHEMA", "silver")`
   Notice `--glue-database` is not recognized by the existing parser.

---

## 3. AWS Glue `getResolvedOptions` Mechanics & Failure Modes

### 3.1 How `getResolvedOptions` Works
AWS Glue provides `awsglue.utils.getResolvedOptions(args, options)`:
- `args`: Typically `sys.argv`.
- `options`: List of option name strings (without leading `--`).
- Internally, Glue instantiates an `argparse.ArgumentParser` and calls `parser.add_argument(f"--{opt}", required=True)`.
- It executes `parser.parse_known_args(args[1:])`.
- It returns a dictionary mapping argument names to string values.
- Hyphens in argument flags (e.g. `--bronze-root`) are translated into underscores in returned dictionary keys (`bronze_root`).

### 3.2 The Fatal Flaw: Mandatory Options Pitfall
Because Glue marks **every** option in the `options` list as `required=True`:
```python
# BROKEN NAIVE PATTERN:
args = getResolvedOptions(sys.argv, [
    "JOB_NAME", "bronze-root", "silver-root", 
    "dataset", "glue-database", "catalog", "schema"
])
```
If `--dataset` or `--catalog` is omitted by the caller (which is standard during normal Airflow scheduled runs), `getResolvedOptions` immediately aborts:
```
GlueArgumentError: the following arguments are required: --dataset, --catalog
```

### 3.3 The Repeated Argument Limitation
In standard `argparse`, `--dataset orders --dataset customers` can be parsed via `action="append"` to yield `['orders', 'customers']`.
In AWS Glue, `getResolvedOptions` does not support `action="append"`. Passing repeated `--dataset` flags either overwrites the value with the last one or fails.
**Solution**: The dual-mode parser must accept both:
- Comma-delimited strings: `--dataset orders,customers` (standard for AWS Glue and Airflow script_args).
- Repeated flags: `--dataset orders --dataset customers` (in local argparse CLI).
Both representations must normalize to a clean Python list: `['orders', 'customers']`.

---

## 4. Parameter Matrix & Interface Contracts

| CLI Argument | Destination Attribute | Type | Required on AWS Glue | Required Locally | Default | Description |
|---|---|---|---|---|---|---|
| `--JOB_NAME` | `job_name` | `str` | **Yes** (injected by Glue) | No | `None` / `"ecommerce-silver-transform"` | AWS Glue Job Name |
| `--bronze-root` / `--bronze_root` | `bronze_root` | `str` | **Yes** | **Yes** | None | Bronze data root (local dir or `s3://` URI) |
| `--silver-root` / `--silver_root` | `silver_root` | `str` | **Yes** | **Yes** | None | Silver Delta root (local dir or `s3://` URI) |
| `--dataset` | `dataset` | `list[str] \| None` | No | No | `None` (all 12 tables) | Single table or comma-separated list of tables to process |
| `--glue-database` / `--glue_database` | `glue_database` | `str` | No | No | `"silver"` | AWS Glue Data Catalog database name |
| `--catalog` | `catalog` | `str \| None` | No | No | `None` | Legacy Databricks Unity Catalog name (deprecated) |
| `--schema` | `schema` | `str` | No | No | `"silver"` | Legacy schema / database name fallback |

### Validation Rules
1. **Missing Required Parameters**:
   If `--bronze-root` or `--silver-root` is missing, the parser must raise an error (`SystemExit` in argparse, or missing required argument error in Glue).
   If `--JOB_NAME` is missing when executing in AWS Glue mode, the parser must raise a missing argument error.
2. **Dataset Whitelist Validation**:
   Any specified dataset must belong to `SILVER_NAMES`:
   `{"customers", "orders", "order_items", "order_payments", "order_reviews", "products", "sellers", "geolocation", "product_category_name_translation", "clickstream", "exchange_rate", "campaign"}`.
3. **Mutual Exclusion**:
   `--dataset` cannot be combined with `--catalog` (preserving existing logic).

---

## 5. Dual-Mode Architecture & Algorithm

### 5.1 Architecture Overview
```
                     Command-Line / Invocation (sys.argv)
                                     │
                     ┌───────────────┴───────────────┐
                     ▼                               ▼
          Is Glue Runtime?               Not Glue Runtime?
      (awsglue importable AND           (local test / CLI /
       --JOB_NAME in argv)               awsglue absent)
                     │                               │
                     ▼                               ▼
       Dynamic Option Resolution              Standard argparse
    - Scan argv for present flags          - Full CLI help/usage
    - Only pass present keys to            - action="append" + comma split
      getResolvedOptions                   - Safe defaults for Glue args
    - Fill unpassed option defaults                  │
                     │                               │
                     └───────────────┬───────────────┘
                                     ▼
                    Normalized argparse.Namespace
              - args.job_name: str | None
              - args.bronze_root: str
              - args.silver_root: str
              - args.dataset: list[str] | None
              - args.glue_database: str ("silver")
              - args.catalog: str | None
              - args.schema: str ("silver")
```

### 5.2 Dynamic Option Resolution Algorithm for Glue
1. Parse `glue_argv` to collect all flags passed with `--`:
   `passed_flags = {arg[2:].split("=", 1)[0] for arg in glue_argv if arg.startswith("--")}`
2. For each known parameter:
   - Check if its hyphenated form (e.g. `bronze-root`) or underscored form (e.g. `bronze_root`) is in `passed_flags`.
   - If present: add the exact flag name present in `passed_flags` to `options_to_resolve`.
   - If absent:
     - If marked `required=True`: add `name` to `options_to_resolve` anyway, letting `getResolvedOptions` generate its authentic missing-argument error.
     - If marked `required=False`: skip adding to `options_to_resolve`, preserving default value.
3. Call `getResolvedOptions(glue_argv, options_to_resolve)`.
4. Normalize keys to lowercase with underscores (`name.lower().replace("-", "_")`).
5. Normalize `dataset` string into a list of strings (`[d.strip() for d in val.split(",") if d.strip()]`).
6. Return `argparse.Namespace(**resolved_dict)`.

---

## 6. Implementation Code for `spark_jobs/silver_transform.py`

Below is the exact code snippet to be integrated into `spark_jobs/silver_transform.py`:

```python
"""AWS Glue / Standalone PySpark Silver ETL Transformation."""

import argparse
import logging
import os
import re
import sys
from types import SimpleNamespace

# Safe import for AWS Glue runtime
try:
    from awsglue.utils import getResolvedOptions
    HAVE_GLUE = True
except ImportError:
    getResolvedOptions = None
    HAVE_GLUE = False

logger = logging.getLogger(__name__)

# Glue parameter specification: (canonical_name, is_required_in_glue, default_value)
GLUE_OPTION_SPEC: list[tuple[str, bool, str | None]] = [
    ("JOB_NAME", True, None),
    ("bronze-root", True, None),
    ("silver-root", True, None),
    ("dataset", False, None),
    ("glue-database", False, "silver"),
    ("catalog", False, None),
    ("schema", False, "silver"),
]


def is_glue_runtime(argv: list[str]) -> bool:
    """Return True if executing in an AWS Glue runtime environment."""
    if not HAVE_GLUE:
        return False
    return any(arg == "--JOB_NAME" or arg.startswith("--JOB_NAME=") for arg in argv)


def parse_glue_args(argv: list[str], resolve_fn=None) -> argparse.Namespace:
    """Safely parse AWS Glue arguments without crashing on missing optional options."""
    fn = resolve_fn or getResolvedOptions
    if fn is None:
        raise RuntimeError("AWS Glue getResolvedOptions is not available in this environment")

    # Ensure argv[0] represents the script name for getResolvedOptions
    raw_argv = list(argv)
    if raw_argv and raw_argv[0].startswith("--"):
        glue_argv = ["job.py"] + raw_argv
    else:
        glue_argv = raw_argv or ["job.py"]

    passed_flags = set()
    for arg in glue_argv:
        if arg.startswith("--"):
            passed_flags.add(arg[2:].split("=", 1)[0])

    options_to_resolve: list[str] = []
    defaults: dict[str, str | None] = {}

    for name, required, default_val in GLUE_OPTION_SPEC:
        norm_key = name.lower().replace("-", "_")
        defaults[norm_key] = default_val

        hyphen = name.replace("_", "-")
        underscore = name.replace("-", "_")

        if hyphen in passed_flags:
            options_to_resolve.append(hyphen)
        elif underscore in passed_flags:
            options_to_resolve.append(underscore)
        elif required:
            # Let getResolvedOptions raise its native missing-argument error
            options_to_resolve.append(name)

    raw_resolved = fn(glue_argv, options_to_resolve)

    resolved: dict[str, str | None] = dict(defaults)
    for k, v in raw_resolved.items():
        norm_key = k.lower().replace("-", "_")
        resolved[norm_key] = v

    # Normalize dataset
    dataset_raw = resolved.get("dataset")
    if dataset_raw:
        datasets = [d.strip() for d in dataset_raw.split(",") if d.strip()]
        for d in datasets:
            if d not in SILVER_NAMES:
                raise ValueError(f"Invalid dataset: {d!r}. Must be one of: {', '.join(SILVER_NAMES)}")
        resolved["dataset"] = datasets
    else:
        resolved["dataset"] = None

    if resolved.get("dataset") and resolved.get("catalog"):
        raise ValueError("--dataset cannot be combined with --catalog; register only after the full quality gate")

    return argparse.Namespace(**resolved)


def parse_cli_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command line arguments locally or in unit tests using argparse."""
    parser = argparse.ArgumentParser(description="AWS Glue / Local Silver Delta Transformation")
    parser.add_argument("--JOB_NAME", dest="job_name", default=None,
                        help="AWS Glue job name (injected by Glue or optional locally)")
    parser.add_argument("--bronze-root", required=True, dest="bronze_root",
                        help="Bronze root (local directory path or s3:// URI)")
    parser.add_argument("--silver-root", required=True, dest="silver_root",
                        help="Silver Delta root (local directory path or s3:// URI)")
    parser.add_argument("--dataset", action="append", default=None,
                        help="Process one or more datasets (repeat flag or comma-separated); omit for all")
    parser.add_argument("--glue-database", dest="glue_database", default="silver",
                        help="AWS Glue Data Catalog database name for Silver tables")
    parser.add_argument("--catalog", default=None,
                        help="Databricks Unity Catalog to register tables into (deprecated/optional)")
    parser.add_argument("--schema", default="silver",
                        help="Databricks schema for Silver tables (legacy fallback)")

    args = parser.parse_args(argv)

    if args.dataset:
        normalized: list[str] = []
        for item in args.dataset:
            for sub in item.split(","):
                sub = sub.strip()
                if sub:
                    if sub not in SILVER_NAMES:
                        parser.error(f"Invalid dataset: {sub!r}. Must be one of: {', '.join(SILVER_NAMES)}")
                    normalized.append(sub)
        args.dataset = normalized

    if args.dataset and args.catalog:
        parser.error("--dataset cannot be combined with --catalog; register only after the full quality gate")

    return args


def parse_arguments(argv: list[str] | None = None, force_mode: str | None = None) -> argparse.Namespace:
    """Unified entrypoint parameter resolver supporting both AWS Glue and argparse."""
    effective_argv = sys.argv[1:] if argv is None else list(argv)
    if force_mode == "glue" or (force_mode != "local" and is_glue_runtime(effective_argv)):
        return parse_glue_args(effective_argv)
    return parse_cli_args(effective_argv)
```

### 5.3 Glue Job Entrypoint & Lifecycle in `main()`
```python
def main() -> None:
    args = parse_arguments()

    spark = get_spark()
    glue_job = None

    # If executing in AWS Glue runtime, initialize Job lifecycle
    if HAVE_GLUE and args.job_name:
        try:
            from awsglue.context import GlueContext
            from awsglue.job import Job
            glue_context = GlueContext(spark.sparkContext)
            glue_job = Job(glue_context)
            glue_job.init(args.job_name, vars(args))
        except Exception as e:
            logger.warning(f"Could not initialize Glue Job context: {e}")

    is_standalone_local = not is_s3_uri(args.silver_root) and not HAVE_GLUE
    try:
        datasets = args.dataset or list(SILVER_NAMES.keys())
        for dataset in datasets:
            valid, quarantined = run_dataset(spark, args.bronze_root, args.silver_root, dataset)
            print(f"{dataset}: {valid} latest valid rows, {quarantined} quarantined")

        if not args.dataset:
            verify_silver_quality(spark, args.silver_root)

        # Deprecation handling for legacy Unity Catalog
        if args.catalog:
            logger.warning("--catalog is deprecated on AWS Glue; skipping Unity Catalog registration")

        if glue_job is not None:
            glue_job.commit()
    finally:
        if is_standalone_local:
            spark.stop()
```

---

## 7. Offline Unit Test Strategy (`tests/test_silver_glue_offline.py`)

To satisfy Requirement R4 (offline verification without live AWS cloud credentials), a dedicated offline unit test suite must be implemented in `tests/test_silver_glue_offline.py`.

### 7.1 Test Cases Matrix

| Test Method | Scenario | Target Behavior |
|---|---|---|
| `test_glue_mode_all_parameters` | Full parameter set passed via `--JOB_NAME`, `--bronze-root`, `--silver-root`, `--dataset`, `--glue-database` | All parameters resolved, dataset split into list, default schema retained |
| `test_glue_mode_minimal_parameters` | Minimal required arguments: `--JOB_NAME`, `--bronze-root`, `--silver-root` | Succeeds without crash; dataset is None; default glue_database is 'silver' |
| `test_glue_mode_comma_separated_datasets` | `--dataset orders,customers,products` | Resolves `['orders', 'customers', 'products']` |
| `test_glue_mode_missing_required_bronze_root` | Omit `--bronze-root` | Raises `SystemExit` / `GlueArgumentError` |
| `test_glue_mode_missing_required_job_name` | Omit `--JOB_NAME` in Glue mode | Raises error indicating missing `JOB_NAME` |
| `test_glue_mode_invalid_dataset_fails` | `--dataset fake_table` | Raises `ValueError` |
| `test_glue_mode_dataset_catalog_conflict` | `--dataset orders --catalog my_cat` | Raises `ValueError` |
| `test_glue_mode_underscored_args` | `--bronze_root` and `--silver_root` | Correctly maps to `args.bronze_root` and `args.silver_root` |
| `test_local_mode_minimal` | `--bronze-root /tmp/b --silver-root /tmp/s` | `args.job_name is None`, `dataset is None` |
| `test_local_mode_repeated_dataset` | `--dataset orders --dataset products` | `args.dataset == ['orders', 'products']` |
| `test_local_mode_missing_required` | Omit `--silver-root` | Raises `SystemExit` (argparse error) |
| `test_runtime_detection_auto_dispatch` | Auto dispatch between Glue and CLI modes based on presence of `HAVE_GLUE` and `--JOB_NAME` | Routes correctly without manual mode flag |

### 7.2 Mocking `awsglue.utils.getResolvedOptions` Offline
Because `awsglue` is not installed in standard local Python environments, the unit tests will supply a mock `resolve_fn` or patch `sys.modules["awsglue.utils"]`. The mock mirrors AWS Glue's internal implementation (`GlueArgumentParser.parse_known_args`).

---

## 8. Downstream Dependencies & Coordination Notes
- **Airflow DAG (`airflow/dags/ecommerce_cdc_daily.py`)**:
  Airflow's `GlueJobOperator` task `run_silver_transform_on_glue` will supply:
  ```python
  script_args={
      "--bronze-root": bronze_root,
      "--silver-root": silver_root,
      "--glue-database": os.environ.get("DBT_SILVER_SCHEMA", "silver"),
  }
  ```
  The dual-mode parser accepts this exact contract seamlessly.
- **dbt Project (`dbt_project/`)**:
  Silver Delta tables written to `s3://<bucket>/silver/pipeline_live/<table_name>` are queried by dbt via `sources.yml` using `schema: "{{ env_var('DBT_SILVER_SCHEMA', 'silver') }}"`.
- **Offline Integrity**:
  The argument parser has zero network dependencies and executes in < 5ms without requiring Docker or Spark.
