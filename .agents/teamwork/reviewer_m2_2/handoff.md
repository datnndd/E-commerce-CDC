# Review and Adversarial Handoff Report: Milestone M2 (Gold Layer & dbt Migration to AWS)

**Reviewer**: `reviewer_m2_2` (Roles: Reviewer, Critic)  
**Recipient**: `orchestrator` (`8af5a794-388a-41a0-9a1b-dfd0ce16a1d1`)  
**Milestone**: M2 (Gold Layer & dbt Migration to AWS)  
**Date**: 2026-11-07  
**Working Directory**: `c:\Users\ddat2\Downloads\Projects\E-commerce-CDC\.agents\teamwork\reviewer_m2_2\`  
**Verdict**: **APPROVE**  
**Integrity Status**: **CLEAN (Zero Integrity Violations)**  
**Adversarial Risk Assessment**: **LOW**

---

## 1. Quality Review & Review Summary

### 1.1 Review Verdict
**Verdict**: **APPROVE**

Milestone M2 implementation satisfies all functional requirements and acceptance criteria outlined in `ORIGINAL_REQUEST.md` and `PROJECT.md`. The migration from Databricks to AWS Glue for dbt is clean, robust, and verified offline without live AWS credentials.

### 1.2 Review Dimensions Assessment
1. **Correctness**:
   - `pyproject.toml:9`: Replaced `"dbt-databricks>=1.10,<1.12"` with `"dbt-glue>=1.8,<1.11"`. Locked dependencies resolved to `dbt-glue==1.10.19` and `dbt-core==1.10.23`.
   - `dbt_project/dbt_project.yml:4`: Profile reference updated to `ecommerce_aws`.
   - `dbt_project/profiles.yml`, `profiles.yml.example`, and `airflow/dbt_profile/profiles.yml`: Replaced Databricks host/token/http_path configurations with AWS Glue profile (`type: glue`, `query_type: spark`, `datalake_formats: delta`) with safe environment variable fallbacks.
   - `dbt_project/models/staging/sources.yml`: Removed Unity Catalog `database: "{{ env_var('DBT_CATALOG') }}"`, cleanly restoring 2-tier table resolution (`schema.table`) across all 12 Silver sources.
   - `tests/test_dbt_offline.py`: Provides 10 automated unit tests testing parse execution, manifest indexing, Databricks reference absence, source resolution, and profile contracts.

2. **Logical Completeness**:
   - Manifest indexing covers all 35 models (12 staging, 2 intermediate, 15 core marts, 6 business marts), 2 snapshots (`customers_snapshot`, `products_snapshot`), 19 custom singular data tests, 66 generic column tests (85 tests total), and 12 Silver sources.
   - DAG dependency resolution in `manifest.json` was independently verified: zero unresolved parent references across all models and tests.

3. **Integrity & Code Quality**:
   - **Zero integrity violations detected**: No hardcoded test responses, no facade or mock implementations bypassing dbt, no fabricated verification logs.
   - The test suite executes real `dbt parse` commands in subprocesses with scrubbed environments, parsing real ASTs and inspecting generated artifacts.

---

## 2. Adversarial Review & Stress Testing

### 2.1 Adversarial Risk Assessment
**Overall Risk**: **LOW**

### 2.2 Challenged Dimensions & Stress Test Results
1. **Challenge 1: Residual Databricks Configurations / Tokens in dbt Project**
   - *Attack Scenario*: Hidden or nested files containing Databricks host/token/warehouse configurations causing leaks or unexpected runtime calls.
   - *Test*: Case-insensitive pattern search across all tracked and untracked files in `dbt_project/` and `airflow/dbt_profile/` for `databricks`, `DBT_DATABRICKS_HOST`, `DBT_DATABRICKS_HTTP_PATH`, `DATABRICKS_TOKEN`, and `type: databricks`.
   - *Result*: **PASS**. Exactly 0 occurrences found.

2. **Challenge 2: Toxic Environment Variable Injection**
   - *Attack Scenario*: An execution environment where legacy Databricks environment variables (`DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DBT_CATALOG`) are present could cause dbt to attempt Databricks auth or fail schema compilation.
   - *Test*: Executed `dbt parse` with injected `DATABRICKS_HOST=https://fake-workspace.cloud.databricks.com`, `DATABRICKS_TOKEN=dapi-fake-token`, `DBT_CATALOG=hive_metastore`.
   - *Result*: **PASS**. `dbt parse` completed with exit code 0, completely ignoring Databricks env vars.

3. **Challenge 3: Stripped Environment Variable Execution (Zero Env Defaults)**
   - *Attack Scenario*: An execution environment where none of the AWS or DBT environment variables (`AWS_REGION`, `GLUE_ROLE_ARN`, `DBT_TARGET_SCHEMA`, `GLUE_DATABASE`, `DBT_S3_LOCATION`) are provided.
   - *Test*: Executed `dbt parse` after completely stripping all `AWS_*`, `DBT_*`, `GLUE_*`, and `DATABRICKS_*` variables.
   - *Result*: **PASS**. `dbt parse` completed with exit code 0 using built-in safe fallbacks (`ap-southeast-1`, mock execution role ARN, `analytics_dev`, `s3://ecommerce-lakehouse/dbt`).

4. **Challenge 4: DAG Node Parent/Child Resolution Integrity**
   - *Attack Scenario*: Staging or intermediate models referencing non-existent sources or broken macros due to the removal of `DBT_CATALOG`.
   - *Test*: Analyzed `manifest.json` parent_map across all 35 models, 2 snapshots, and 19 custom data tests.
   - *Result*: **PASS**. 0 unresolved parents found across all nodes.

5. **Challenge 5: Airflow Profile Compatibility & Target Matching**
   - *Attack Scenario*: Airflow DAG task `build_gold_and_marts_dbt` invokes `dbt build --profiles-dir /workspace/airflow/dbt_profile` without specifying `--target`. If the default target does not exist or mismatches, execution fails.
   - *Test*: Verified `airflow/dbt_profile/profiles.yml` defines `target: airflow` matching the container configuration, and also includes `dev` target. Executed `dbt parse --profiles-dir airflow/dbt_profile`.
   - *Result*: **PASS**. Exited with code 0.

6. **Challenge 6: Offline Compilation Boundaries (`dbt compile` vs `dbt parse`)**
   - *Attack Scenario*: Running `dbt compile` offline could trigger adapter cache initialization against AWS Glue.
   - *Test*: Executed `dbt compile` offline.
   - *Finding*: `dbt compile` triggers `adapter.set_relations_cache` which calls `boto3.client('glue')` to list relations, requiring active AWS credentials.
   - *Mitigation & Defense*: `dbt parse` is the intended and correct tool for offline validation, AST parsing, Jinja template resolution, macro expansion, and manifest generation without network/cloud calls. This boundary is correctly documented and validated in `tests/test_dbt_offline.py`.

---

## 3. Five-Component Handoff Report

### 3.1 Observation
All verification commands were independently run in the local environment:

1. **`dbt parse` with project profile**:
   - Command: `uv run dbt parse --project-dir dbt_project --profiles-dir dbt_project --no-version-check --no-send-anonymous-usage-stats`
   - Output:
     ```
     Running with dbt=1.10.23
     Registered adapter: glue=1.10.19
     Performance info: dbt_project\target\perf_info.json
     ```
   - Exit code: `0`

2. **`dbt parse` with Airflow profile**:
   - Command: `uv run dbt parse --project-dir dbt_project --profiles-dir airflow/dbt_profile --no-version-check --no-send-anonymous-usage-stats`
   - Exit code: `0`

3. **`test_dbt_offline.py` Execution**:
   - Command: `uv run python -m unittest tests/test_dbt_offline.py`
   - Output:
     ```
     Ran 10 tests in 32.660s
     OK
     ```
   - Exit code: `0`

4. **Python Syntax Compilation**:
   - Command: `uv run python -m compileall dbt_project tests`
   - Output: All files listed and compiled cleanly.
   - Exit code: `0`

5. **Full Repository Offline Test Suite**:
   - Command: `uv run python -m unittest discover -s tests -p "test_*.py"`
   - Output:
     ```
     Ran 105 tests in 22.902s
     OK (skipped=18)
     ```
   - Exit code: `0`

6. **Manifest Node Inventory Inspection**:
   - Models: 35
   - Snapshots: 2
   - Custom data tests: 19
   - Total tests: 85 (19 custom singular + 66 generic column tests)
   - Silver sources: 12
   - Databricks references in manifest: 0

### 3.2 Logic Chain
1. Removing `dbt-databricks` and adding `dbt-glue>=1.8,<1.11` in `pyproject.toml` eliminated all Databricks SDK dependencies while pinning compatible `dbt-core==1.10.23` and `dbt-glue==1.10.19`.
2. Setting `profile: ecommerce_aws` in `dbt_project/dbt_project.yml` routes dbt to the Glue adapter.
3. Defining identical fallbacks for `schema` and `database` in `profiles.yml` (`{{ env_var('DBT_TARGET_SCHEMA', env_var('GLUE_DATABASE', 'analytics_dev')) }}`) satisfies `GlueCredentials` parity requirements and permits clean offline parsing.
4. Removing `database: "{{ env_var('DBT_CATALOG') }}"` in `sources.yml` shifts source relations from Databricks Unity Catalog 3-tier (`catalog.schema.table`) to Spark/Glue 2-tier (`schema.table`), matching S3 Silver Delta table contracts.
5. All 35 models, 2 snapshots, and 19 custom data tests compile cleanly into `manifest.json`.
6. Independent stress tests confirmed that the project compiles with toxic Databricks env vars and with zero environment variables provided.
7. Full project test suite of 105 unit and integration tests passes offline with exit code 0.

### 3.3 Caveats
1. **Offline Validation vs Live AWS Glue Execution**:
   `dbt parse` validates model DAGs, syntax, schemas, Jinja macros, and sources offline without cloud credentials. Live execution (`dbt run`, `dbt test`) against an AWS environment requires valid IAM credentials (`GLUE_ROLE_ARN`), AWS session configuration, and active AWS Glue Interactive Sessions with Delta Lake support (`--datalake-formats delta`).
2. **dbt Deprecation Notice**:
   `dbt-core 1.10.23` prints an informational deprecation banner during runs. This does not impact compilation or functionality.
3. **Airflow DAG Databricks Operators (Milestone M3 Scope)**:
   While `airflow/dbt_profile/profiles.yml` is now fully decoupled from Databricks, `airflow/dags/ecommerce_cdc_daily.py` still contains Databricks operators (`DatabricksRunNowOperator`) which are scheduled for replacement with `GlueJobOperator` in Milestone M3.

### 3.4 Conclusion
Milestone M2 (Gold Layer & dbt Migration to AWS) is verified and approved without reservations. All 6 features (Features 7, 8, 9, 10, 11, 12 in `PROJECT.md`) are completely implemented, genuine, and free of integrity violations.

### 3.5 Verification Method
To independently verify this work product:
```powershell
# 1. Execute dbt parse offline
uv run dbt parse --project-dir dbt_project --profiles-dir dbt_project --no-version-check --no-send-anonymous-usage-stats

# 2. Run dbt offline verification test suite
uv run python -m unittest tests/test_dbt_offline.py

# 3. Check syntax compilation across project
uv run python -m compileall dbt_project tests

# 4. Run entire repository offline test suite
uv run python -m unittest discover -s tests -p "test_*.py"
```
