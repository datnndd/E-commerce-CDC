# Forensic Audit Report: Milestone M2 (Gold Layer & dbt Migration to AWS)

**Auditor**: `auditor_m2_1` (Forensic Integrity Auditor)  
**Parent / Recipient**: `orchestrator` (`8af5a794-388a-41a0-9a1b-dfd0ce16a1d1`)  
**Target**: Milestone M2 (Gold Layer & dbt Migration to AWS)  
**Integrity Mode**: Benchmark  
**Verdict**: **CLEAN**  
**Working Directory**: `c:\Users\ddat2\Downloads\Projects\E-commerce-CDC\.agents\teamwork\auditor_m2_1\`

---

## 1. Observation

1. **Dependency Installation & Lockfile Analysis**:
   - `pyproject.toml:9`: `"dbt-glue>=1.8,<1.11"` replaced `"dbt-databricks>=1.10,<1.12"`.
   - Empirically queried `importlib.metadata`:
     - `dbt-core`: `1.10.23`
     - `dbt-glue`: `1.10.19`
     - `dbt-databricks`: `NOT INSTALLED`
     - `databricks-sdk`: `NOT INSTALLED`
     - `databricks-sql-connector`: `NOT INSTALLED`
   - In `uv.lock`, Databricks dependencies (`databricks-sdk`, `databricks-sql-connector`, `dbt-databricks`, `thrift`, `secretstorage`) were completely purged, and `dbt-glue` 1.10.19 and `waiter` were added.

2. **Configuration Decoupling**:
   - `dbt_project/dbt_project.yml:4`: `profile: ecommerce_aws` (previously `profile: ecommerce_databricks`).
   - `dbt_project/profiles.yml:1-16` & `profiles.yml.example:1-16`:
     ```yaml
     ecommerce_aws:
       target: dev
       outputs:
         dev:
           type: glue
           query_type: spark
           region: "{{ env_var('AWS_REGION', 'ap-southeast-1') }}"
           role_arn: "{{ env_var('GLUE_ROLE_ARN', 'arn:aws:iam::123456789012:role/GlueDbtExecutionRole') }}"
           schema: "{{ env_var('DBT_TARGET_SCHEMA', env_var('GLUE_DATABASE', 'analytics_dev')) }}"
           database: "{{ env_var('GLUE_DATABASE', env_var('DBT_TARGET_SCHEMA', 'analytics_dev')) }}"
           location: "{{ env_var('DBT_S3_LOCATION', 's3://ecommerce-lakehouse/dbt') }}"
           datalake_formats: "delta"
           workers: 2
           worker_type: G.1X
           idle_timeout: 10
     ```
   - `airflow/dbt_profile/profiles.yml:1-28`: Both `airflow` and `dev` targets configured identically with `type: glue`.
   - Ripgrep searches for `databricks` and `DBT_CATALOG` across `dbt_project/` and `airflow/dbt_profile/` returned zero matches.

3. **Source Model 2-Tier Namespace Decoupling**:
   - `dbt_project/models/staging/sources.yml`: `database: "{{ env_var('DBT_CATALOG') }}"` was removed.
   - All 12 source tables retained under `schema: "{{ env_var('DBT_SILVER_SCHEMA', 'silver') }}"`.
   - Inspection of compiled SQL (`dbt_project/target/compiled/ecommerce_cdc/models/staging/stg_orders.sql`):
     ```sql
     select * from silver.slv_orders
     ```
     Resolves cleanly as a 2-tier table relation (`silver.slv_orders`) without 3-tier catalog prefixes.

4. **Pre-Populated Artifact & Facade Detection**:
   - Tested filesystem before execution: `dbt_project/target` did NOT exist (`Test-Path dbt_project/target` returned `False`).
   - `target/manifest.json` is generated dynamically by executing `dbt parse`, proving it is not a pre-populated static fixture.
   - Zero hardcoded mock returns or facade stubs found in `dbt_project/` or `tests/test_dbt_offline.py`.

5. **Independent Execution of Required Test Commands**:
   - Command 1: `uv run dbt parse --project-dir dbt_project --profiles-dir dbt_project --no-version-check --no-send-anonymous-usage-stats`
     - Output:
       ```
       12:28:36 Running with dbt=1.10.23
       12:28:39 Registered adapter: glue=1.10.19
       12:28:41 Unable to do partial parsing because profile has changed
       12:28:51 Performance info: dbt_project\target\perf_info.json
       ```
     - Return code: `0`.
   - Command 2: `uv run python -m unittest tests/test_dbt_offline.py`
     - Output: `Ran 10 tests in 19.403s. OK`. Return code: `0`.
   - Command 3: `uv run python -m compileall dbt_project tests`
     - Output: All files listed and compiled cleanly. Return code: `0`.
   - Command 4: `uv run python -m unittest discover -s tests -p "test_*.py"`
     - Output: `Ran 105 tests in 21.976s. OK (skipped=18)`. Return code: `0`.

6. **Adversarial `dbt compile` Stress Test**:
   - Executing `dbt compile` with scrubbed credentials and mock region/schema resolved all 35 models, 2 snapshots, 85 data tests, 12 sources, and 562 macros.
   - Output confirmed the adapter attempts to contact AWS Glue (`GetTables` / `GetTable` via `boto3.client('glue')`), completely decoupled from Databricks SQL Warehouse.
   - All 35 model SQL queries and tests compiled cleanly to `dbt_project/target/compiled/`.

---

## 2. Logic Chain

1. From Observation 1, `dbt-databricks` and its runtime SDKs (`databricks-sdk`, `databricks-sql-connector`) were completely removed from both `pyproject.toml`, `uv.lock`, and the active virtual environment. The official open-source adapter `dbt-glue` (1.10.19) and `dbt-core` (1.10.23) were genuinely installed.
2. From Observation 2, all references to Databricks profiles (`ecommerce_databricks`, `type: databricks`, `DBT_DATABRICKS_HOST`, `DBT_DATABRICKS_HTTP_PATH`, `DATABRICKS_TOKEN`) were eliminated from `dbt_project.yml`, `profiles.yml`, `profiles.yml.example`, and `airflow/dbt_profile/profiles.yml`.
3. From Observation 3, removing `database: "{{ env_var('DBT_CATALOG') }}"` from `sources.yml` shifts source references from Databricks Unity Catalog 3-tier (`catalog.silver.table`) to Spark / AWS Glue 2-tier (`silver.table`), verified by examining the generated AST in `target/compiled/`.
4. From Observation 4, `target/` was absent prior to audit execution, proving that test results and `manifest.json` are generated dynamically and not hardcoded or fabricated.
5. From Observation 5 and 6, independent test executions verify that:
   - `dbt parse` resolves the full project DAG offline with return code 0.
   - `tests/test_dbt_offline.py` executes genuine subprocess calls to `dbt parse` in an isolated environment and non-tautologically verifies all 35 models, 2 snapshots, 19 custom data tests, and 12 sources.
   - `dbt compile` executes cleanly and routes network requests exclusively to AWS Glue APIs rather than Databricks.
   - Full repository test discovery executes 105 tests with 0 failures.
6. Under Benchmark Integrity Mode, no prohibited patterns (hardcoded test results, facade implementations, fabricated verification outputs, self-certifying tests, or unauthorized delegation of core deliverables) were detected.

---

## 3. Caveats

1. **Live AWS Glue Interactive Sessions**: Offline parsing and compilation verify SQL syntax, macro expansions, relation contracts, and DAG graph structure. Live table materialization requires an AWS IAM role (`GLUE_ROLE_ARN`) and active Glue Interactive Sessions.
2. **Skipped Live Integration Tests**: In full test discovery (`Ran 105 tests ... OK (skipped=18)`), 18 tests were skipped because they require live external services (e.g., PostgreSQL container or Docker daemon for `test_cdc_extract.py` and `test_silver_local.py`). This is standard and expected for offline unit test suites.

---

## 4. Conclusion

The implementation for Milestone M2 (Gold Layer & dbt Migration to AWS) is **CLEAN**.
- All Databricks dependencies across the dbt project and environment have been removed.
- The project is fully migrated to AWS Glue (`dbt-glue`), supporting delta table formats.
- All 35 models, 2 snapshots, and 19 custom tests compile and parse offline without cloud credentials.
- `tests/test_dbt_offline.py` provides authentic, rigorous offline contract enforcement.
- Final Verdict: **CLEAN** (Accepted).

---

## 5. Verification Method

To independently reproduce this forensic audit:

```powershell
# 1. Verify absence of Databricks packages
uv run python -c "import importlib.metadata as m; [print(p, 'INSTALLED') for p in ['dbt-databricks', 'databricks-sdk', 'databricks-sql-connector'] if m.version(p)]"

# 2. Run dbt parse offline
uv run dbt parse --project-dir dbt_project --profiles-dir dbt_project --no-version-check --no-send-anonymous-usage-stats

# 3. Run dbt offline verification test suite
uv run python -m unittest tests/test_dbt_offline.py

# 4. Verify python syntax and compilation
uv run python -m compileall dbt_project tests

# 5. Run complete repository test discovery
uv run python -m unittest discover -s tests -p "test_*.py"
```

### Invalidation Conditions
- If any Databricks package is installed or referenced in `dbt_project/` or `airflow/dbt_profile/`.
- If `dbt parse` requires live AWS credentials or exits with a non-zero code.
- If `tests/test_dbt_offline.py` or repository tests fail.
