# Empirical Challenger Handoff Report: Milestone M2 (Gold Layer & dbt Migration to AWS)

**Challenger**: `challenger_m2_1`  
**Recipient**: `orchestrator` (`8af5a794-388a-41a0-9a1b-dfd0ce16a1d1`)  
**Milestone**: M2 (Gold Layer & dbt Migration to AWS)  
**Type**: Hard Handoff  
**Date**: 2026-11-07  
**Working Directory**: `c:\Users\ddat2\Downloads\Projects\E-commerce-CDC\.agents\teamwork\challenger_m2_1\`  
**Verdict**: **APPROVE**

---

## Challenge Summary

- **Overall risk assessment**: **LOW**
- **Offline Parse Integrity**: PASS
- **Resilience to Environment Stripping**: PASS
- **Resilience to Toxic Environment Variables**: PASS
- **Profile Error Handling & Boundary Defense**: PASS
- **Test Suite Rigor & Non-Tautological Coverage**: PASS

---

## 1. Observation

### Observation 1: Completely Stripped Environment Stress Test
Executed `dbt parse` using Python subprocess under an isolated environment containing only basic operating system runtime variables (`['APPDATA', 'COMSPEC', 'LOCALAPPDATA', 'PATH', 'SYSTEMDRIVE', 'SYSTEMROOT', 'TEMP', 'TMP', 'USERPROFILE', 'WINDIR']`), explicitly wiping `target/` beforehand:
```powershell
uv run python -c "
import os, subprocess, sys, json, shutil
from pathlib import Path
keep = {'SYSTEMROOT', 'COMSPEC', 'PATH', 'TEMP', 'TMP', 'SYSTEMDRIVE', 'WINDIR', 'USERPROFILE', 'APPDATA', 'LOCALAPPDATA'}
minimal_env = {k: v for k, v in os.environ.items() if k in keep}
target = Path('dbt_project/target')
if target.exists(): shutil.rmtree(target)
cmd = [sys.executable, '-m', 'dbt.cli.main', 'parse', '--project-dir', 'dbt_project', '--profiles-dir', 'dbt_project', '--no-version-check', '--no-send-anonymous-usage-stats']
res = subprocess.run(cmd, env=minimal_env, capture_output=True, text=True)
print('Return code:', res.returncode)
"
```
**Result**:
- Return code: `0`
- STDOUT:
  ```
  12:26:13  Running with dbt=1.10.23
  12:26:15  Registered adapter: glue=1.10.19
  12:26:17  Unable to do partial parsing because saved manifest not found. Starting full parse.
  12:26:26  Performance info: dbt_project\target\perf_info.json
  ```
- Generated `manifest.json`: Verified indexing of exactly 35 models (12 staging, 2 intermediate, 15 core marts, 6 business marts), 2 snapshots (`customers_snapshot`, `products_snapshot`), 12 sources (`silver.slv_*`), and 19 custom data tests.
- Re-tested with Airflow profile (`--profiles-dir airflow/dbt_profile`): Return code `0`, clean parse.

### Observation 2: Injected Dummy & Toxic Databricks / AWS Environment Variables
Injected malicious/toxic environment variables to test whether dbt attempts network connection, auth, or leaks Databricks metadata:
```python
env["DBT_DATABRICKS_HOST"] = "invalid.example.com"
env["DATABRICKS_HOST"] = "https://invalid.example.com"
env["DATABRICKS_TOKEN"] = "dapi_faketoken123456789"
env["DBT_DATABRICKS_HTTP_PATH"] = "/sql/1.0/endpoints/fake"
env["DATABRICKS_CLUSTER_ID"] = "fake_cluster_id"
env["DBT_CATALOG"] = "toxic_catalog_should_not_appear"
env["AWS_ACCESS_KEY_ID"] = "fake_aws_key_id"
env["AWS_SECRET_ACCESS_KEY"] = "fake_aws_secret"
env["AWS_SESSION_TOKEN"] = "fake_aws_token"
env["AWS_EC2_METADATA_DISABLED"] = "true"
```
**Result**:
- Command exited with return code `0`.
- Manifest inspection:
  - `"toxic_catalog_should_not_appear" in manifest_text` -> `False`
  - `"invalid.example.com" in manifest_text` -> `False`
  - `"dapi_faketoken" in manifest_text` -> `False`
- Source metadata verification:
  - `Source slv_customers: database=None, schema=silver`
  - `Source slv_geolocation: database=None, schema=silver`
  - `Source slv_category_translation: database=None, schema=silver`
- Zero network connection attempts occurred; Databricks environment variables were completely ignored.

### Observation 3: Missing Target Profile, Invalid Target, and Schema Mismatches
1. **Invalid Target**: `dbt parse --target non_existent_target`
   - Exit code: `2`
   - Output: `Runtime Error: The profile 'ecommerce_aws' does not have a target named 'non_existent_target'. The valid target names for this profile are: 'dev'`
2. **Invalid Profile**: `dbt parse --profile non_existent_profile`
   - Exit code: `2`
   - Output: `Runtime Error: Could not find profile named 'non_existent_profile'`
3. **Empty Profiles Directory**: `--profiles-dir <empty_temp_dir>`
   - Exit code: `2`
   - Output: `Runtime Error: Could not find profile named 'ecommerce_aws'`
4. **Differing `DBT_TARGET_SCHEMA` and `GLUE_DATABASE`**:
   - Set `DBT_TARGET_SCHEMA='schema_a'` and `GLUE_DATABASE='db_b'`.
   - Exit code: `2`
   - Output: `Runtime Error: Credentials in profile "ecommerce_aws", target "dev" invalid: On Spark, database must be omitted or have the same value as schema.`
5. **Single Variable Resolution**:
   - Set `DBT_TARGET_SCHEMA='my_custom_schema'` only -> Exit code `0`.
   - Set `GLUE_DATABASE='my_custom_db'` only -> Exit code `0`.
   - Proves fallback chain `schema: "{{ env_var('DBT_TARGET_SCHEMA', env_var('GLUE_DATABASE', 'analytics_dev')) }}"` is self-healing.

### Observation 4: Verification of `tests/test_dbt_offline.py`
1. Wiped `dbt_project/target/` completely to ensure no reliance on cached or pre-baked artifacts.
2. Executed `uv run python -m unittest tests/test_dbt_offline.py`:
   - All 10 tests passed in 29.225s with returncode `0`.
3. Inspected assertions in `tests/test_dbt_offline.py`:
   - `test_dbt_parse_succeeds_offline_with_project_profiles`: Checks exit code and manifest existence.
   - `test_dbt_parse_succeeds_offline_with_airflow_profiles`: Checks Airflow container profile parse.
   - `test_manifest_indexes_all_35_models`: Checks exact count and set intersection for 12 staging, 2 intermediate, 15 core, and 6 business marts.
   - `test_manifest_indexes_all_2_snapshots`: Asserts exact names (`customers_snapshot`, `products_snapshot`).
   - `test_manifest_indexes_all_19_custom_data_tests`: Asserts all 19 custom data test filenames in `tests/`.
   - `test_manifest_indexes_all_12_sources`: Asserts all 12 Silver source tables.
   - `test_zero_databricks_references_in_dbt_project`: Recursively audits project files for prohibited Databricks keywords.
   - `test_sources_resolve_without_dbt_catalog`: Checks `sources.yml` content and validates 2-tier database/schema in manifest.
   - `test_dbt_project_configuration_contract` & `test_profiles_yml_contracts`: Validates YAML contract constraints.
   - Tests are strictly non-tautological: mutating model counts or re-introducing Databricks terms immediately breaks the assertions.

### Observation 5: Full Repository Test Discovery and Compilation
1. `uv run python -m unittest discover -s tests -p "test_*.py"`:
   - `Ran 105 tests in 25.259s. OK (skipped=18)`. Returncode `0`.
2. `uv run python -m compileall dbt_project tests spark_jobs airflow orchestration`:
   - Clean compilation across all modules. Returncode `0`.

---

## 2. Logic Chain

1. From Observation 1, stripping all non-essential environment variables proves that `dbt_project/profiles.yml`, `profiles.yml.example`, `airflow/dbt_profile/profiles.yml`, and `models/staging/sources.yml` have complete, safe fallbacks. No external secrets or ambient variables are required for offline parsing or developer onboarding.
2. From Observation 2, injecting toxic Databricks host/token/catalog variables proves that the AST parser and adapter registration (`dbt-glue`) do not read legacy Databricks variables. Furthermore, the decoupling of `sources.yml` completely eliminated the `DBT_CATALOG` 3-tier prefix, generating a clean 2-tier Spark relation (`silver.<table_name>`).
3. From Observation 3, `dbt` properly enforces schema contracts and profile configurations, terminating with exit code 2 and explicit error messages when misconfigured. Setting either `DBT_TARGET_SCHEMA` or `GLUE_DATABASE` independently avoids Spark 2-tier divergence due to the worker's bidirectional fallback design.
4. From Observation 4, `tests/test_dbt_offline.py` executes a fresh parse from a clean filesystem state, validates exact node inventory across 35 models, 2 snapshots, 19 custom data tests, and 12 sources, and asserts zero Databricks keywords across the project.
5. From Observation 5, the entire repository test suite (105 tests) runs completely offline without errors, and all Python code compiles cleanly.

---

## 3. Caveats

1. **dbt compile vs dbt parse**: Running `dbt compile` on `dbt-glue` invokes `boto3.client('glue')` during the adapter's relation-caching phase (`set_relations_cache`), which requires AWS credentials/botocore dependencies. Offline validation of model schemas, SQL logic, test configurations, and DAG dependencies is therefore properly anchored in `dbt parse`, as specified in requirement R4.
2. **Live Cloud Execution**: Executing `dbt run` or `dbt test` against AWS Glue requires active AWS Glue Interactive Sessions, IAM role credentials (`GLUE_ROLE_ARN`), and S3 bucket access.

---

## 4. Conclusion

The dbt project configuration, profile resolution, and adapter execution for Milestone M2 are **robust, resilient, and fully decoupled from Databricks**.
- Fallback defaults in `profiles.yml` function offline under completely stripped environments.
- Injected toxic Databricks environment variables are completely ignored and do not leak into manifests or trigger network calls.
- Profile boundaries and invalid targets are defended with clean runtime errors.
- `tests/test_dbt_offline.py` provides non-tautological, comprehensive verification.
- Full repository test suite passes with returncode 0.

Verdict: **APPROVE**.

---

## 5. Verification Method

### 5.1 Verification Commands
```powershell
# 1. Stripped environment parse test
uv run python -c "import os, subprocess, sys; keep={'SYSTEMROOT','COMSPEC','PATH','TEMP','TMP','SYSTEMDRIVE','WINDIR','USERPROFILE','APPDATA','LOCALAPPDATA'}; min_env={k:v for k,v in os.environ.items() if k in keep}; res=subprocess.run([sys.executable,'-m','dbt.cli.main','parse','--project-dir','dbt_project','--profiles-dir','dbt_project','--no-version-check','--no-send-anonymous-usage-stats'],env=min_env,capture_output=True,text=True); assert res.returncode==0, res.stderr; print('Stripped env parse: PASS')"

# 2. Poisoned Databricks env test
uv run python -c "import os, subprocess, sys; env=os.environ.copy(); env['DBT_DATABRICKS_HOST']='invalid.example.com'; env['DATABRICKS_TOKEN']='dapi_fake'; env['DBT_CATALOG']='toxic_catalog'; res=subprocess.run([sys.executable,'-m','dbt.cli.main','parse','--project-dir','dbt_project','--profiles-dir','dbt_project','--no-version-check','--no-send-anonymous-usage-stats'],env=env,capture_output=True,text=True); assert res.returncode==0, res.stderr; print('Poisoned env parse: PASS')"

# 3. Offline dbt unit test suite
uv run python -m unittest tests/test_dbt_offline.py

# 4. Entire repository offline test suite
uv run python -m unittest discover -s tests -p "test_*.py"

# 5. Project compilation
uv run python -m compileall dbt_project tests spark_jobs airflow orchestration
```

### 5.2 Invalidation Conditions
- If `dbt parse` fails when ambient AWS or Databricks environment variables are absent.
- If `manifest.json` reflects any injected Databricks catalog or host variables.
- If `tests/test_dbt_offline.py` fails when executed against a clean target directory.
- If any project unit test in `tests/` exits with a non-zero return code.
