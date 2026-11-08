# Handoff Report: Milestone M2 dbt Profile & Adapter Migration Investigation

**Sender**: `explorer_m2_1`  
**Recipient**: `orchestrator` / `implementer_m2`  
**Milestone**: M2 (Gold Layer & dbt Migration to AWS)  
**Type**: Hard Handoff  

---

## 1. Observation

1. **`dbt_project/dbt_project.yml`**:
   - Line 4 specifies `profile: ecommerce_databricks`.
   - Lines 12–29 configure models: `staging` and `intermediate` as `view`, `marts/core` and `marts/business` as `table` with `+file_format: delta` and `+schema: gold`.
   - Lines 30–33 configure `snapshots` with `+schema: snapshots`.

2. **`dbt_project/profiles.yml` & `profiles.yml.example`**:
   - Configures `ecommerce_databricks` with `type: databricks`, `auth_type: oauth`, and required variables `DBT_DATABRICKS_HOST`, `DBT_DATABRICKS_HTTP_PATH`, `DBT_CATALOG`.

3. **`airflow/dbt_profile/profiles.yml`**:
   - Configures `ecommerce_databricks` with `type: databricks`, `auth_type: pat`, referencing `DATABRICKS_TOKEN`.

4. **`dbt_project/models/staging/sources.yml`**:
   - Line 5 specifies: `database: "{{ env_var('DBT_CATALOG') }}"`.
   - Running `dbt parse` without `DBT_CATALOG` threw verbatim:
     ```
     Parsing Error
       Failed to render models\staging\sources.yml from project ecommerce_cdc: Parsing Error
         Env var required but not provided: 'DBT_CATALOG'
     ```

5. **`pyproject.toml`**:
   - Line 9 specifies: `"dbt-databricks>=1.10,<1.12"`.
   - `uv pip install --dry-run "dbt-glue>=1.8,<1.10"` resolves in 92ms to:
     ```
     Resolved 61 packages in 92ms
     Would download 5 packages
     Would uninstall 3 packages
     Would install 5 packages
      - dbt-core==1.11.8
      + dbt-core==1.9.11
      + dbt-glue==1.9.4
      - dbt-semantic-interfaces==0.9.0
      + dbt-semantic-interfaces==0.7.5
      - dbt-spark==1.10.3
      + dbt-spark==1.9.3
      + waiter==1.6
     ```

6. **`dbt-glue` internals and execution verification**:
   - `dbt/adapters/glue/credentials.py`: `GlueCredentials` is a dataclass supporting `region`, `role_arn`, `schema`, `workers`, `worker_type`, `idle_timeout`, `session_provisioning_timeout_in_seconds`, `location`, `datalake_formats`.
   - `dbt/adapters/glue/connections.py`: `open()` instantiates `GlueConnection` without calling AWS or opening network sockets.
   - `dbt/adapters/glue/impl.py`: `get_table_type` and `get_relation` call `client.get_table()` within `try ... except Exception: return None`.
   - `dbt/task/compile.py`: `populate_adapter_cache` checks `if not self.args.populate_cache: return`. Passing `--no-populate-cache` skips `list_relations_without_caching` entirely.
   - Executing `dbt compile --no-populate-cache --no-version-check --no-send-anonymous-usage-stats` with mock AWS environment credentials on the decoupled project parsed all 35 models, 2 snapshots, 85 tests, 12 sources, and compiled with verbatim:
     ```
     Registered adapter: glue=1.9.4
     Found 35 models, 2 snapshots, 85 data tests, 12 sources, 550 macros
     Concurrency: 1 threads (target='dev')
     exit code: 0
     ```

---

## 2. Logic Chain

1. **Step 1 (Profile & Project Unification)**: Observation 1 shows `dbt_project.yml` pointing to `ecommerce_databricks`. Observations 2 and 3 show all profile files targeting `databricks`. Changing `dbt_project.yml` to `profile: ecommerce_aws` and providing `ecommerce_aws` with `type: glue` in `dbt_project/profiles.yml`, `profiles.yml.example`, and `airflow/dbt_profile/profiles.yml` creates a unified profile definition for both local dev and Airflow container execution.
2. **Step 2 (Safe Defaults for Profile)**: In Observation 6, `GlueCredentials` accepts `region`, `role_arn`, `schema`, `workers`, `worker_type`, `idle_timeout`, `location`, and `datalake_formats: "delta"`. Using Jinja `{{ env_var('VAR', 'default_val') }}` fallbacks ensures that dbt commands never fail due to unset environment variables when running in isolated test suites or CI.
3. **Step 3 (Decoupling sources.yml from Unity Catalog)**: Observation 4 demonstrates that `database: "{{ env_var('DBT_CATALOG') }}"` in `sources.yml` causes a hard parse error when `DBT_CATALOG` is absent. Furthermore, Spark SQL and AWS Glue use a 2-level namespace (`<database/schema>.<table>`), where setting a separate `database` violates `GlueCredentials.__post_init__`. Removing `database` from `sources.yml` resolves sources directly to `{{ schema }}.{{ table }}` (`silver.slv_...`), completely decoupling from Unity Catalog.
4. **Step 4 (Package Selection in pyproject.toml)**: Observation 5 proves that replacing `"dbt-databricks>=1.10,<1.12"` with `"dbt-glue>=1.8,<1.10"` satisfies the version range constraint, cleanly resolves `dbt-glue==1.9.4`, `dbt-core==1.9.11`, and `dbt-spark==1.9.3` with minimal package diffs (5 packages), and eliminates all Databricks dependencies.
5. **Step 5 (Offline Compilation Guarantee)**: Observation 6 demonstrates that while `dbt compile` by default calls relation caching, the `--no-populate-cache` flag completely suppresses calls to AWS Glue `get_tables()`. Model compilation in initial mode catches AWS errors gracefully (`except Exception: return None`), allowing offline compilation of all 35 models and 2 snapshots to succeed with return code 0.

---

## 3. Caveats

1. **Offline Mode vs Live Glue Execution**: In a live execution (`dbt run` on AWS), AWS Glue will spin up Interactive Sessions which require valid IAM permissions (`GLUE_ROLE_ARN`) and network connectivity to AWS Glue. Offline compilation verifies SQL generation and AST graph integrity, but cannot validate AWS IAM role assumption or Glue session startup.
2. **Relation Existence in Incremental DDL**: During offline compilation, `adapter.get_relation()` returns `None`, so incremental models compile as fresh table/view queries rather than executing delta MERGE against an existing remote catalog table.
3. **Botocore Retries**: If mock AWS keys are used without `--no-populate-cache`, `botocore` will attempt to connect to AWS Glue endpoints and retry multiple times. Passing `--no-populate-cache` is required to avoid these network timeouts.

---

## 4. Conclusion

1. **`dbt_project/dbt_project.yml`**: Change `profile: ecommerce_databricks` to `profile: ecommerce_aws`.
2. **`dbt_project/profiles.yml`, `profiles.yml.example`, `airflow/dbt_profile/profiles.yml`**: Define `ecommerce_aws` profile with `type: glue`, safe defaults (`AWS_REGION: ap-southeast-1`, `GLUE_ROLE_ARN: arn:aws:iam::123456789012:role/GlueInteractiveSessionRole`, `DBT_TARGET_SCHEMA: analytics_dev`, `datalake_formats: "delta"`).
3. **`dbt_project/models/staging/sources.yml`**: Remove `database: "{{ env_var('DBT_CATALOG') }}"`.
4. **`pyproject.toml`**: Replace `"dbt-databricks>=1.10,<1.12"` with `"dbt-glue>=1.8,<1.10"`.
5. **Offline compilation**: Run with `--no-populate-cache --no-version-check --no-send-anonymous-usage-stats` to guarantee offline compilation with return code 0.

---

## 5. Verification Method

### 5.1 Independent Verification Commands
Once the implementer applies the diffs:

```powershell
# 1. Update dependencies
uv sync

# 2. Set offline mock environment variables
$env:AWS_ACCESS_KEY_ID="testing"
$env:AWS_SECRET_ACCESS_KEY="testing"
$env:AWS_DEFAULT_REGION="ap-southeast-1"
$env:AWS_REGION="ap-southeast-1"
$env:AWS_EC2_METADATA_DISABLED="true"
$env:DBT_TARGET_SCHEMA="analytics_dev"
$env:DBT_SILVER_SCHEMA="silver"

# 3. Test offline dbt parse
dbt parse --no-version-check --no-send-anonymous-usage-stats --project-dir dbt_project --profiles-dir dbt_project

# 4. Test offline dbt compile
dbt compile --no-populate-cache --no-version-check --no-send-anonymous-usage-stats --project-dir dbt_project --profiles-dir dbt_project

# 5. Verify Gold layer model tests with local PySpark
pytest tests/test_gold_scd2_local.py -v
```

### 5.2 Invalidation Conditions
- If `dbt parse` exits with non-zero or complains about missing adapter `glue` (indicates `dbt-glue` not installed).
- If `dbt parse` fails on `DBT_CATALOG` (indicates `sources.yml` still references `DBT_CATALOG`).
- If `dbt compile` attempts network calls and times out (indicates missing `--no-populate-cache` flag).
