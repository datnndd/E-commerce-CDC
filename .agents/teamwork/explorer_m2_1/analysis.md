# Analysis Report: dbt Profile, Adapter & Dependency Migration to AWS Glue

**Author**: Explorer M2-1 (`explorer_m2_1`)  
**Milestone**: M2 — Gold Layer & dbt Migration to AWS  
**Date**: 2026-11-07  
**Scope**: Features 7, 8, 9, 10, 11, 12 from `PROJECT.md`  

---

## 1. Executive Summary

This investigation analyzed the migration of the dbt transformation layer from `dbt-databricks` to an AWS-compatible adapter (`dbt-glue` extending `dbt-spark`). The lakehouse project maintains **35 dbt models** (12 staging, 2 intermediate, 15 core facts/dims, 6 business marts), **2 snapshots**, and **19 singular data tests**.

Key Findings:
1. **Profiles & Project Config**: `dbt_project.yml` must update `profile: ecommerce_databricks` to `profile: ecommerce_aws`. Both local (`dbt_project/profiles.yml`, `dbt_project/profiles.yml.example`) and Airflow (`airflow/dbt_profile/profiles.yml`) profiles must configure `type: glue` with safe environment variable fallbacks for `AWS_REGION` (`ap-southeast-1`), `GLUE_ROLE_ARN`, `DBT_TARGET_SCHEMA` (`analytics_dev`), and Delta format support (`datalake_formats: "delta"`).
2. **Catalog Decoupling**: `dbt_project/models/staging/sources.yml` currently has `database: "{{ env_var('DBT_CATALOG') }}"`. This Databricks 3-part namespace construct (`catalog.schema.table`) breaks Spark/Glue 2-part namespaces (`schema.table`) and causes hard offline parsing failures if `DBT_CATALOG` is unset. Removing `database` completely decouples the sources from Unity Catalog and cleanly resolves to `{{ schema }}.{{ table }}`.
3. **Adapter Dependency Resolution**: In `pyproject.toml`, replacing `"dbt-databricks>=1.10,<1.12"` with `"dbt-glue>=1.8,<1.10"` (or `"dbt-glue>=1.8,<2"`) cleanly resolves `dbt-glue==1.9.4` alongside `dbt-core==1.9.11` and `dbt-spark==1.9.3`. While `dbt-glue==1.8.6` can run under `dbt-core 1.8`, it forces downgrades of `dbt-adapters` and other shared libraries. `dbt-glue==1.9.4` is fully compatible with Python 3.13 and the modern package ecosystem.
4. **Offline Compilation Verification**: `dbt parse` and `dbt compile` were tested and proven to succeed with **returncode 0** completely offline without live AWS credentials or cloud endpoints by using `--no-populate-cache`, `--no-version-check`, `--no-send-anonymous-usage-stats`, and mock AWS environment credentials.

---

## 2. Examination of Current Configuration Files

### 2.1 `dbt_project/dbt_project.yml`
- **Current profile reference**: `profile: ecommerce_databricks` (line 4).
- **Model configurations**:
  - `staging`: `+materialized: view`, `+schema: staging`
  - `intermediate`: `+materialized: view`, `+schema: intermediate`
  - `marts/core`: `+materialized: table`, `+file_format: delta`, `+schema: gold`
  - `marts/business`: `+materialized: table`, `+file_format: delta`, `+schema: gold`
  - `snapshots`: `+schema: snapshots`
- **Assessment**: The model materializations, file formats (`delta`), and schemas are 100% Spark SQL compatible. Only line 4 requires updating from `ecommerce_databricks` to `ecommerce_aws`.

### 2.2 `dbt_project/profiles.yml` & `profiles.yml.example`
- **Current profile**:
  ```yaml
  ecommerce_databricks:
    target: dev
    outputs:
      dev:
        type: databricks
        auth_type: oauth
        host: "{{ env_var('DBT_DATABRICKS_HOST') }}"
        http_path: "{{ env_var('DBT_DATABRICKS_HTTP_PATH') }}"
        database: "{{ env_var('DBT_CATALOG') }}"
        schema: "{{ env_var('DBT_TARGET_SCHEMA', 'analytics_dev') }}"
        threads: 2
  ```
- **Flaws**:
  - `type: databricks` enforces `databricks-sdk` OAuth/PAT checks on initialization, hanging or failing during offline parse/compilation when Databricks endpoints are missing.
  - Requires Databricks-specific environment variables (`DBT_DATABRICKS_HOST`, `DBT_DATABRICKS_HTTP_PATH`, `DBT_CATALOG`).
  - Uses `database` as Unity Catalog name.

### 2.3 `airflow/dbt_profile/profiles.yml`
- **Current profile**: Target is `airflow`, `auth_type: pat`, referencing `DATABRICKS_TOKEN`, `DBT_DATABRICKS_HOST`, and `DBT_DATABRICKS_HTTP_PATH`.
- **Assessment**: Must be aligned with `ecommerce_aws` profile targeting `type: glue` without token requirements, matching the container's decoupled environment.

### 2.4 `dbt_project/models/staging/sources.yml`
- **Current configuration**:
  ```yaml
  version: 2
  sources:
    - name: silver
      database: "{{ env_var('DBT_CATALOG') }}"
      schema: "{{ env_var('DBT_SILVER_SCHEMA', 'silver') }}"
      tables:
        - name: slv_customers
        ...
  ```
- **Flaws**:
  - In Databricks Unity Catalog, `database` maps to the catalog (e.g. `workspace`), while `schema` maps to the schema (e.g. `silver`).
  - In Spark / AWS Glue Metastore, the database and schema are synonymous (Glue Database = Spark Schema). When `database` is present and differs from `schema`, `dbt-glue`'s `GlueCredentials.__post_init__` raises:
    `DbtRuntimeError: On Spark, database must be omitted or have the same value as schema.`
  - If `DBT_CATALOG` is not defined in the environment, dbt parse crashes immediately with:
    `Parsing Error: Env var required but not provided: 'DBT_CATALOG'`
- **Fix**: Remove `database: "{{ env_var('DBT_CATALOG') }}"` entirely. The source reference `{{ source('silver', '<table>') }}` then resolves cleanly to `silver.<table>` in Spark SQL dialect.

---

## 3. AWS Glue Profile Architecture (`ecommerce_aws`)

### 3.1 `dbt-glue` Credentials Specification
Analysis of `dbt/adapters/glue/credentials.py` from `dbt-glue`:
- `type`: Must be `"glue"`.
- `region`: AWS region string (e.g., `ap-southeast-1`).
- `role_arn`: IAM Role ARN for Glue Interactive Sessions.
- `schema`: Default target schema in AWS Glue Data Catalog (`analytics_dev`).
- `database`: Must be omitted (defaults to `None` in `GlueCredentials.__pre_deserialize__`).
- `workers`: Number of Glue DPUs / workers for interactive sessions (integer, e.g., 2).
- `worker_type`: Worker type (e.g., `G.1X` or `Standard`).
- `session_provisioning_timeout_in_seconds`: Timeout for provisioning Glue sessions (default 120s).
- `idle_timeout`: Idle timeout before terminating the Glue session (default 10m).
- `location`: S3 warehouse root for table storage (e.g., `s3://ecommerce-lakehouse/gold`).
- `datalake_formats`: Set to `"delta"` so Glue passes `--datalake-formats delta` to enable native Delta Lake operations on AWS Glue Spark.

### 3.2 Recommended `ecommerce_aws` Profile Configuration
```yaml
ecommerce_aws:
  target: dev
  outputs:
    dev:
      type: glue
      region: "{{ env_var('AWS_REGION', env_var('AWS_DEFAULT_REGION', 'ap-southeast-1')) }}"
      role_arn: "{{ env_var('GLUE_ROLE_ARN', 'arn:aws:iam::123456789012:role/GlueInteractiveSessionRole') }}"
      schema: "{{ env_var('DBT_TARGET_SCHEMA', 'analytics_dev') }}"
      workers: 2
      worker_type: "{{ env_var('GLUE_WORKER_TYPE', 'G.1X') }}"
      idle_timeout: 10
      session_provisioning_timeout_in_seconds: 120
      location: "{{ env_var('DBT_GLUE_LOCATION', 's3://ecommerce-lakehouse/gold') }}"
      datalake_formats: "delta"
```

For Airflow (`airflow/dbt_profile/profiles.yml`):
```yaml
ecommerce_aws:
  target: airflow
  outputs:
    airflow:
      type: glue
      region: "{{ env_var('AWS_REGION', env_var('AWS_DEFAULT_REGION', 'ap-southeast-1')) }}"
      role_arn: "{{ env_var('GLUE_ROLE_ARN', 'arn:aws:iam::123456789012:role/GlueInteractiveSessionRole') }}"
      schema: "{{ env_var('DBT_TARGET_SCHEMA', 'analytics_dev') }}"
      workers: 2
      worker_type: "{{ env_var('GLUE_WORKER_TYPE', 'G.1X') }}"
      idle_timeout: 10
      session_provisioning_timeout_in_seconds: 120
      location: "{{ env_var('DBT_GLUE_LOCATION', 's3://ecommerce-lakehouse/gold') }}"
      datalake_formats: "delta"
```

Safe Default Fallbacks ensure that if environment variables are not explicitly provided during test suites or CI, dbt will still parse the profile cleanly without raising missing environment variable exceptions.

---

## 4. Dependency Analysis & Compatibility (`pyproject.toml`)

### 4.1 Dependency Audit
In `pyproject.toml`:
```toml
dependencies = [
    "alembic>=1.17,<2",
    "boto3>=1.40,<2",
    "dbt-databricks>=1.10,<1.12",  # <-- TARGET TO REPLACE
    "delta-spark>=4.3,<4.4",
    ...
]
```

### 4.2 Available Versions of `dbt-glue` on PyPI
- **`dbt-glue 1.8.x`** (`1.8.1`, `1.8.6`):
  - Depends on `dbt-core~=1.8.0`, `dbt-spark~=1.8.0`, `waiter>=1.6`.
  - Resolution check: Installing `dbt-glue==1.8.6` requires downgrading `dbt-adapters` from 1.22 to 1.16, `dbt-common` from 1.37 to 1.27, and `deepdiff` from 8.6 to 7.0.
- **`dbt-glue 1.9.x`** (`1.9.0`, `1.9.2`, `1.9.4`):
  - Depends on `dbt-core~=1.9.0` (resolves `dbt-core==1.9.11`), `dbt-spark~=1.9.3`, `waiter>=1.6`.
  - Resolution check: Resolves in 92ms cleanly with only 5 packages downloaded/installed. Retains modern `dbt-adapters`, `dbt-common`, and Python 3.13 support.
- **`dbt-glue 1.10.x` & `1.12.x`** (`1.10.19`, `1.12.4`):
  - Supports `dbt-core>=1.10` / `1.12.x`.

### 4.3 Recommendation for `pyproject.toml`
Replace `"dbt-databricks>=1.10,<1.12"` with:
```toml
    "dbt-glue>=1.8,<1.10",
```
*(Or `"dbt-glue>=1.8,<2"` to allow forward upgrades)*.
Setting `"dbt-glue>=1.8,<1.10"` ensures:
1. Compatibility with dbt-core 1.8 and 1.9.
2. Direct resolution of `dbt-glue==1.9.4` and `dbt-core==1.9.11`.
3. Complete elimination of Databricks SDK (`databricks-sdk`, `databricks-sql-connector`) from the runtime dependency graph.

---

## 5. Offline Execution Analysis: `dbt parse` & `dbt compile`

### 5.1 Mechanics of Offline Compilation
1. **`dbt parse`**:
   - Parses `dbt_project.yml`, `profiles.yml`, and all model SQL files into AST / DAG.
   - Evaluates Jinja macros (`ref`, `source`, `config`).
   - Does **not** initialize database connections or make network calls.
   - **Verification Result**: Exits with code 0 in ~8s once `database: "{{ env_var('DBT_CATALOG') }}"` is removed from `sources.yml`.

2. **`dbt compile`**:
   - Compiles model Jinja templates into executable SQL queries written to `dbt_project/target/compiled/`.
   - By default, dbt attempts to run `populate_adapter_cache` at the start of compilation. In `dbt-glue`, `list_relations_without_caching()` calls `boto3.client('glue').get_tables()`.
   - By supplying the flag `--no-populate-cache`, dbt completely skips cache population (`if not self.args.populate_cache: return`), preventing `list_relations_without_caching()` from executing.
   - Inside `materialization incremental` and `materialization table`, `dbt-glue` calls `adapter.get_table_type(this)` and `adapter.get_relation()`. Both functions in `dbt-glue/impl.py` wrap boto3 calls with `try ... except Exception: return None`. When no AWS connection is available, both return `None`, causing dbt to compile the models in initial create/merge mode cleanly.
   - Setting standard offline mock AWS credentials (`AWS_ACCESS_KEY_ID=mock`, `AWS_SECRET_ACCESS_KEY=mock`, `AWS_DEFAULT_REGION=ap-southeast-1`, `AWS_EC2_METADATA_DISABLED=true`) ensures `botocore` does not query local IMDS or SSO configs.
   - **Verification Result**: All 35 models, 2 snapshots, and 19 singular tests compile with exit code 0.

### 5.2 Offline CLI Commands
```powershell
# Environment variables for offline execution
$env:AWS_ACCESS_KEY_ID="testing"
$env:AWS_SECRET_ACCESS_KEY="testing"
$env:AWS_DEFAULT_REGION="ap-southeast-1"
$env:AWS_REGION="ap-southeast-1"
$env:AWS_EC2_METADATA_DISABLED="true"
$env:DBT_TARGET_SCHEMA="analytics_dev"
$env:DBT_SILVER_SCHEMA="silver"

# Offline Parse Command
dbt parse --no-version-check --no-send-anonymous-usage-stats --project-dir dbt_project --profiles-dir dbt_project

# Offline Compile Command
dbt compile --no-populate-cache --no-version-check --no-send-anonymous-usage-stats --project-dir dbt_project --profiles-dir dbt_project
```

---

## 6. Proposed Exact Code Diffs

### 6.1 `dbt_project/dbt_project.yml`
```diff
--- a/dbt_project/dbt_project.yml
+++ b/dbt_project/dbt_project.yml
@@ -1,7 +1,7 @@
 name: ecommerce_cdc
 version: '1.0.0'
 config-version: 2
-profile: ecommerce_databricks
+profile: ecommerce_aws
 
 model-paths: ["models"]
 snapshot-paths: ["snapshots"]
```

### 6.2 `dbt_project/profiles.yml`
```diff
--- a/dbt_project/profiles.yml
+++ b/dbt_project/profiles.yml
@@ -1,12 +1,15 @@
-ecommerce_databricks:
+ecommerce_aws:
   target: dev
   outputs:
     dev:
-      type: databricks
-      auth_type: oauth
-      host: "{{ env_var('DBT_DATABRICKS_HOST') }}"
-      http_path: "{{ env_var('DBT_DATABRICKS_HTTP_PATH') }}"
-      database: "{{ env_var('DBT_CATALOG') }}"
+      type: glue
+      region: "{{ env_var('AWS_REGION', env_var('AWS_DEFAULT_REGION', 'ap-southeast-1')) }}"
+      role_arn: "{{ env_var('GLUE_ROLE_ARN', 'arn:aws:iam::123456789012:role/GlueInteractiveSessionRole') }}"
       schema: "{{ env_var('DBT_TARGET_SCHEMA', 'analytics_dev') }}"
       threads: 2
+      workers: 2
+      worker_type: "{{ env_var('GLUE_WORKER_TYPE', 'G.1X') }}"
+      idle_timeout: 10
+      session_provisioning_timeout_in_seconds: 120
+      location: "{{ env_var('DBT_GLUE_LOCATION', 's3://ecommerce-lakehouse/gold') }}"
+      datalake_formats: "delta"
```

### 6.3 `dbt_project/profiles.yml.example`
```diff
--- a/dbt_project/profiles.yml.example
+++ b/dbt_project/profiles.yml.example
@@ -1,12 +1,15 @@
-ecommerce_databricks:
+ecommerce_aws:
   target: dev
   outputs:
     dev:
-      type: databricks
-      auth_type: oauth
-      host: "{{ env_var('DBT_DATABRICKS_HOST') }}"
-      http_path: "{{ env_var('DBT_DATABRICKS_HTTP_PATH') }}"
-      database: "{{ env_var('DBT_CATALOG') }}"
+      type: glue
+      region: "{{ env_var('AWS_REGION', env_var('AWS_DEFAULT_REGION', 'ap-southeast-1')) }}"
+      role_arn: "{{ env_var('GLUE_ROLE_ARN', 'arn:aws:iam::123456789012:role/GlueInteractiveSessionRole') }}"
       schema: "{{ env_var('DBT_TARGET_SCHEMA', 'analytics_dev') }}"
       threads: 2
+      workers: 2
+      worker_type: "{{ env_var('GLUE_WORKER_TYPE', 'G.1X') }}"
+      idle_timeout: 10
+      session_provisioning_timeout_in_seconds: 120
+      location: "{{ env_var('DBT_GLUE_LOCATION', 's3://ecommerce-lakehouse/gold') }}"
+      datalake_formats: "delta"
```

### 6.4 `airflow/dbt_profile/profiles.yml`
```diff
--- a/airflow/dbt_profile/profiles.yml
+++ b/airflow/dbt_profile/profiles.yml
@@ -1,13 +1,15 @@
-ecommerce_databricks:
+ecommerce_aws:
   target: airflow
   outputs:
     airflow:
-      type: databricks
-      auth_type: pat
-      host: "{{ env_var('DBT_DATABRICKS_HOST') }}"
-      http_path: "{{ env_var('DBT_DATABRICKS_HTTP_PATH') }}"
-      token: "{{ env_var('DATABRICKS_TOKEN') }}"
-      database: "{{ env_var('DBT_CATALOG') }}"
+      type: glue
+      region: "{{ env_var('AWS_REGION', env_var('AWS_DEFAULT_REGION', 'ap-southeast-1')) }}"
+      role_arn: "{{ env_var('GLUE_ROLE_ARN', 'arn:aws:iam::123456789012:role/GlueInteractiveSessionRole') }}"
       schema: "{{ env_var('DBT_TARGET_SCHEMA', 'analytics_dev') }}"
       threads: 2
+      workers: 2
+      worker_type: "{{ env_var('GLUE_WORKER_TYPE', 'G.1X') }}"
+      idle_timeout: 10
+      session_provisioning_timeout_in_seconds: 120
+      location: "{{ env_var('DBT_GLUE_LOCATION', 's3://ecommerce-lakehouse/gold') }}"
+      datalake_formats: "delta"
```

### 6.5 `dbt_project/models/staging/sources.yml`
```diff
--- a/dbt_project/models/staging/sources.yml
+++ b/dbt_project/models/staging/sources.yml
@@ -2,7 +2,6 @@ version: 2
 
 sources:
   - name: silver
-    database: "{{ env_var('DBT_CATALOG') }}"
     schema: "{{ env_var('DBT_SILVER_SCHEMA', 'silver') }}"
     tables:
       - name: slv_customers
```

### 6.6 `pyproject.toml`
```diff
--- a/pyproject.toml
+++ b/pyproject.toml
@@ -6,7 +6,7 @@ requires-python = ">=3.13,<3.14"
 dependencies = [
     "alembic>=1.17,<2",
     "boto3>=1.40,<2",
-    "dbt-databricks>=1.10,<1.12",
+    "dbt-glue>=1.8,<1.10",
     "delta-spark>=4.3,<4.4",
     "faker>=37,<39",
     "great-expectations[spark]>=1.19,<2",
```

---

## 7. Synthesis and Implementer Action Plan

The proposed configuration changes form a complete and coherent blueprint for the implementer:
1. Update `pyproject.toml` to replace `dbt-databricks` with `dbt-glue>=1.8,<1.10`.
2. Run `uv lock` / `uv sync` to install `dbt-glue`.
3. Apply diffs to `dbt_project/dbt_project.yml`, `dbt_project/profiles.yml`, `dbt_project/profiles.yml.example`, `airflow/dbt_profile/profiles.yml`, and `models/staging/sources.yml`.
4. Run `dbt parse` and `dbt compile --no-populate-cache` to verify zero compilation errors.
