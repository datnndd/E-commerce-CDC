# Investigation Report: Decoupling Environment from Databricks

**Agent**: `explorer_m3_3`  
**Milestone**: M3 (Airflow Orchestration & Environment Decoupling - Part 3: Dockerfile, Compose, and Environment Configs)  
**Date**: 2026-11-07  
**Working Directory**: `c:\Users\ddat2\Downloads\Projects\E-commerce-CDC\.agents\teamwork\explorer_m3_3\`  

---

## 1. Observation

### 1.1 `docker/airflow.Dockerfile`
Direct observation of `docker/airflow.Dockerfile` (lines 1–14):
```dockerfile
1: FROM apache/airflow:3.3.2-python3.13
2: 
3: USER root
4: ENV UV_PROJECT_ENVIRONMENT=/opt/project-venv
5: ENV PYTHONPATH=/workspace
6: RUN mkdir -p /opt/project-venv /opt/project && chown -R airflow:root /opt/project-venv /opt/project
7: COPY --chown=airflow:root pyproject.toml uv.lock /opt/project/
8: USER airflow
9: WORKDIR /opt/project
10: RUN pip install --no-cache-dir uv==0.11.21
11: RUN uv sync --frozen --no-dev
12: RUN pip install --no-cache-dir apache-airflow-providers-databricks
13: WORKDIR /workspace
```
- Line 12 explicitly installs `apache-airflow-providers-databricks`.
- There are no other Databricks packages installed directly in this Dockerfile.
- The project virtual environment (`/opt/project-venv`) is populated via `uv sync --frozen --no-dev` using `pyproject.toml` and `uv.lock`. In `pyproject.toml`, `boto3>=1.40,<2` and `dbt-glue>=1.8,<1.11` are already configured, while `dbt-databricks` has already been eliminated in Milestone 2.
- Airflow's main execution environment (where `GlueJobOperator` runs) currently lacks `apache-airflow-providers-amazon`.

### 1.2 `compose.yaml`
Direct observation of `compose.yaml` (lines 19–53):
```yaml
19:   airflow:
20:     build:
21:       context: .
22:       dockerfile: docker/airflow.Dockerfile
23:     image: ecommerce-airflow-local:stage6
24:     command: standalone
25:     env_file: .env
26:     environment:
27:       AIRFLOW__CORE__LOAD_EXAMPLES: "False"
28:       AIRFLOW__CORE__PARALLELISM: "2"
29:       AIRFLOW__CORE__DAGS_FOLDER: /workspace/airflow/dags
30:       POSTGRES_HOST: ${AIRFLOW_POSTGRES_HOST:-host.docker.internal}
31:       POSTGRES_PORT: ${AIRFLOW_POSTGRES_PORT:-${POSTGRES_PORT:-5432}}
32:       POSTGRES_DB: ${AIRFLOW_POSTGRES_DB:-${POSTGRES_DB:-ecommerce}}
33:       S3_BUCKET: ${AIRFLOW_S3_BUCKET:-${S3_BUCKET:-}}
34:       AIRFLOW_S3_BUCKET: ${AIRFLOW_S3_BUCKET:-}
35:       PIPELINE_S3_PREFIX: ${PIPELINE_S3_PREFIX:-}
36:       DBT_DATABRICKS_HOST: ${DBT_DATABRICKS_HOST:-}
37:       DBT_DATABRICKS_HTTP_PATH: ${DBT_DATABRICKS_HTTP_PATH:-}
38:       DBT_CATALOG: ${DBT_CATALOG:-}
39:       DBT_SILVER_SCHEMA: ${DBT_SILVER_SCHEMA:-silver}
40:       DBT_TARGET_SCHEMA: ${DBT_TARGET_SCHEMA:-analytics_dev}
41:       DATABRICKS_TOKEN: ${DATABRICKS_TOKEN:-}
42:       DATABRICKS_SILVER_JOB_ID: ${DATABRICKS_SILVER_JOB_ID:-}
43:       AIRFLOW_CONN_DATABRICKS_DEFAULT: '{"conn_type":"databricks","host":"${DBT_DATABRICKS_HOST:-}","password":"${DATABRICKS_TOKEN:-}"}'
44:       AIRFLOW_ALERT_WEBHOOK_URL: ${AIRFLOW_ALERT_WEBHOOK_URL:-}
45:       EXCHANGE_BASE_CURRENCY: ${EXCHANGE_BASE_CURRENCY:-BRL}
46:       EXCHANGE_QUOTE_CURRENCIES: ${EXCHANGE_QUOTE_CURRENCIES:-USD}
47:       UV_PROJECT_ENVIRONMENT: /opt/project-venv
48:     ports:
49:       - "127.0.0.1:${AIRFLOW_PORT:-8080}:8080"
50:     volumes:
51:       - airflow_data:/opt/airflow
52:       - ./:/workspace
```
- Six environment variables in lines 36–38 and 41–43 are Databricks-specific:
  1. `DBT_DATABRICKS_HOST: ${DBT_DATABRICKS_HOST:-}`
  2. `DBT_DATABRICKS_HTTP_PATH: ${DBT_DATABRICKS_HTTP_PATH:-}`
  3. `DBT_CATALOG: ${DBT_CATALOG:-}`
  4. `DATABRICKS_TOKEN: ${DATABRICKS_TOKEN:-}`
  5. `DATABRICKS_SILVER_JOB_ID: ${DATABRICKS_SILVER_JOB_ID:-}`
  6. `AIRFLOW_CONN_DATABRICKS_DEFAULT: '{"conn_type":"databricks","host":"${DBT_DATABRICKS_HOST:-}","password":"${DATABRICKS_TOKEN:-}"}'`
- Missing AWS variables required by AWS Glue Silver execution, `dbt-glue`, and Airflow AWS connection:
  - `AWS_DEFAULT_REGION` / `AWS_REGION`
  - `AWS_ACCESS_KEY_ID`
  - `AWS_SECRET_ACCESS_KEY`
  - `AWS_SESSION_TOKEN`
  - `GLUE_SILVER_JOB_NAME`
  - `GLUE_ROLE_ARN`
  - `GLUE_DATABASE`
  - `DBT_S3_LOCATION`
  - `AIRFLOW_CONN_AWS_DEFAULT`
- The `postgres` service in `compose.yaml` (lines 1–18) contains no Databricks dependencies.

### 1.3 `.env.example`
Direct observation of `.env.example` (lines 17–52):
```ini
17: # S3 Bronze --------------------------------------------------------------------
18: AWS_REGION=ap-southeast-1
19: # Use bucket names only: no s3:// prefix and no trailing slash.
20: # S3_BUCKET=your-project-bucket
21: # Airflow requires the bucket covered by the Databricks external location.
22: # AIRFLOW_S3_BUCKET=your-project-bucket
...
31: # Databricks Silver Job and dbt Gold ------------------------------------------
32: # The SQL warehouse is used by Ops, dbt, and Power BI Import refresh.
33: # DBT_DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
34: # DBT_DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id
35: DBT_CATALOG=ecommerce_cdc
36: DBT_SILVER_SCHEMA=silver
37: DBT_TARGET_SCHEMA=analytics_dev
38: # DATABRICKS_TOKEN=
39: # Numeric ID of a Spark Python Job running spark_jobs/silver_transform.py.
40: # DATABRICKS_SILVER_JOB_ID=
41: 
42: # Local Airflow ----------------------------------------------------------------
43: AIRFLOW_PORT=8080
44: # Airflow runs in Docker; use host.docker.internal for a host-side DB or tunnel.
45: AIRFLOW_POSTGRES_HOST=host.docker.internal
46: # AIRFLOW_POSTGRES_PORT=5432
47: # AIRFLOW_POSTGRES_DB=ecommerce
48: # Compose builds Airflow's Databricks connection from DBT_DATABRICKS_HOST and DATABRICKS_TOKEN.
```
- Line 21 references "Databricks external location".
- Lines 31–41 contain an entire section dedicated to "Databricks Silver Job and dbt Gold" including `DBT_DATABRICKS_HOST`, `DBT_DATABRICKS_HTTP_PATH`, `DBT_CATALOG`, `DATABRICKS_TOKEN`, and `DATABRICKS_SILVER_JOB_ID`.
- Line 48 states: `# Compose builds Airflow's Databricks connection from DBT_DATABRICKS_HOST and DATABRICKS_TOKEN.`
- Lines 53–62 (Isolated 1C test) omit `GLUE_DATABASE`.

---

## 2. Logic Chain

1. **Airflow Docker Image Contract**:
   - In `airflow/dags/ecommerce_cdc_daily.py`, the DAG is replacing `DatabricksRunNowOperator` with `airflow.providers.amazon.aws.operators.glue.GlueJobOperator`.
   - `GlueJobOperator` requires the package `apache-airflow-providers-amazon`.
   - Line 12 of `docker/airflow.Dockerfile` currently installs `apache-airflow-providers-databricks`. Replacing line 12 with `RUN pip install --no-cache-dir apache-airflow-providers-amazon` ensures that:
     a) `GlueJobOperator` and `AwsBaseHook` are importable and functional inside the Airflow container.
     b) Unneeded Databricks provider dependencies (`databricks-sql-connector`, Databricks REST API SDKs) are no longer built into the container image.
     c) `boto3` and `botocore` are pulled in as dependencies of the Amazon provider.

2. **Docker Compose Environment Contract**:
   - Removing the 6 Databricks keys (`DBT_DATABRICKS_HOST`, `DBT_DATABRICKS_HTTP_PATH`, `DBT_CATALOG`, `DATABRICKS_TOKEN`, `DATABRICKS_SILVER_JOB_ID`, `AIRFLOW_CONN_DATABRICKS_DEFAULT`) eliminates stale credentials and prevents confusion.
   - Adding `GLUE_SILVER_JOB_NAME: ${GLUE_SILVER_JOB_NAME:-ecommerce-silver-transform}` fulfills the interface contract established in `PROJECT.md` line 52: `os.environ.get("GLUE_SILVER_JOB_NAME", "ecommerce-silver-transform")`.
   - Adding `AWS_DEFAULT_REGION` and `AWS_REGION` ensures both standard Boto3/AWS SDK conventions and Airflow hooks resolve the target AWS region (default `ap-southeast-1`).
   - Adding `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_SESSION_TOKEN` allows optional local credential forwarding into the container, while defaulting to empty string when relying on instance profiles or offline mocks.
   - Adding `GLUE_ROLE_ARN`, `GLUE_DATABASE`, and `DBT_S3_LOCATION` directly matches the requirements of `airflow/dbt_profile/profiles.yml`, which uses `env_var('GLUE_ROLE_ARN')`, `env_var('GLUE_DATABASE')`, and `env_var('DBT_S3_LOCATION')`.
   - Defining `AIRFLOW_CONN_AWS_DEFAULT: '{"conn_type":"aws","extra":{"region_name":"${AWS_REGION:-ap-southeast-1}"}}'` sets up the default connection expected by `GlueJobOperator(aws_conn_id="aws_default")` without hardcoding credentials, falling back cleanly to environment variables or IAM instance metadata.
   - Tested syntax with `docker compose config`: executes with returncode 0 and cleanly resolves all variables.

3. **`.env.example` Audit and Developer Ergonomics**:
   - Eliminating all Databricks configuration keys and comments removes dead configuration debt.
   - Replacing them with AWS-native variables with safe, clear example values (`GLUE_SILVER_JOB_NAME=ecommerce-silver-transform`, `GLUE_ROLE_ARN=arn:aws:iam::123456789012:role/GlueDbtExecutionRole`, `GLUE_DATABASE=analytics_dev`, `DBT_SILVER_SCHEMA=silver`, `DBT_TARGET_SCHEMA=analytics_dev`, and commented `DBT_S3_LOCATION=s3://your-project-bucket/dbt`) guides developers and CI pipelines to provide only valid AWS parameters.

---

## 3. Caveats

1. **Docker Engine Execution**:
   - Running `docker build` or `docker compose up` directly inside this environment requires access to a running Docker daemon and live internet access to pull base images (`apache/airflow:3.3.2-python3.13`). In read-only offline investigation mode, syntax validation is verified via `docker compose config` rather than running a multi-gigabyte container build.
2. **Local Credential Safety**:
   - The `.env.example` file contains placeholder role ARNs (`arn:aws:iam::123456789012:role/GlueDbtExecutionRole`) and no sensitive secrets. Developers copying `.env.example` to `.env` must never commit their `.env` file to Git (already guarded by `.gitignore`).
3. **Integration with Peer Changes**:
   - Changes to `compose.yaml` align with the DAG migration in `airflow/dags/ecommerce_cdc_daily.py` (explorer_m3_1) and logging decoupling in `orchestration/ops_runs.py` (explorer_m3_2). The variable names (`GLUE_SILVER_JOB_NAME`, `DBT_SILVER_SCHEMA`, `DBT_TARGET_SCHEMA`, `GLUE_DATABASE`) match the contracts across all components.

---

## 4. Conclusion & Proposed Deliverables

The three environment configuration files have been analyzed, and concrete replacement specifications are ready for implementation.

### 4.1 Proposed `docker/airflow.Dockerfile`
File path: `docker/airflow.Dockerfile`  
Pre-created candidate in agent directory: `c:\Users\ddat2\Downloads\Projects\E-commerce-CDC\.agents\teamwork\explorer_m3_3\proposed_airflow.Dockerfile`

```dockerfile
FROM apache/airflow:3.3.2-python3.13

USER root
ENV UV_PROJECT_ENVIRONMENT=/opt/project-venv
ENV PYTHONPATH=/workspace
RUN mkdir -p /opt/project-venv /opt/project && chown -R airflow:root /opt/project-venv /opt/project
COPY --chown=airflow:root pyproject.toml uv.lock /opt/project/
USER airflow
WORKDIR /opt/project
RUN pip install --no-cache-dir uv==0.11.21
RUN uv sync --frozen --no-dev
RUN pip install --no-cache-dir apache-airflow-providers-amazon
WORKDIR /workspace
```

Diff snippet:
```diff
--- a/docker/airflow.Dockerfile
+++ b/docker/airflow.Dockerfile
@@ -9,6 +9,6 @@ USER airflow
 WORKDIR /opt/project
 RUN pip install --no-cache-dir uv==0.11.21
 RUN uv sync --frozen --no-dev
-RUN pip install --no-cache-dir apache-airflow-providers-databricks
+RUN pip install --no-cache-dir apache-airflow-providers-amazon
 WORKDIR /workspace
```

### 4.2 Proposed `compose.yaml`
File path: `compose.yaml`  
Pre-created candidate in agent directory: `c:\Users\ddat2\Downloads\Projects\E-commerce-CDC\.agents\teamwork\explorer_m3_3\proposed_compose.yaml`

```yaml
services:
  postgres:
    image: postgres:18.6
    command: ["postgres", "-c", "wal_level=logical"]
    environment:
      POSTGRES_USER: ${POSTGRES_USER:?Copy .env.example to .env}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:?Set POSTGRES_PASSWORD in .env}
      POSTGRES_DB: ${POSTGRES_DB:-ecommerce}
    ports:
      - "127.0.0.1:${POSTGRES_PORT:-5432}:5432"
    volumes:
      - postgres18_data:/var/lib/postgresql
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U $$POSTGRES_USER -d $$POSTGRES_DB"]
      interval: 10s
      timeout: 5s
      retries: 10

  airflow:
    build:
      context: .
      dockerfile: docker/airflow.Dockerfile
    image: ecommerce-airflow-local:stage6
    command: standalone
    env_file: .env
    environment:
      AIRFLOW__CORE__LOAD_EXAMPLES: "False"
      AIRFLOW__CORE__PARALLELISM: "2"
      AIRFLOW__CORE__DAGS_FOLDER: /workspace/airflow/dags
      POSTGRES_HOST: ${AIRFLOW_POSTGRES_HOST:-host.docker.internal}
      POSTGRES_PORT: ${AIRFLOW_POSTGRES_PORT:-${POSTGRES_PORT:-5432}}
      POSTGRES_DB: ${AIRFLOW_POSTGRES_DB:-${POSTGRES_DB:-ecommerce}}
      S3_BUCKET: ${AIRFLOW_S3_BUCKET:-${S3_BUCKET:-}}
      AIRFLOW_S3_BUCKET: ${AIRFLOW_S3_BUCKET:-}
      PIPELINE_S3_PREFIX: ${PIPELINE_S3_PREFIX:-}
      AWS_DEFAULT_REGION: ${AWS_DEFAULT_REGION:-${AWS_REGION:-ap-southeast-1}}
      AWS_REGION: ${AWS_REGION:-ap-southeast-1}
      AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID:-}
      AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY:-}
      AWS_SESSION_TOKEN: ${AWS_SESSION_TOKEN:-}
      GLUE_SILVER_JOB_NAME: ${GLUE_SILVER_JOB_NAME:-ecommerce-silver-transform}
      GLUE_ROLE_ARN: ${GLUE_ROLE_ARN:-arn:aws:iam::123456789012:role/GlueDbtExecutionRole}
      GLUE_DATABASE: ${GLUE_DATABASE:-${DBT_TARGET_SCHEMA:-analytics_dev}}
      DBT_SILVER_SCHEMA: ${DBT_SILVER_SCHEMA:-silver}
      DBT_TARGET_SCHEMA: ${DBT_TARGET_SCHEMA:-analytics_dev}
      DBT_S3_LOCATION: ${DBT_S3_LOCATION:-}
      AIRFLOW_CONN_AWS_DEFAULT: '{"conn_type":"aws","extra":{"region_name":"${AWS_REGION:-ap-southeast-1}"}}'
      AIRFLOW_ALERT_WEBHOOK_URL: ${AIRFLOW_ALERT_WEBHOOK_URL:-}
      EXCHANGE_BASE_CURRENCY: ${EXCHANGE_BASE_CURRENCY:-BRL}
      EXCHANGE_QUOTE_CURRENCIES: ${EXCHANGE_QUOTE_CURRENCIES:-USD}
      UV_PROJECT_ENVIRONMENT: /opt/project-venv
    ports:
      - "127.0.0.1:${AIRFLOW_PORT:-8080}:8080"
    volumes:
      - airflow_data:/opt/airflow
      - ./:/workspace

volumes:
  postgres18_data:
  airflow_data:
```

Diff snippet:
```diff
--- a/compose.yaml
+++ b/compose.yaml
@@ -33,14 +33,16 @@ services:
       S3_BUCKET: ${AIRFLOW_S3_BUCKET:-${S3_BUCKET:-}}
       AIRFLOW_S3_BUCKET: ${AIRFLOW_S3_BUCKET:-}
       PIPELINE_S3_PREFIX: ${PIPELINE_S3_PREFIX:-}
-      DBT_DATABRICKS_HOST: ${DBT_DATABRICKS_HOST:-}
-      DBT_DATABRICKS_HTTP_PATH: ${DBT_DATABRICKS_HTTP_PATH:-}
-      DBT_CATALOG: ${DBT_CATALOG:-}
+      AWS_DEFAULT_REGION: ${AWS_DEFAULT_REGION:-${AWS_REGION:-ap-southeast-1}}
+      AWS_REGION: ${AWS_REGION:-ap-southeast-1}
+      AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID:-}
+      AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY:-}
+      AWS_SESSION_TOKEN: ${AWS_SESSION_TOKEN:-}
+      GLUE_SILVER_JOB_NAME: ${GLUE_SILVER_JOB_NAME:-ecommerce-silver-transform}
+      GLUE_ROLE_ARN: ${GLUE_ROLE_ARN:-arn:aws:iam::123456789012:role/GlueDbtExecutionRole}
+      GLUE_DATABASE: ${GLUE_DATABASE:-${DBT_TARGET_SCHEMA:-analytics_dev}}
       DBT_SILVER_SCHEMA: ${DBT_SILVER_SCHEMA:-silver}
       DBT_TARGET_SCHEMA: ${DBT_TARGET_SCHEMA:-analytics_dev}
-      DATABRICKS_TOKEN: ${DATABRICKS_TOKEN:-}
-      DATABRICKS_SILVER_JOB_ID: ${DATABRICKS_SILVER_JOB_ID:-}
-      AIRFLOW_CONN_DATABRICKS_DEFAULT: '{"conn_type":"databricks","host":"${DBT_DATABRICKS_HOST:-}","password":"${DATABRICKS_TOKEN:-}"}'
+      DBT_S3_LOCATION: ${DBT_S3_LOCATION:-}
+      AIRFLOW_CONN_AWS_DEFAULT: '{"conn_type":"aws","extra":{"region_name":"${AWS_REGION:-ap-southeast-1}"}}'
       AIRFLOW_ALERT_WEBHOOK_URL: ${AIRFLOW_ALERT_WEBHOOK_URL:-}
```

### 4.3 Proposed `.env.example`
File path: `.env.example`  
Pre-created candidate in agent directory: `c:\Users\ddat2\Downloads\Projects\E-commerce-CDC\.agents\teamwork\explorer_m3_3\proposed_.env.example`

```ini
# Copy this file to .env, fill in the commented cloud settings, and keep .env out of Git.
# These defaults target the main pipeline. See "Isolated 1C test" below before testing source changes.

# PostgreSQL source ------------------------------------------------------------
# Host-side Python commands use POSTGRES_HOST. Compose also uses the user,
# password, database, and port to start its local PostgreSQL service.
POSTGRES_USER=ecommerce
POSTGRES_PASSWORD=replace-with-postgres-password
POSTGRES_DB=ecommerce
POSTGRES_HOST=127.0.0.1
POSTGRES_PORT=5432
POSTGRES_SSLMODE=prefer
# For RDS on EC2: set POSTGRES_HOST to the private endpoint, use verify-full,
# and set POSTGRES_SSLROOTCERT to the absolute path of the RDS CA bundle.
# POSTGRES_SSLROOTCERT=/home/ubuntu/global-bundle.pem

# AWS & S3 Bronze/Silver -------------------------------------------------------
AWS_REGION=ap-southeast-1
AWS_DEFAULT_REGION=ap-southeast-1
# Use bucket names only: no s3:// prefix and no trailing slash.
# S3_BUCKET=your-project-bucket
# AIRFLOW_S3_BUCKET=your-project-bucket
# Choose one credential source for local runs: AWS_PROFILE or both access keys.
# On EC2, leave these unset and use the instance role.
# AWS_PROFILE=ecommerce-cdc-test-sdk
# AWS_ACCESS_KEY_ID=
# AWS_SECRET_ACCESS_KEY=
# Add this only for temporary credentials:
# AWS_SESSION_TOKEN=

# AWS Glue Silver Job and dbt Gold ---------------------------------------------
# Name of the AWS Glue Spark ETL Job running spark_jobs/silver_transform.py.
GLUE_SILVER_JOB_NAME=ecommerce-silver-transform
# IAM execution role ARN for AWS Glue jobs and dbt-glue Spark sessions.
GLUE_ROLE_ARN=arn:aws:iam::123456789012:role/GlueDbtExecutionRole
# AWS Glue Data Catalog database names for Silver tables and Gold models.
DBT_SILVER_SCHEMA=silver
DBT_TARGET_SCHEMA=analytics_dev
GLUE_DATABASE=analytics_dev
# S3 location for dbt model data and Glue temporary tables.
# DBT_S3_LOCATION=s3://your-project-bucket/dbt

# Local Airflow ----------------------------------------------------------------
AIRFLOW_PORT=8080
# Airflow runs in Docker; use host.docker.internal for a host-side DB or tunnel.
AIRFLOW_POSTGRES_HOST=host.docker.internal
# AIRFLOW_POSTGRES_PORT=5432
# AIRFLOW_POSTGRES_DB=ecommerce
# Compose configures Airflow's AWS connection using AWS_REGION and standard AWS credentials.
# AIRFLOW_ALERT_WEBHOOK_URL=
EXCHANGE_BASE_CURRENCY=BRL
EXCHANGE_QUOTE_CURRENCIES=USD

# Isolated 1C test -------------------------------------------------------------
# Keep the same project bucket. Replace the values above with:
#   AIRFLOW_POSTGRES_DB: ecommerce_1c_test
#   DBT_SILVER_SCHEMA: silver_validation_1c
#   DBT_TARGET_SCHEMA: validation_1c
#   GLUE_DATABASE: validation_1c
# Add this line to .env only for the isolated run:
# PIPELINE_S3_PREFIX=stage6_1c_test
# For the main pipeline/Power BI, remove PIPELINE_S3_PREFIX and restore the
# main database and schema values above.
```

---

## 5. Verification Method

1. **Docker Compose Syntax Validation**:
   Execute:
   ```powershell
   docker compose --project-directory . --env-file .agents/teamwork/explorer_m3_3/proposed_.env.example -f .agents/teamwork/explorer_m3_3/proposed_compose.yaml config
   ```
   **Expected**: Returncode 0 with fully parsed YAML output, with no syntax errors, and resolving `GLUE_SILVER_JOB_NAME`, `GLUE_ROLE_ARN`, `GLUE_DATABASE`, and `AIRFLOW_CONN_AWS_DEFAULT`.
   **Result**: Tested and verified. Exit code 0.

2. **Absence of Databricks References Check**:
   Once applied, execute:
   ```powershell
   git grep -i "databricks" -- compose.yaml .env.example docker/airflow.Dockerfile
   ```
   **Expected**: 0 matches across all three files.

3. **dbt Offline Test Suite Compatibility**:
   Execute:
   ```powershell
   uv run python -m unittest tests.test_dbt_offline
   ```
   **Expected**: All 10 tests pass, confirming the profile variables (`GLUE_ROLE_ARN`, `GLUE_DATABASE`, `DBT_S3_LOCATION`, `AWS_REGION`) match the dbt-glue adapter specifications.
   **Result**: Tested and verified. 10/10 tests pass.

4. **Python Syntax Compilation**:
   Execute:
   ```powershell
   uv run python -m compileall spark_jobs airflow dbt_project orchestration tests
   ```
   **Expected**: Exit code 0, all files compile with zero syntax errors.
