# Handoff Report: Survey of Gold Layer and dbt Transformations for AWS Migration (Requirement R2)

**Agent**: `explorer_survey_2`  
**Date**: 2026-11-07  
**Scope**: Gold Layer, dbt Project (`dbt_project/`), Business Data Marts, and Adapter Migration to AWS-Compatible Engine (Requirement R2).

---

## 1. Observation

### 1.1 Project Structure & Inventory
- **dbt Project Directory**: `dbt_project/` containing:
  - Configuration: `dbt_project.yml` (33 lines), `profiles.yml` (12 lines), `profiles.yml.example` (12 lines).
  - Airflow profile: `airflow/dbt_profile/profiles.yml` (13 lines).
  - Models: **35 total SQL models**:
    - Staging: 12 models in `models/staging/` (`stg_customers.sql`, `stg_orders.sql`, `stg_order_items.sql`, `stg_payments.sql`, `stg_reviews.sql`, `stg_products.sql`, `stg_sellers.sql`, `stg_geolocation.sql`, `stg_category_translation.sql`, `stg_clickstream_events.sql`, `stg_exchange_rates.sql`, `stg_campaigns.sql`).
    - Intermediate: 2 models in `models/intermediate/` (`int_orders_enriched.sql`, `int_customer_orders.sql`).
    - Core Dimensions: 8 models in `models/marts/core/dimensions/` (`dim_campaign.sql`, `dim_category.sql`, `dim_channel.sql`, `dim_customer.sql`, `dim_date.sql`, `dim_geography.sql`, `dim_product.sql`, `dim_seller.sql`).
    - Core Facts: 7 models in `models/marts/core/commerce/` (`fct_orders.sql`, `fct_order_items.sql`, `fct_payments.sql`, `fct_reviews.sql`), `models/marts/core/behavior/` (`fct_clickstream_events.sql`, `fct_sessions.sql`), and `models/marts/core/reference/` (`fct_exchange_rates.sql`).
    - Business Marts: 6 models in `models/marts/business/` (`mart_daily_sales.sql`, `mart_product_performance.sql`, `mart_customer_rfm.sql`, `mart_funnel_daily.sql`, `mart_payment_mix_daily.sql`, `mart_order_experience_daily.sql`).
  - Snapshots: 2 models in `snapshots/` (`customers_snapshot.sql`, `products_snapshot.sql`).
  - Custom SQL Tests: 19 test files in `tests/` (`assert_core_grains.sql`, `assert_core_reconciliation.sql`, `assert_daily_sales_grain.sql`, `assert_experience_totals.sql`, `assert_funnel_bounds.sql`, `assert_funnel_daily_grain.sql`, `assert_funnel_totals.sql`, `assert_item_revenue.sql`, `assert_nonnegative_items.sql`, `assert_order_experience_grain.sql`, `assert_payment_mix_grain.sql`, `assert_payment_totals.sql`, `assert_product_performance_grain.sql`, `assert_product_totals.sql`, `assert_rfm_scores.sql`, `assert_rfm_ties.sql`, `assert_rfm_totals.sql`, `assert_sales_totals.sql`, `assert_scd2_windows.sql`).
  - Schema documentation & tests: `models/staging/sources.yml`, `models/marts/core/core.yml`, `models/marts/business/business.yml`.

### 1.2 Databricks Dependencies in Code & Configuration
- **`dbt_project/dbt_project.yml`**:
  - Line 4: `profile: ecommerce_databricks`.
  - Line 23: `+file_format: delta` in `marts.core`.
  - Line 27: `+file_format: delta` in `marts.business`.
- **`dbt_project/profiles.yml` & `profiles.yml.example`**:
  - Line 1: `ecommerce_databricks:`
  - Line 5: `type: databricks`
  - Line 6: `auth_type: oauth`
  - Lines 7-10: `host: "{{ env_var('DBT_DATABRICKS_HOST') }}"`, `http_path: "{{ env_var('DBT_DATABRICKS_HTTP_PATH') }}"`, `database: "{{ env_var('DBT_CATALOG') }}"`, `schema: "{{ env_var('DBT_TARGET_SCHEMA', 'analytics_dev') }}"`.
- **`airflow/dbt_profile/profiles.yml`**:
  - Lines 1-12: `ecommerce_databricks`, `type: databricks`, `auth_type: pat`, `token: "{{ env_var('DATABRICKS_TOKEN') }}"`.
- **`dbt_project/models/staging/sources.yml`**:
  - Line 5: `database: "{{ env_var('DBT_CATALOG') }}"`
  - Line 6: `schema: "{{ env_var('DBT_SILVER_SCHEMA', 'silver') }}"`
  - Lines 8-19: 12 Silver tables (`slv_customers` through `slv_campaigns`).
- **Snapshots**:
  - `snapshots/customers_snapshot.sql:2` and `snapshots/products_snapshot.sql:2`: `file_format='delta'`.
- **Incremental Models**:
  - `fct_orders.sql:1`, `fct_order_items.sql:1`, `fct_payments.sql:1`, `fct_reviews.sql:1`, `fct_clickstream_events.sql:1`, `fct_exchange_rates.sql:1`: all specify `file_format='delta'`, `incremental_strategy='merge'`, and `timestamp('1900-01-01')`.
- **Spark SQL Dialect Functions**:
  - `from_utc_timestamp(..., 'America/Sao_Paulo')`: `dim_date.sql:2-7`, `fct_orders.sql:7-8`, `fct_reviews.sql:4`, `fct_clickstream_events.sql:3`, `fct_sessions.sql:4`, `mart_order_experience_daily.sql:9-10,12-13`.
  - `lateral view explode(sequence(...))`: `dim_date.sql:15`.
  - `to_date(...)`: `dim_date.sql`, `int_customer_orders.sql:4`, `mart_customer_rfm.sql:5`, `mart_order_experience_daily.sql:9-13`.
  - `datediff(end, start)` (2 args): `mart_customer_rfm.sql:13`, `mart_order_experience_daily.sql:8-11`, `dbt_project/tests/assert_rfm_scores.sql:6`.
  - `date_add(date, 1)`: `mart_customer_rfm.sql:10`.
  - `unix_timestamp(...)`: `fct_reviews.sql:6`.
  - `min_by(...)`: `fct_sessions.sql:5-8`.
- **Repository Python Dependencies**:
  - `pyproject.toml:9`: `"dbt-databricks>=1.10,<1.12"`.

### 1.3 Empirical Tool Execution & Offline Compilation Tracing
- **Compilation Failure Without Env Vars**:
  Command: `uv run dbt compile --project-dir dbt_project --profiles-dir dbt_project`  
  Verbatim output:
  `Parsing Error: Env var required but not provided: 'DBT_DATABRICKS_HOST'` (exit code 1).
- **Socket Interception on Dummy Credentials**:
  When dummy Databricks credentials were provided, `urllib3.connectionpool` attempted an HTTPS connection to `127.0.0.1:443` during `dbt-databricks` connection manager initialization, blocking on socket connect.
- **Spark Adapter Constraint**:
  When testing a local Spark profile (`type: spark`), `dbt parse` failed with:
  `Runtime Error: Cannot set database in spark!` because `models/staging/sources.yml` specified `database: "{{ env_var('DBT_CATALOG') }}"`.
- **Existing Local Test Suite Verification**:
  Command: `uv run python -m unittest discover -s tests -p "test_*.py"`  
  Verbatim output: `Ran 34 tests in 0.080s. OK (skipped=13)`.
  Local Spark Gold proof in `spark_jobs/verify_gold.py` uses Jinja2 template rendering to run all 35 models and 19 tests against local PySpark frames.

---

## 2. Logic Chain

1. **Root Cause of Databricks Coupling**:
   - Observations 1.2 and 1.3 show that the dbt project is tied to Databricks primarily at the transport/adapter level (`dbt-databricks`), catalog naming (`database: DBT_CATALOG` in `sources.yml`), and warehouse connectivity.
   - The models themselves are written in standard Spark SQL and Delta Lake MERGE semantics.

2. **Engine Suitability Evaluation**:
   - **Athena (`dbt-athena-community`)**:
     - Athena is backed by Presto/Trino SQL. Translating the project to Athena requires rewriting all Spark-specific functions: `from_utc_timestamp` to `at time zone`, `lateral view explode` to `cross join unnest`, `datediff(end, start)` to 3-arg `date_diff('day', start, end)`, and `to_date` to `date_parse`.
     - Athena cannot write Delta Lake with native incremental merge via dbt; models would need to migrate to Apache Iceberg (`table_type: iceberg`).
     - Crucially, rewriting the SQL dialect to Trino breaks `spark_jobs/verify_gold.py` and `tests/test_gold_scd2_local.py`, which validate the Gold layer locally using PySpark.
   - **AWS Glue Spark (`dbt-glue` / Glue Spark Job)**:
     - AWS Glue 4.0/5.0 runs native Apache Spark with Delta Lake (`--datalake-formats delta`).
     - 100% of the existing SQL models (`from_utc_timestamp`, `datediff`, `lateral view explode`, `unix_timestamp`, `to_date`) run natively on Glue Spark with zero dialect modifications.
     - Delta Lake incremental merge (`incremental_strategy='merge'`) operates identically, preserving idempotency.
     - Perfect alignment with the Silver layer (which is migrated to AWS Glue Spark under Requirement R1).
     - Full compatibility with existing local PySpark test assets (`verify_gold.py` and `test_gold_scd2_local.py`).

3. **Offline Compilation Strategy (R2 & R4)**:
   - Observation 1.3 demonstrated that `dbt-databricks` blocked offline compilation because it initiates network requests to the Databricks SQL Warehouse hostname.
   - By updating `profiles.yml` to an AWS adapter (`glue` or `athena`) with safe fallback defaults (`{{ env_var('AWS_REGION', 'ap-southeast-1') }}`), and removing `database: "{{ env_var('DBT_CATALOG') }}"` from `sources.yml` (using schema-only or Glue database mapping), `dbt parse` and `dbt compile` will complete cleanly offline without network access or live AWS credentials.
   - Full data and grain validation will continue to run offline via `spark_jobs/verify_gold.py`.

---

## 3. Caveats

1. **Live AWS Service Validation**:
   - This survey was performed in read-only local mode. Live execution on AWS Glue Interactive Sessions or Athena workgroups requires live AWS credentials and an active S3 bucket.
2. **Airflow Execution Mechanism**:
   - In Airflow, `build_gold_and_marts_dbt` currently executes `dbt build` via `BashOperator`. If using `dbt-glue`, Glue Interactive Sessions must be permitted; alternatively, a lightweight Glue Spark job or runner can trigger the compiled transformations. The interface (`dbt build --project-dir ... --profiles-dir ...`) is preserved.
3. **Windows PySpark Restrictions**:
   - Direct execution of local Spark tests on Windows host can encounter winutils/Java 24 compatibility skips. The repository provides `docker/silver.Dockerfile` (Debian Linux + OpenJDK 17) for full local execution.

---

## 4. Conclusion

1. The Gold layer and dbt project are ready for migration to AWS.
2. **AWS Glue / Spark** is the most robust target engine: it preserves 100% of the model SQL, maintains Delta Lake merge consistency, avoids dialect regressions, and guarantees offline testability via `spark_jobs/verify_gold.py`.
3. The required changes are well-bounded:
   - Update `pyproject.toml` to replace `dbt-databricks` with `dbt-glue` (and/or provide `dbt-athena-community` profile options).
   - Update `dbt_project.yml` and `profiles.yml` / `profiles.yml.example` to target `ecommerce_aws`.
   - Update `sources.yml` to remove `database: "{{ env_var('DBT_CATALOG') }}"`.
   - Update Airflow's dbt profile in `airflow/dbt_profile/profiles.yml`.
   - Decouple `orchestration/ops_runs.py` from Databricks SQL connector.

Comprehensive findings, function mappings, and model catalog are documented in:  
`c:\Users\ddat2\Downloads\Projects\E-commerce-CDC\.agents\teamwork\explorer_survey_2\analysis.md`

---

## 5. Verification Method

### 5.1 dbt Project Parsing Offline
Run dbt parse under the updated AWS profile to confirm that all 35 models, 2 snapshots, and 19 tests resolve:
```powershell
uv run dbt parse --project-dir dbt_project --profiles-dir dbt_project
```
*Expected Outcome*: Returncode 0; `manifest.json` written to `dbt_project/target/` with 0 parsing errors.

### 5.2 Local Gold & Business Marts Execution Proof
Run the offline PySpark verification harness against local Silver Delta tables:
```powershell
uv run python -m spark_jobs.verify_gold --silver-root <test_silver_dir> --business
```
*Expected Outcome*: All 35 models compile and execute, all dimension keys validated unique and non-null, and all 19 custom business tests pass.

### 5.3 Local SCD2 Fact Join Test
Run the local unit test for SCD2 customer/product point-in-time joins:
```powershell
uv run python -m unittest tests/test_gold_scd2_local.py
```
*Expected Outcome*: All assertions pass.

### 5.4 Invalidation Conditions
- Any change that alters the grain or business definitions of the 6 data marts (`mart_daily_sales`, `mart_product_performance`, `mart_customer_rfm`, `mart_funnel_daily`, `mart_payment_mix_daily`, `mart_order_experience_daily`).
- Any configuration that requires a live Databricks endpoint, token, or SQL warehouse to compile dbt models.
- Any regression that breaks offline model parsing or PySpark validation in `verify_gold.py`.
