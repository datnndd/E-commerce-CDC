# Technical Survey & Analysis: Gold Layer and dbt Project Migration to AWS-Compatible Engine (Requirement R2)

**Author**: `explorer_survey_2`  
**Date**: 2026-11-07  
**Scope**: Gold Layer, dbt Project (`dbt_project/`), Business Data Marts, and Adapter Migration to AWS-Compatible Engine (Athena / AWS Glue).

---

## 1. Executive Summary

This survey provides a comprehensive technical analysis of the existing Gold layer and dbt project transformations (`dbt_project/`), evaluating the requirements to eliminate Databricks SQL Warehouse and `dbt-databricks` dependencies in favor of an AWS-compatible data processing engine (Requirement R2).

The key findings are:
1. **Existing Architecture**: The dbt project contains **35 models** (12 staging views, 2 intermediate views, 8 core dimensions, 7 core facts, 6 business marts), **2 snapshots** (SCD Type 2), and **19 data tests**.
2. **Databricks Coupling**: The project is tightly bound to Databricks through:
   - The `dbt-databricks` adapter declared in `pyproject.toml` and configured in `dbt_project.yml`, `profiles.yml`, and `airflow/dbt_profile/profiles.yml`.
   - Unity Catalog 3-level naming (`{{ env_var('DBT_CATALOG') }}`) in `sources.yml` and profile outputs.
   - `+file_format: delta` in `dbt_project.yml`, snapshots, and all 6 incremental fact models.
   - Spark SQL-specific functions: `from_utc_timestamp`, `lateral view explode(sequence(...))`, `unix_timestamp`, 2-argument `datediff(end, start)`, `to_date`, `date_add`, and `min_by`.
   - Offline compilation blocker: `dbt-databricks` attempts a network TCP handshake to the Databricks SQL Warehouse hostname during adapter initialization, causing offline commands (`dbt compile` / `dbt parse`) to hang or fail without a live Databricks endpoint.
3. **Target AWS Engine Evaluation**:
   - **Option A: AWS Glue Spark (`dbt-glue` or AWS Glue Spark job)**: Direct 100% dialect and file-format parity with existing Spark SQL models and Delta Lake tables. Eliminates the need to rewrite complex date/spine/time-zone SQL, maintains exact compatibility with existing offline test runner `spark_jobs/verify_gold.py` and `tests/test_gold_scd2_local.py`, and aligns with the Silver Glue Spark job (Requirement R1).
   - **Option B: Amazon Athena (`dbt-athena-community`)**: Pure serverless SQL engine using Trino dialect. Requires converting table format from Delta to Apache Iceberg (`table_type: iceberg`), translating Spark-specific functions (`from_utc_timestamp` to `at time zone`, `datediff` to 3-arg `date_diff`, `lateral view explode` to `cross join unnest`), and reconciling with PySpark-based offline test runners.
4. **Offline Compilation & Test Strategy**:
   - By switching the dbt profile to an AWS adapter with dummy AWS credentials (`aws_access_key_id: "testing"`, `region_name: "us-east-1"`) and configuring `sources.yml` cleanly, `dbt parse` and `dbt compile` can execute offline without connecting to live AWS endpoints.
   - Offline business logic verification is already reinforced by `spark_jobs/verify_gold.py`, which compiles Jinja models locally and validates all 35 models and 19 business tests against local PySpark frames.

---

## 2. Complete Inventory of Existing dbt Project

### 2.1 Directory Structure
```
dbt_project/
├── .gitkeep
├── dbt_project.yml
├── profiles.yml
├── profiles.yml.example
├── models/
│   ├── staging/
│   │   ├── sources.yml
│   │   ├── stg_campaigns.sql
│   │   ├── stg_category_translation.sql
│   │   ├── stg_clickstream_events.sql
│   │   ├── stg_customers.sql
│   │   ├── stg_exchange_rates.sql
│   │   ├── stg_geolocation.sql
│   │   ├── stg_order_items.sql
│   │   ├── stg_orders.sql
│   │   ├── stg_payments.sql
│   │   ├── stg_products.sql
│   │   ├── stg_reviews.sql
│   │   └── stg_sellers.sql
│   ├── intermediate/
│   │   ├── int_customer_orders.sql
│   │   └── int_orders_enriched.sql
│   └── marts/
│       ├── core/
│       │   ├── core.yml
│       │   ├── behavior/
│       │   │   ├── fct_clickstream_events.sql
│       │   │   └── fct_sessions.sql
│       │   ├── commerce/
│       │   │   ├── fct_order_items.sql
│       │   │   ├── fct_orders.sql
│       │   │   ├── fct_payments.sql
│       │   │   └── fct_reviews.sql
│       │   ├── dimensions/
│       │   │   ├── dim_campaign.sql
│       │   │   ├── dim_category.sql
│       │   │   ├── dim_channel.sql
│       │   │   ├── dim_customer.sql
│       │   │   ├── dim_date.sql
│       │   │   ├── dim_geography.sql
│       │   │   ├── dim_product.sql
│       │   │   └── dim_seller.sql
│       │   └── reference/
│       │       └── fct_exchange_rates.sql
│       └── business/
│           ├── business.yml
│           ├── behavior/
│           │   └── mart_funnel_daily.sql
│           ├── customer/
│           │   └── mart_customer_rfm.sql
│           ├── experience/
│           │   └── mart_order_experience_daily.sql
│           ├── payment/
│           │   └── mart_payment_mix_daily.sql
│           └── sales/
│               ├── mart_daily_sales.sql
│               └── mart_product_performance.sql
├── snapshots/
│   ├── customers_snapshot.sql
│   └── products_snapshot.sql
└── tests/
    ├── assert_core_grains.sql
    ├── assert_core_reconciliation.sql
    ├── assert_daily_sales_grain.sql
    ├── assert_experience_totals.sql
    ├── assert_funnel_bounds.sql
    ├── assert_funnel_daily_grain.sql
    ├── assert_funnel_totals.sql
    ├── assert_item_revenue.sql
    ├── assert_nonnegative_items.sql
    ├── assert_order_experience_grain.sql
    ├── assert_payment_mix_grain.sql
    ├── assert_payment_totals.sql
    ├── assert_product_performance_grain.sql
    ├── assert_product_totals.sql
    ├── assert_rfm_scores.sql
    ├── assert_rfm_ties.sql
    ├── assert_rfm_totals.sql
    ├── assert_sales_totals.sql
    └── assert_scd2_windows.sql
```

### 2.2 Model & Transformation Catalog

| Layer | Model Name | Materialization | Strategy / Format | Grain / Business Purpose |
|---|---|---|---|---|
| **Staging** | `stg_customers` | View | Source view | 1 row per customer record from Silver `slv_customers` |
| **Staging** | `stg_geolocation` | View | Source view | Zip prefix and lat/long coordinates from `slv_geolocation` |
| **Staging** | `stg_category_translation` | View | Source view | Portuguese to English category mappings |
| **Staging** | `stg_products` | View | Source view | 1 row per product record from `slv_products` |
| **Staging** | `stg_sellers` | View | Source view | 1 row per seller from `slv_sellers` |
| **Staging** | `stg_orders` | View | Source view | 1 row per order from `slv_orders` |
| **Staging** | `stg_order_items` | View | Source view | 1 row per order item from `slv_order_items` |
| **Staging** | `stg_payments` | View | Source view | 1 row per payment installment from `slv_payments` |
| **Staging** | `stg_reviews` | View | Source view | 1 row per review from `slv_reviews` |
| **Staging** | `stg_clickstream_events` | View | Source view | Raw events from `slv_clickstream_events` |
| **Staging** | `stg_exchange_rates` | View | Source view | Daily currency rates from `slv_exchange_rates` |
| **Staging** | `stg_campaigns` | View | Source view | Marketing campaign metadata from `slv_campaigns` |
| **Snapshots** | `customers_snapshot` | Snapshot | SCD2 (`timestamp`) | Historical customer location changes tracked via `updated_at` |
| **Snapshots** | `products_snapshot` | Snapshot | SCD2 (`timestamp`) | Historical product attribute changes tracked via `updated_at` |
| **Intermediate** | `int_orders_enriched` | View | Joined aggregation | Enriches orders with items subtotal, freight, payment totals |
| **Intermediate** | `int_customer_orders` | View | Aggregated customer | Lifetime delivered order count, revenue, and last purchase date |
| **Core Dim** | `dim_campaign` | Table | Delta (`gold`) | Unique campaign dimension (`campaign_key`) |
| **Core Dim** | `dim_category` | Table | Delta (`gold`) | Category dimension coalesced across products & translation |
| **Core Dim** | `dim_channel` | Table | Delta (`gold`) | Marketing channels from clickstream and campaigns |
| **Core Dim** | `dim_customer` | Table | Delta (`gold`) | Customer dimension with SCD2 `customer_key` (`dbt_scd_id`) |
| **Core Dim** | `dim_date` | Table | Delta (`gold`) | Calendar date spine (2016–2018) in Sao Paulo timezone |
| **Core Dim** | `dim_geography` | Table | Delta (`gold`) | Distinct zip prefix + state with centroid lat/lng |
| **Core Dim** | `dim_product` | Table | Delta (`gold`) | Product dimension with SCD2 `product_key` (`dbt_scd_id`) |
| **Core Dim** | `dim_seller` | Table | Delta (`gold`) | Seller dimension with `seller_key` and geography key |
| **Core Fact** | `fct_orders` | Incremental | Merge on `order_id` | 1 row per order, point-in-time join to `dim_customer` SCD2 |
| **Core Fact** | `fct_order_items` | Incremental | Merge `[order_id, order_item_id]` | 1 row per item, point-in-time join to `dim_product` SCD2 |
| **Core Fact** | `fct_payments` | Incremental | Merge `[order_id, payment_seq]` | 1 row per payment sequence |
| **Core Fact** | `fct_reviews` | Incremental | Merge `review_row_id` | Review scores and review response duration in hours |
| **Core Fact** | `fct_clickstream_events` | Incremental | Merge `event_id` | Clickstream events with SCD2 joins to customer & product |
| **Core Fact** | `fct_sessions` | Table | Delta (`gold`) | Aggregated sessions (`min_by` for attribution, funnel flags) |
| **Core Fact** | `fct_exchange_rates` | Incremental | Merge `[date, base, quote]` | Daily FX conversion rates |
| **Business Mart** | `mart_daily_sales` | Table | Delta (`gold`) | Grain: `[purchase_date_key, customer_state]`; revenue, AOV |
| **Business Mart** | `mart_product_performance`| Table | Delta (`gold`) | Grain: `[product_id, seller_id, category_key]`; revenue |
| **Business Mart** | `mart_customer_rfm` | Table | Delta (`gold`) | Grain: `customer_unique_id`; R/F/M percentile scores (1-5) |
| **Business Mart** | `mart_funnel_daily` | Table | Delta (`gold`) | Grain: `[session_date_key, device, channel_key]`; rates |
| **Business Mart** | `mart_payment_mix_daily` | Table | Delta (`gold`) | Grain: `[order_purchase_date_key, payment_type]`; mix |
| **Business Mart** | `mart_order_experience_daily`| Table | Delta (`gold`) | Grain: `[purchase_date_key, customer_state]`; delivery & CSAT |

---

## 3. Detailed Identification of Databricks Dependencies

### 3.1 Configuration & Profile References
1. **`dbt_project.yml`**:
   - Line 4: `profile: ecommerce_databricks`
   - Lines 23, 27: `+file_format: delta` under `marts.core` and `marts.business`.
2. **`dbt_project/profiles.yml` & `profiles.yml.example`**:
   - Root profile: `ecommerce_databricks`
   - `type: databricks`
   - `auth_type: oauth`
   - `host: "{{ env_var('DBT_DATABRICKS_HOST') }}"`
   - `http_path: "{{ env_var('DBT_DATABRICKS_HTTP_PATH') }}"`
   - `database: "{{ env_var('DBT_CATALOG') }}"`
   - `schema: "{{ env_var('DBT_TARGET_SCHEMA', 'analytics_dev') }}"`
3. **`airflow/dbt_profile/profiles.yml`**:
   - `ecommerce_databricks` configured with `type: databricks`, `auth_type: pat`, `token: "{{ env_var('DATABRICKS_TOKEN') }}"`.
4. **`models/staging/sources.yml`**:
   - `database: "{{ env_var('DBT_CATALOG') }}"` (references Databricks Unity Catalog 3-level naming).
   - `schema: "{{ env_var('DBT_SILVER_SCHEMA', 'silver') }}"`.
5. **Snapshots (`snapshots/*.sql`)**:
   - Both `customers_snapshot.sql` and `products_snapshot.sql` specify `file_format='delta'`.
6. **Incremental Model Headers**:
   - `fct_orders.sql`, `fct_order_items.sql`, `fct_payments.sql`, `fct_reviews.sql`, `fct_clickstream_events.sql`, `fct_exchange_rates.sql` all specify `file_format='delta'` in their `config(...)`.

### 3.2 SQL Dialect & Function References (Spark SQL vs. Trino/Athena)

The following dialect-specific functions are present across the dbt models:

| Function / Syntax | File(s) and Line Number(s) | Databricks / Spark SQL Behavior | AWS Athena (Trino) Equivalent | AWS Glue Spark Behavior |
|---|---|---|---|---|
| `from_utc_timestamp(ts, 'America/Sao_Paulo')` | `dim_date.sql:2-7`<br>`fct_orders.sql:7,8`<br>`fct_reviews.sql:4`<br>`fct_clickstream_events.sql:3`<br>`fct_sessions.sql:4`<br>`mart_order_experience_daily.sql:9,10,12,13` | Converts UTC timestamp to Sao Paulo timezone | `ts at time zone 'America/Sao_Paulo'` | Native (built-in function) |
| `lateral view explode(sequence(...)) d as day` | `dim_date.sql:15` | Hive/Spark table-generating explode syntax | `CROSS JOIN UNNEST(sequence(...)) as t(day)` | Native (built-in syntax) |
| `to_date(...)` | `dim_date.sql`<br>`int_customer_orders.sql:4`<br>`mart_customer_rfm.sql:5`<br>`mart_order_experience_daily.sql:9,10,12,13` | Truncates timestamp or parses date string | `date(ts)` or `date_parse(str, '%Y%m%d')` | Native (built-in function) |
| `datediff(end, start)` (2 args) | `mart_customer_rfm.sql:13`<br>`mart_order_experience_daily.sql:8-11`<br>`tests/assert_rfm_scores.sql:6` | Returns integer days between `end - start` | `date_diff('day', start, end)` (3 args, reverse order) | Native (built-in function) |
| `date_add(date, 1)` | `mart_customer_rfm.sql:10` | Adds integer days to date | `date_add('day', 1, date)` (3 args) | Native (built-in function) |
| `unix_timestamp(ts)` | `fct_reviews.sql:6` | Converts timestamp to epoch seconds | `to_unixtime(ts)` or `date_diff('second', ...)` | Native (built-in function) |
| `min_by(col, val)` | `fct_sessions.sql:5-8` | Returns `col` associated with min `val` | `min_by(col, val)` | Native (built-in function) |
| `timestamp('1900-01-01')` | All 6 incremental `fct_*.sql` | Spark timestamp literal constructor | `timestamp '1900-01-01 00:00:00'` or `cast('1900-01-01' as timestamp)` | Native (built-in function) |
| `date_format(day, 'yyyyMMdd')` | `dim_date.sql:12`, `fct_orders.sql:7`, etc. | Formats date using Java date pattern | `date_format(day, '%Y%m%d')` | Native (built-in function) |

### 3.3 Root Cause of Offline Compilation Blockers

Our empirical trace of `dbt parse` and `dbt compile` revealed:
1. When `type: databricks` is active in `profiles.yml`, dbt instantiates `DatabricksConnectionManager`.
2. Even during offline commands (`dbt parse`, `dbt compile`), the connection manager attempts to establish a HTTPS connection to port 443 of `DBT_DATABRICKS_HOST` to validate OAuth tokens or inspect the SQL warehouse state.
3. If dummy or unreachable values are set (e.g. `127.0.0.1` or `dummy.cloud.databricks.com`), `urllib3.connectionpool` attempts TCP connect and blocks until OS socket timeout (causing hangs of 20–60 seconds per attempt).
4. Furthermore, `dbt-databricks` requires mandatory environment variables (`DBT_DATABRICKS_HOST`, `DBT_DATABRICKS_HTTP_PATH`, `DBT_CATALOG`) that fail with `Parsing Error: Env var required but not provided` if unset.

---

## 4. Assessment of Target AWS-Compatible Engines

Requirement R2 specifies migrating the Gold layer and data marts transformation pipeline to an **AWS-compatible data processing engine (such as AWS Athena or AWS Glue)**. Below is an exhaustive comparison of the two viable paths:

### 4.1 Option 1: AWS Glue Spark (`dbt-glue` or Glue Spark Gold Execution)

#### Architecture
- **Engine**: AWS Glue Serverless Spark (Glue 4.0/5.0 with Spark 3.3/3.5).
- **dbt Adapter**: `dbt-glue` (or running compiled SQL via a dedicated Glue Gold Spark job).
- **Catalog**: AWS Glue Data Catalog (`database: <glue_database>`).
- **Table Format**: Delta Lake (`file_format='delta'`) on S3, enabled in Glue via `--datalake-formats delta`.

#### Advantages
1. **Zero SQL Dialect Changes**: All 35 models and 19 tests use standard Spark SQL syntax (`from_utc_timestamp`, `lateral view explode`, `unix_timestamp`, `datediff(end, start)`, `to_date`). Not a single line of model SQL needs to be altered.
2. **Delta Lake & Incremental Merge Parity**: Delta Lake MERGE (`incremental_strategy='merge'`) operates identically to Databricks, preserving ACID guarantees and idempotency.
3. **Seamless Offline Test Verification**: The existing repository test runner `spark_jobs/verify_gold.py` and unit test `tests/test_gold_scd2_local.py` run PySpark locally. Because Glue uses the exact same Spark SQL semantics, local tests validate 100% of production behavior without dialect divergence.
4. **Architectural Uniformity**: Requirement R1 already migrates the Silver layer to an AWS Glue Spark job. Using Glue for Gold maintains a single unified engine and shared IAM role across Silver and Gold.

#### Trade-offs & Mitigations
- In `dbt-glue`, Glue Interactive Sessions must be configured if running `dbt run` directly from Airflow, which can incur session spin-up latency (1-2 minutes).
- **Mitigation**: In Airflow, Gold can be executed either via `dbt-glue` or triggered via an AWS Glue Spark job that runs the verified Gold transformations sequentially, matching the operational pattern of Silver.

---

### 4.2 Option 2: Amazon Athena (`dbt-athena-community`)

#### Architecture
- **Engine**: Amazon Athena (Presto/Trino SQL query engine).
- **dbt Adapter**: `dbt-athena-community` (pip package: `dbt-athena-community>=1.11.0`).
- **Catalog**: AWS Glue Data Catalog via `database: awsdatacatalog`, `schema: <athena_database>`.
- **Table Format**: Apache Iceberg (`table_type='iceberg'`) or Parquet (`format='parquet'`).

#### Advantages
1. **Serverless & Instant**: No cluster or session warmup time; executes individual queries directly against S3 via Athena query execution endpoints.
2. **Standard dbt CLI Flow**: Works cleanly with standard Airflow `BashOperator` running `dbt build --project-dir ... --profiles-dir ...`.

#### Trade-offs & Migration Costs
1. **Delta Lake Incompatibility for Writes**: Athena cannot natively write or MERGE into Delta Lake tables directly via dbt. All core facts and marts must be converted from `file_format='delta'` to `table_type='iceberg'` or `format='parquet'`.
2. **Extensive SQL Dialect Rewriting**:
   - `from_utc_timestamp(...)` must be rewritten as `ts at time zone 'America/Sao_Paulo'`.
   - `lateral view explode(sequence(...))` in `dim_date.sql` must be rewritten as `CROSS JOIN UNNEST(sequence(...)) as t(day)`.
   - `datediff(x.as_of_date, a.last_purchase_date)` must be changed to `date_diff('day', a.last_purchase_date, x.as_of_date)`.
   - `unix_timestamp(ts)` must be changed to `to_unixtime(ts)`.
   - `to_date(...)` must be changed to `date(...)` or `date_parse(...)`.
3. **Local Offline Test Divergence**: Rewriting models to Trino syntax breaks PySpark execution in `spark_jobs/verify_gold.py` and `tests/test_gold_scd2_local.py`, requiring extensive mocking or Trino cross-compilation macros to keep local tests working.

---

### 4.3 Recommended Strategy: Glue/Spark Parity with Cross-Compatible dbt Profile

To fulfill all requirements of R2 and R4 while maintaining 100% offline test suite integrity:
1. **Primary Engine**: **AWS Glue / Spark**. This preserves 100% of the tested business logic in `dim_date`, `mart_customer_rfm`, `mart_order_experience_daily`, and the 19 grain/reconciliation tests without dialect regressions.
2. **Adapter & Profile**: Configure an AWS-compatible profile in `dbt_project/profiles.yml` and `airflow/dbt_profile/profiles.yml`:
   - Replace `type: databricks` with `type: glue` (or Athena-compatible mock for offline compile).
   - In `profiles.yml.example`, supply complete configuration parameters for AWS Glue and Athena.
   - Remove all Databricks host, HTTP path, and PAT token variables.
3. **Offline Compilation Proof**:
   - Provide safe dummy defaults in `env_var(...)` calls within `profiles.yml` (e.g. `{{ env_var('AWS_REGION', 'ap-southeast-1') }}`).
   - Ensure `sources.yml` references catalog schemas without triggering `Cannot set database` errors.
   - Verify `dbt parse` and `dbt compile` run with zero network activity and zero errors.
4. **Verification Script Continuity**:
   - Maintain and update `spark_jobs/verify_gold.py` so that local PySpark tests execute against S3/local Silver Delta inputs and prove that all 35 models and 19 data tests pass without cloud credentials.

---

## 5. Detailed File-by-File Migration Plan

### 5.1 `pyproject.toml`
- Remove `"dbt-databricks>=1.10,<1.12"`.
- Replace with AWS-compatible adapter:
  ```toml
  # For AWS Glue / Spark:
  "dbt-glue>=1.12,<2",
  # Or for Athena:
  # "dbt-athena-community>=1.11,<2",
  ```

### 5.2 `dbt_project/dbt_project.yml`
- Line 4: Change `profile: ecommerce_databricks` to `profile: ecommerce_aws`.
- Lines 23, 27: Ensure `file_format: delta` is maintained for Spark/Glue Delta or updated if targeting another format.

### 5.3 `dbt_project/profiles.yml` and `profiles.yml.example`
Replace Databricks credentials with AWS configuration:
```yaml
ecommerce_aws:
  target: dev
  outputs:
    dev:
      type: glue
      role_arn: "{{ env_var('GLUE_ROLE_ARN', 'arn:aws:iam::123456789012:role/GlueServiceRole') }}"
      region: "{{ env_var('AWS_REGION', 'ap-southeast-1') }}"
      database: "{{ env_var('DBT_TARGET_SCHEMA', 'analytics_dev') }}"
      location: "s3://{{ env_var('S3_BUCKET', 'ecommerce-cdc-lakehouse') }}/gold/"
      workers: 2
      worker_type: "G.1X"
      idle_timeout: 10
      glue_version: "4.0"
      threads: 2
```
*Note*: For Athena alternative, provide the matching `type: athena` output target in `profiles.yml.example`.

### 5.4 `airflow/dbt_profile/profiles.yml`
Update Airflow's production dbt profile to mirror `ecommerce_aws`, reading standard AWS IAM environment variables or instance profiles without Databricks tokens.

### 5.5 `dbt_project/models/staging/sources.yml`
Update Silver source definition to eliminate Databricks Unity Catalog coupling:
```yaml
version: 2

sources:
  - name: silver
    schema: "{{ env_var('DBT_SILVER_SCHEMA', 'silver') }}"
    tables:
      - name: slv_customers
      - name: slv_geolocation
      - name: slv_category_translation
      - name: slv_products
      - name: slv_sellers
      - name: slv_orders
      - name: slv_order_items
      - name: slv_payments
      - name: slv_reviews
      - name: slv_clickstream_events
      - name: slv_exchange_rates
      - name: slv_campaigns
```
*(Removes `database: "{{ env_var('DBT_CATALOG') }}"` which triggered the Databricks Unity Catalog 3-level requirement).*

### 5.6 Environment & Docker Decoupling
- In `.env.example`: Remove `DBT_DATABRICKS_HOST`, `DBT_DATABRICKS_HTTP_PATH`, `DBT_CATALOG`, `DATABRICKS_TOKEN`, and `DATABRICKS_SILVER_JOB_ID`. Add `GLUE_SILVER_JOB_NAME` and `GLUE_ROLE_ARN`.
- In `compose.yaml`: Remove Databricks environment variables and `AIRFLOW_CONN_DATABRICKS_DEFAULT`. Add `AIRFLOW_CONN_AWS_DEFAULT`.
- In `docker/airflow.Dockerfile`: Replace `RUN pip install --no-cache-dir apache-airflow-providers-databricks` with `RUN pip install --no-cache-dir apache-airflow-providers-amazon`.
- In `orchestration/ops_runs.py`: Decouple `connection()` and `ops_table()` from `from databricks import sql` so pipeline metadata logging writes to a local SQLite or S3/JSON file when Databricks is disabled.

---

## 6. Verification and Validation Plan

1. **dbt Parsing & Compilation Offline**:
   Verify dbt syntax, model DAG, and macros parse cleanly without network connection:
   ```powershell
   uv run dbt parse --project-dir dbt_project --profiles-dir dbt_project
   ```
2. **Local PySpark Gold Verification**:
   Execute all 35 models and 19 data tests against local Silver test fixtures:
   ```powershell
   uv run python -m spark_jobs.verify_gold --silver-root <test_silver_dir> --business
   ```
3. **Unit Tests for SCD2 Logic**:
   Run SCD2 fact join tests:
   ```powershell
   uv run python -m unittest tests/test_gold_scd2_local.py
   ```
4. **Code Syntax and Compilation Integrity**:
   Verify zero syntax errors:
   ```powershell
   python -m compileall spark_jobs/ orchestration/ airflow/
   ```
