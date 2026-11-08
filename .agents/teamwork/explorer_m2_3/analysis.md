# Milestone M2 Gold Layer & dbt Models Compatibility Analysis

**Author**: `explorer_m2_3`  
**Date**: 2026-11-07  
**Scope**: 35 dbt models, 2 snapshots, 19 custom tests, `spark_jobs/verify_gold.py`, `tests/test_gold_scd2_local.py`, and AWS Glue Spark compatibility.

---

## 1. Executive Summary

This investigation performed a comprehensive audit of all 35 dbt models, 2 snapshots, 19 custom tests, and local verification harnesses in the repository. The core findings are:
- **Zero Databricks-Proprietary SQL Functions**: Every SQL function across all 35 models and 19 tests (`from_utc_timestamp`, `date_format`, `to_date`, `date_add`, `datediff`, `unix_timestamp`, `sequence`, `explode`, `min_by`, `percent_rank`, `row_number`, `lead`, `greatest`, `least`, `sha2`) is standard Apache Spark SQL built-in, natively supported on AWS Glue Spark (Glue 4.0 / Spark 3.3 and Glue 5.0 / Spark 3.5).
- **Zero Macro Dependencies**: There are no custom macros in `dbt_project/macros` and no external dbt package dependencies (such as `dbt_utils` or `dbt_expectations`). Models use only standard Jinja2 constructs (`ref`, `source`, `config`, `is_incremental`, `this`).
- **Decoupling Requirements**: The only Databricks-specific anchors are:
  1. `models/staging/sources.yml`: `database: "{{ env_var('DBT_CATALOG') }}"` (forces 3-tier Unity Catalog namespace).
  2. `dbt_project.yml`: `profile: ecommerce_databricks`.
  3. `profiles.yml`: `type: databricks` with `auth_type: oauth` (which attempts interactive browser login and hangs offline).
- **Local PySpark Execution Harness**: `spark_jobs/verify_gold.py` and `tests/test_gold_scd2_local.py` provide a robust, offline verification harness. They mock dbt's Jinja environment and execute the exact SQL of all 35 models and 19 tests in local PySpark with Delta Lake extensions, proving full semantic compatibility with AWS Glue Spark without live cloud credentials.

---

## 2. Complete Asset Inventory

### 2.1 Staging Models (12 models)
All staging models are materialized as views (`+materialized: view`, `+schema: staging` via `dbt_project.yml`) and perform 1-to-1 reads from `source('silver', ...)`:
| Model File | Source Table | Description |
|---|---|---|
| `stg_customers.sql` | `slv_customers` | Customer entity records |
| `stg_geolocation.sql` | `slv_geolocation` | Zip code prefix lat/lng and cities |
| `stg_category_translation.sql` | `slv_category_translation` | Portuguese to English category map |
| `stg_products.sql` | `slv_products` | Product dimensions and weights |
| `stg_sellers.sql` | `slv_sellers` | Seller zip code and state location |
| `stg_orders.sql` | `slv_orders` | Order headers and lifecycle timestamps |
| `stg_order_items.sql` | `slv_order_items` | Order item prices and freight values |
| `stg_payments.sql` | `slv_payments` | Order payment methods and installments |
| `stg_reviews.sql` | `slv_reviews` | Order review scores and response times |
| `stg_clickstream_events.sql` | `slv_clickstream_events` | User event sessions and pageviews |
| `stg_exchange_rates.sql` | `slv_exchange_rates` | Currency conversion rates |
| `stg_campaigns.sql` | `slv_campaigns` | Marketing campaign discounts and channels |

### 2.2 Intermediate Models (2 models)
Materialized as views (`+materialized: view`, `+schema: intermediate`):
| Model File | Upstream Refs | Description & Transformations |
|---|---|---|
| `int_orders_enriched.sql` | `stg_orders`, `stg_order_items`, `stg_payments` | Aggregates item subtotals, freight, and payments per order; computes `source_ingested_at = greatest(o.ingested_at, i.items_ingested_at, p.payments_ingested_at)`. |
| `int_customer_orders.sql` | `int_orders_enriched`, `stg_customers` | Aggregates delivered orders per `customer_unique_id`: order count, revenue, and `max(to_date(order_purchase_timestamp))`. |

### 2.3 Marts Core Dimensions (8 models)
Materialized as Delta tables (`+materialized: table`, `+file_format: delta`, `+schema: gold`):
| Model File | Upstream Refs | Key Features & SQL Functions |
|---|---|---|
| `dim_date.sql` | `stg_orders`, `stg_reviews`, `stg_clickstream_events`, `stg_exchange_rates` | `bounds lateral view explode(sequence(first_day, last_day, interval 1 day)) d as day`. Timezone: `America/Sao_Paulo`. Generates `date_key = cast(date_format(day, 'yyyyMMdd') as int)`. |
| `dim_customer.sql` | `customers_snapshot` | Reads snapshot, surrogate key `customer_key = dbt_scd_id`, `geography_key = concat(zip, '\|', upper(state))`, `row_number() over (partition by customer_id order by dbt_valid_from) as version_number`. |
| `dim_product.sql` | `products_snapshot` | Reads snapshot, surrogate key `product_key = dbt_scd_id`, `category_key = coalesce(product_category_name, '__unknown__')`, `row_number() over (partition by product_id order by dbt_valid_from) as version_number`. |
| `dim_seller.sql` | `stg_sellers` | `seller_key = seller_id`, `geography_key = concat(zip, '\|', upper(state))`. |
| `dim_category.sql` | `products_snapshot`, `stg_category_translation` | Unions distinct categories with `'__unknown__'`, left joins English translation. |
| `dim_geography.sql` | `customers_snapshot`, `stg_sellers`, `stg_geolocation` | Unions customer and seller locations, left joins aggregated geolocation lat/lng and city counts. |
| `dim_channel.sql` | `stg_clickstream_events`, `stg_campaigns` | Unions `utm_source` channels with `'unknown'`. |
| `dim_campaign.sql` | `stg_campaigns` | Direct map of campaign ID, discount percentage, and synthetic flag. |

### 2.4 Marts Core Facts (7 models)
Configured with Delta format and incremental MERGE strategies:
| Model File | Incremental Strategy & Unique Key | Key Joins & Features |
|---|---|---|
| `fct_orders.sql` | `incremental_strategy='merge'`, `unique_key='order_id'` | Joins `int_orders_enriched` and `dim_customer` via SCD2 temporal window (valid_from to valid_to with fallback to version 1 for predating orders). |
| `fct_order_items.sql` | `incremental_strategy='merge'`, `unique_key=['order_id', 'order_item_id']` | Joins `stg_order_items`, `fct_orders`, `dim_product` (SCD2 join), `dim_seller`. |
| `fct_payments.sql` | `incremental_strategy='merge'`, `unique_key=['order_id', 'payment_sequential']` | Joins `stg_payments` and `fct_orders`. |
| `fct_reviews.sql` | `incremental_strategy='merge'`, `unique_key='review_row_id'` | Joins `stg_reviews` and `fct_orders`. Computes `response_hours = (unix_timestamp(answer) - unix_timestamp(creation)) / 3600.0`. |
| `fct_clickstream_events.sql` | `incremental_strategy='merge'`, `unique_key='event_id'` | Joins `stg_clickstream_events`, `dim_customer` (SCD2), and `dim_product` (SCD2). |
| `fct_sessions.sql` | Table (`group by session_id`) | Aggregates `fct_clickstream_events` using `min_by(customer_id, event_timestamp)`, `min_by(customer_key, event_timestamp)`, `min_by(device, event_timestamp)`, `min_by(channel_key, event_timestamp)` and max funnel flags. |
| `fct_exchange_rates.sql` | `incremental_strategy='merge'`, `unique_key=['date', 'base', 'quote']` | Incremental read of currency rates. |

### 2.5 Marts Business (6 models)
Materialized as Delta tables (`+materialized: table`, `+file_format: delta`, `+schema: gold`):
| Model File | Grain | Transformations & Metrics |
|---|---|---|
| `mart_daily_sales.sql` | `purchase_date_key`, `customer_state` | Delivered vs canceled order counts, delivered item revenue, delivered freight, AOV, cancellation rate. |
| `mart_product_performance.sql` | `product_id`, `seller_id`, `category_key` | Delivered item counts, order counts, delivered item revenue, delivered freight, average item price. |
| `mart_customer_rfm.sql` | `customer_unique_id` | Dynamic `as_of_date = date_add(max(last_purchase_date), 1)`. `recency_days = datediff(as_of_date, last_purchase_date)`. Quantile scoring 1-5 via `percent_rank() over (...)`. Segment assignments: Champions, Loyal, Potential, At Risk, Churned. |
| `mart_funnel_daily.sql` | `session_date_key`, `device`, `channel_key` | Funnel dropoff stages: views, carts, checkouts, purchases, view-to-cart rate, cart-to-checkout rate, checkout-to-purchase rate. |
| `mart_payment_mix_daily.sql` | `order_purchase_date_key`, `payment_type` | Payment record counts, distinct paid orders, total value, average installments, installment payment count. |
| `mart_order_experience_daily.sql` | `purchase_date_key`, `customer_state` | Pre-aggregates reviews per order. `delivery_days = datediff(...)`. `late_delivery` flag, late delivery rate, average delivery days, average review score. |

### 2.6 Snapshots (2 snapshots)
Materialized in `snapshots` schema as Delta tables:
| Snapshot File | Target Entity | Strategy & Keys |
|---|---|---|
| `customers_snapshot.sql` | `stg_customers` | `strategy='timestamp'`, `unique_key='customer_id'`, `updated_at='updated_at'`, `file_format='delta'` |
| `products_snapshot.sql` | `stg_products` | `strategy='timestamp'`, `unique_key='product_id'`, `updated_at='updated_at'`, `file_format='delta'` |

### 2.7 Custom Data Tests (19 tests)
Located in `dbt_project/tests/`:
1. `assert_core_grains.sql`: Composite grain uniqueness for `fct_order_items`, `fct_payments`, and `fct_exchange_rates`.
2. `assert_core_reconciliation.sql`: Row count parity between 6 staging tables and facts.
3. `assert_daily_sales_grain.sql`: Grain uniqueness on `(purchase_date_key, customer_state)` in `mart_daily_sales`.
4. `assert_experience_totals.sql`: Order count parity between `mart_order_experience_daily` and delivered `fct_orders`.
5. `assert_funnel_bounds.sql`: Validates funnel funneling logic (`view_sessions <= session_count`, etc.) and rate boundaries (`0 <= rate <= 1`).
6. `assert_funnel_daily_grain.sql`: Grain uniqueness on `(session_date_key, device, channel_key)` in `mart_funnel_daily`.
7. `assert_funnel_totals.sql`: Total session count parity between `mart_funnel_daily` and `fct_sessions`.
8. `assert_item_revenue.sql`: Three-way revenue reconciliation across `stg_order_items`, `fct_order_items`, and `fct_orders`.
9. `assert_nonnegative_items.sql`: Validates non-negative price and freight in `fct_order_items`.
10. `assert_order_experience_grain.sql`: Grain uniqueness on `(purchase_date_key, customer_state)` in `mart_order_experience_daily`.
11. `assert_payment_mix_grain.sql`: Grain uniqueness on `(order_purchase_date_key, payment_type)` in `mart_payment_mix_daily`.
12. `assert_payment_totals.sql`: Total payment counts and BRL values between `mart_payment_mix_daily` and `fct_payments`.
13. `assert_product_performance_grain.sql`: Grain uniqueness on `(product_id, seller_id, category_key)` in `mart_product_performance`.
14. `assert_product_totals.sql`: Item counts and revenue parity between `mart_product_performance` and delivered `fct_order_items`.
15. `assert_rfm_scores.sql`: Validates score ranges [1, 5], recency days calculation, and segment case statements in `mart_customer_rfm`.
16. `assert_rfm_ties.sql`: Validates deterministic score assignment across tied metrics in `mart_customer_rfm`.
17. `assert_rfm_totals.sql`: Delivered order count and revenue parity between `mart_customer_rfm` and `fct_orders`.
18. `assert_sales_totals.sql`: Order count and revenue parity between `mart_daily_sales` and delivered `fct_orders`.
19. `assert_scd2_windows.sql`: Validates SCD2 validity window constraints (`valid_to > valid_from`, no overlapping or unclosed prior windows) in `dim_customer` and `dim_product`.

---

## 3. SQL Dialect & Compatibility with AWS Glue Spark

A line-by-line audit of all SQL expressions across the 35 models and 19 tests confirms:
1. **Spark SQL Standard Compliance**:
   - `from_utc_timestamp(col, 'America/Sao_Paulo')`: Standard Spark SQL timezone transformation function, native to Spark 1.5+.
   - `date_format(col, 'yyyyMMdd')`: Standard Spark SQL date formatting.
   - `to_date(col, 'yyyyMMdd')`: Standard Spark SQL date casting with format string.
   - `datediff(endDate, startDate)`: Spark SQL 2-argument signature. Correctly formatted in all models and tests.
   - `date_add(startDate, numDays)`: Standard Spark SQL date arithmetic.
   - `sequence(start, stop, step)` + `lateral view explode(...)`: Standard Spark SQL generator functions (Spark 2.4+).
   - `min_by(valueCol, orderCol)`: Standard Spark SQL aggregation function (Spark 3.0+). Supported in AWS Glue 4.0 (Spark 3.3.0) and AWS Glue 5.0 (Spark 3.5.2).
   - `percent_rank() over (...)`, `row_number() over (...)`, `lead(...) over (...)`: Standard Spark SQL analytic window functions.
   - `greatest(...)`, `least(...)`, `coalesce(...)`, `nullif(...)`: Standard Spark SQL scalar functions.
   - `sha2(concat(...), 256)`: Standard Spark SQL hashing function.
2. **Delta Lake On Glue Compatibility**:
   - `file_format: delta`: Supported in AWS Glue Spark jobs by setting `--datalake-formats delta` or configuring `io.delta.sql.DeltaSparkSessionExtension` and `org.apache.spark.sql.delta.catalog.DeltaCatalog`.
   - `incremental_strategy: merge`: Delta MERGE is fully supported by Delta Lake 2.x/3.x on AWS Glue.
3. **Absence of Proprietary Databricks Features**:
   - No Unity Catalog 3-part identifiers in model queries.
   - No Databricks Photon-specific functions.
   - No Databricks AI functions (`ai_query`, `ai_classify`, etc.).
   - No proprietary Databricks macros or packages.

---

## 4. Verification Harness Analysis (`verify_gold.py` & `test_gold_scd2_local.py`)

### 4.1 How Local PySpark Executes Gold Models and Tests
`spark_jobs/verify_gold.py` provides an offline execution engine for the dbt project:
- **Jinja2 Render Environment**:
  ```python
  def render(path: Path, *, incremental: bool = False, target: str = "target") -> str:
      template = Environment(undefined=StrictUndefined).from_string(path.read_text(encoding="utf-8"))
      return template.render(
          ref=lambda name: name,
          source=lambda _source, table: table,
          config=lambda **_settings: "",
          is_incremental=lambda: incremental,
          this=target,
      )
  ```
  This resolves dbt model dependencies to in-memory Spark temporary views without requiring an active database or dbt server.
- **Topological Execution & Memory Management**:
  - Registers Silver Delta tables as views (`slv_customers`, etc.).
  - Runs staging models (1-12) as views.
  - Generates mock snapshot tables (`customers_snapshot`, `products_snapshot`) with SCD2 columns.
  - Runs intermediate, dimension, and fact models (13-29), persisting frames with `StorageLevel.MEMORY_AND_DISK` to prevent executor OOM.
  - Runs business marts with `.localCheckpoint(eager=True)` to break lazy query plan lineage.
- **Verification Gates**:
  - Validates dimension key uniqueness and non-nullity.
  - Reconciles staging-to-fact row counts.
  - Runs all 5 core custom tests and 14 business custom tests. Any returning row triggers `AssertionError`.
  - Negative proof injection: Injects artificial duplicate rows into `mart_daily_sales` and `fct_order_items` and asserts that `assert_daily_sales_grain.sql` and `assert_core_grains.sql` correctly reject them.

### 4.2 SCD2 Fact Join Semantics in `test_gold_scd2_local.py`
`tests/test_gold_scd2_local.py` specifically tests temporal window joins:
```sql
join {{ ref('dim_customer') }} c
  on o.customer_id = c.customer_id
 and ((o.order_purchase_timestamp >= c.dbt_valid_from
       and (c.dbt_valid_to is null or o.order_purchase_timestamp < c.dbt_valid_to))
      or (c.version_number = 1 and o.order_purchase_timestamp < c.dbt_valid_from))
```
- Orders placed *before* the earliest snapshot timestamp join `version_number = 1`.
- Orders placed within subsequent snapshot windows join the matching temporal version.
- Incremental merge updates correctly track `source_ingested_at`.

---

## 5. Offline Compilation and Test Execution Recommendations

### 5.1 Resolving the Offline `dbt parse` Blocker
1. **The Issue**:
   The existing `dbt_project/profiles.yml` configures `type: databricks` with `auth_type: oauth`. During `dbt parse`, `dbt-databricks` attempts to initialize an interactive OAuth browser flow or connect to a Databricks host, hanging indefinitely in headless/CI environments.
2. **The Solution**:
   - Update `dbt_project/dbt_project.yml` profile name: `profile: ecommerce_aws`.
   - Update `dbt_project/profiles.yml` and `airflow/dbt_profile/profiles.yml`:
     ```yaml
     ecommerce_aws:
       target: dev
       outputs:
         dev:
           type: glue
           role_arn: "{{ env_var('AWS_GLUE_ROLE_ARN', 'arn:aws:iam::123456789012:role/GlueInteractiveSessionRole') }}"
           region: "{{ env_var('AWS_REGION', 'us-east-1') }}"
           workers: 2
           worker_type: "G.1X"
           schema: "{{ env_var('DBT_TARGET_SCHEMA', 'analytics_dev') }}"
           session_provisioning_timeout_in_seconds: 120
           location: "s3://{{ env_var('S3_BUCKET', 'ecommerce-cdc') }}/gold/"
     ```
   - In `models/staging/sources.yml`, delete line 5 (`database: "{{ env_var('DBT_CATALOG') }}"`).
   - With `type: glue` and safe defaults, `dbt parse` constructs `manifest.json` completely offline without attempting network connections.

### 5.2 Test Execution Strategy Across Environments
| Test Type | Target Asset | Command | Environment | Live AWS Needed? |
|---|---|---|---|---|
| Python Syntax | All modules | `python -m compileall spark_jobs tests dbt_project` | Local / Host CI | No |
| Offline Unit Tests | Glue CLI, manifests, CDC merge | `uv run python -m unittest tests/test_silver_manifest.py tests/test_silver_glue_offline.py tests/test_silver_glue_adversarial.py tests/test_silver_adversarial.py` | Local / Host CI | No |
| dbt DAG & Models | 35 models, 2 snapshots, 19 tests | `uv run dbt parse --project-dir dbt_project --profiles-dir dbt_project` | Local / Host CI | No |
| SCD2 & Local Gold Joins | `dim_customer`, `dim_product`, `fct_orders` | `uv run python -m unittest tests/test_gold_scd2_local.py` | Spark Docker (`SILVER_TEST_DOCKER=1`) | No |
| Full Gold Model & Test Run | 35 models & 19 tests | `uv run python -m spark_jobs.verify_gold --silver-root <path> --business` | Spark Docker (`SILVER_TEST_DOCKER=1`) | No |
