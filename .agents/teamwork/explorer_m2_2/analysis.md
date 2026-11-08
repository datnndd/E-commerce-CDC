# Detailed Analysis: `sources.yml` Decoupling and 2-Tier Source Resolution

**Author**: `explorer_m2_2`  
**Milestone**: M2 (Gold Layer & dbt Migration to AWS)  
**Date**: 2026-11-07  
**Working Directory**: `c:\Users\ddat2\Downloads\Projects\E-commerce-CDC\.agents\teamwork\explorer_m2_2\`

---

## 1. Executive Summary

This investigation analyzes `dbt_project/models/staging/sources.yml`, the mechanics of dbt source resolution, and the transition from Databricks Unity Catalog 3-tier namespaces (`catalog.schema.table`) to AWS Glue / Apache Spark 2-tier namespaces (`schema.table`).

### Key Findings:
1. **Current `sources.yml` Coupling**: `dbt_project/models/staging/sources.yml` explicitly specifies `database: "{{ env_var('DBT_CATALOG') }}"` at line 5. Because this environment variable lacks a default fallback value, running `dbt parse` when `DBT_CATALOG` is unset immediately fails with `Parsing Error: Env var required but not provided: 'DBT_CATALOG'`.
2. **Spark Relation Invariant**: In `dbt-spark` (and AWS Glue Spark), `SparkRelation` explicitly forbids setting `database` when it differs from `schema`. Specifically, `SparkRelation.__post_init__` raises `dbt_common.exceptions.base.DbtRuntimeError: Runtime Error: Cannot set database in spark!` whenever `database` is not None. In addition, `SparkIncludePolicy` sets `database: False`, ensuring that when `database` is `None`, relations render strictly as 2-tier: `schema.identifier` (e.g. `silver.slv_customers`).
3. **AWS Glue Data Catalog Topology**: AWS Glue Data Catalog and Apache Spark SQL are inherently 2-tier metastores (`database.table`, which dbt maps to `schema.identifier`). In AWS Glue, the Silver tables reside in Glue database `silver` (configurable via `DBT_SILVER_SCHEMA`). Attempting to use a 3-tier reference such as `ecommerce_cdc.silver.slv_customers` causes Spark SQL `AnalysisException: Cannot find catalog: ecommerce_cdc`.
4. **Scope of Silver Sources & `DBT_CATALOG` References**:
   - There are exactly 12 Silver tables in `sources.yml`, perfectly aligned with `spark_jobs/silver_schema.py` (`SILVER_NAMES`).
   - Exactly 12 staging models in `dbt_project/models/staging/` reference `{{ source('silver', 'slv_...') }}` via single-line `select * from ...` queries.
   - Zero intermediate models, zero marts models (facts/dimensions), zero snapshots, zero custom tests, and zero macros reference `source(...)` or `DBT_CATALOG`.
   - In `dbt_project/`, the ONLY references to `DBT_CATALOG` are `models/staging/sources.yml:5` and `profiles.yml.example:9` (and local `profiles.yml:9`).
5. **Recommended Action**: Remove line 5 (`database: "{{ env_var('DBT_CATALOG') }}"`) from `dbt_project/models/staging/sources.yml`. This completely decouples source parsing from Unity Catalog and `DBT_CATALOG`, allowing offline compilation and 2-tier Spark SQL execution against AWS Glue.

---

## 2. Investigation of `models/staging/sources.yml`

### Current File Contents (`dbt_project/models/staging/sources.yml`):
```yaml
version: 2

sources:
  - name: silver
    database: "{{ env_var('DBT_CATALOG') }}"
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

### Analysis of Current Configuration:
- **`database: "{{ env_var('DBT_CATALOG') }}"`**:
  - In Databricks Unity Catalog, `database` represents the catalog name (e.g., `ecommerce_cdc`).
  - Unlike `DBT_SILVER_SCHEMA` (which has a safe fallback `'silver'`), `DBT_CATALOG` has no fallback argument.
  - Observed error during offline verification when `DBT_CATALOG` is unset:
    ```
    11:45:08  Running with dbt=1.11.8
    11:45:09  Encountered an error:
    Parsing Error
      Env var required but not provided: 'DBT_CATALOG'
    ```
- **`schema: "{{ env_var('DBT_SILVER_SCHEMA', 'silver') }}"`**:
  - Properly configured with a default fallback to `'silver'`.
  - In AWS Glue / Spark, this becomes the Glue Catalog database name containing the 12 Silver Delta tables.
- **`tables`**:
  - Exactly 12 tables matching the Silver schema definitions.

---

## 3. The 2-Tier vs 3-Tier Source Resolution Mechanism

### dbt Internal Architecture & Relation Classes

dbt converts source entries in `sources.yml` into `SourceDefinition` nodes in `Manifest.sources`. When a model calls `{{ source('silver', 'slv_customers') }}`, dbt's `source()` macro calls `adapter.Relation.create_from(configuration, node)`.

Let us compare how the relation classes behave:

#### 1. `DatabricksRelation` (`dbt-databricks`)
Located in `.venv/Lib/site-packages/dbt/adapters/databricks/relation.py`:
```python
@dataclass
class DatabricksIncludePolicy(Policy):
    database: bool = True
    schema: bool = True
    identifier: bool = True
```
When `database` is set to `ecommerce_cdc` and `schema` is `silver`:
- `include_policy.database` is `True`
- `_render_iterator()` yields `('database', '`ecommerce_cdc`')`, `('schema', '`silver`')`, `('identifier', '`slv_customers`')`
- Output SQL: `` `ecommerce_cdc`.`silver`.`slv_customers` `` (3-tier).

#### 2. `SparkRelation` (`dbt-spark` and AWS Glue Spark)
Located in `.venv/Lib/site-packages/dbt/adapters/spark/relation.py`:
```python
@dataclass
class SparkIncludePolicy(Policy):
    database: bool = False
    schema: bool = True
    identifier: bool = True

@dataclass(frozen=True, eq=False, repr=False)
class SparkRelation(BaseRelation):
    quote_policy: Policy = field(default_factory=lambda: SparkQuotePolicy())
    include_policy: Policy = field(default_factory=lambda: SparkIncludePolicy())
    quote_character: str = "`"

    def __post_init__(self) -> None:
        if self.database != self.schema and self.database:
            raise DbtRuntimeError("Cannot set database in spark!")

    def render(self) -> str:
        if self.include_policy.database and self.include_policy.schema:
            raise DbtRuntimeError(
                "Got a spark relation with schema and database set to "
                "include, but only one can be set"
            )
        return super().render()
```

### Empirical Test Execution:

1. **Attempting to create a SparkRelation with `database='ecommerce_cdc'`**:
   ```python
   from dbt.adapters.spark.relation import SparkRelation
   r = SparkRelation.create(database='ecommerce_cdc', schema='silver', identifier='slv_customers')
   ```
   **Result**:
   ```
   dbt_common.exceptions.base.DbtRuntimeError: Runtime Error
     Cannot set database in spark!
   ```

2. **Creating a SparkRelation with `database=None`**:
   ```python
   r = SparkRelation.create(database=None, schema='silver', identifier='slv_customers')
   print(r.render())
   ```
   **Result**:
   ```
   silver.slv_customers
   ```

3. **Behavior in `dbt.parser.sources.SourcePatcher`**:
   In `SourcePatcher.parse_source()`:
   ```python
   default_database = self.root_project.credentials.database

   parsed_source = SourceDefinition(
       package_name=target.package_name,
       database=(source.database or default_database),
       unrendered_database=source.unrendered_database,
       schema=(source.schema or source.name),
       unrendered_schema=source.unrendered_schema,
       identifier=(table.identifier or table.name),
       ...
   )
   ```
   When `database` is omitted from `sources.yml`:
   - `source.database` evaluates to `None`.
   - `default_database` in an AWS Glue / Spark profile is `None` (or Spark schema).
   - `parsed_source.database` becomes `None`.
   - When models compile, `{{ source('silver', '<table_name>') }}` renders strictly as `silver.<table_name>`.

---

## 4. Comprehensive Inventory of Silver Sources and References

### Table Inventory Comparison

| # | Silver Table Name in `sources.yml` | Staging Model File (`models/staging/`) | Staging Model Query | Definition in `spark_jobs/silver_schema.py` |
|---|------------------------------------|----------------------------------------|---------------------|---------------------------------------------|
| 1 | `slv_customers` | `stg_customers.sql` | `select * from {{ source('silver', 'slv_customers') }}` | `OLTP_NAMES["customers"]` |
| 2 | `slv_geolocation` | `stg_geolocation.sql` | `select * from {{ source('silver', 'slv_geolocation') }}` | `OLTP_NAMES["geolocation"]` |
| 3 | `slv_category_translation` | `stg_category_translation.sql` | `select * from {{ source('silver', 'slv_category_translation') }}` | `OLTP_NAMES["product_category_name_translation"]` |
| 4 | `slv_products` | `stg_products.sql` | `select * from {{ source('silver', 'slv_products') }}` | `OLTP_NAMES["products"]` |
| 5 | `slv_sellers` | `stg_sellers.sql` | `select * from {{ source('silver', 'slv_sellers') }}` | `OLTP_NAMES["sellers"]` |
| 6 | `slv_orders` | `stg_orders.sql` | `select * from {{ source('silver', 'slv_orders') }}` | `OLTP_NAMES["orders"]` |
| 7 | `slv_order_items` | `stg_order_items.sql` | `select * from {{ source('silver', 'slv_order_items') }}` | `OLTP_NAMES["order_items"]` |
| 8 | `slv_payments` | `stg_payments.sql` | `select * from {{ source('silver', 'slv_payments') }}` | `OLTP_NAMES["order_payments"]` |
| 9 | `slv_reviews` | `stg_reviews.sql` | `select * from {{ source('silver', 'slv_reviews') }}` | `OLTP_NAMES["order_reviews"]` |
| 10 | `slv_clickstream_events` | `stg_clickstream_events.sql` | `select * from {{ source('silver', 'slv_clickstream_events') }}` | `SILVER_NAMES["clickstream"]` |
| 11 | `slv_exchange_rates` | `stg_exchange_rates.sql` | `select * from {{ source('silver', 'slv_exchange_rates') }}` | `SILVER_NAMES["exchange_rate"]` |
| 12 | `slv_campaigns` | `stg_campaigns.sql` | `select * from {{ source('silver', 'slv_campaigns') }}` | `SILVER_NAMES["campaign"]` |

### Audit of `source()` and `DBT_CATALOG` Usage Across Project

1. **Calls to `source()`**:
   - Exactly 12 occurrences across the entire project, all inside `dbt_project/models/staging/`.
   - Zero occurrences in `models/intermediate/`.
   - Zero occurrences in `models/marts/core/`.
   - Zero occurrences in `models/marts/business/`.
   - Zero occurrences in `snapshots/`.
   - Zero occurrences in `tests/`.

2. **References to `DBT_CATALOG` across the entire codebase**:
   - `dbt_project/models/staging/sources.yml:5`: `database: "{{ env_var('DBT_CATALOG') }}"`
   - `dbt_project/profiles.yml.example:9`: `database: "{{ env_var('DBT_CATALOG') }}"`
   - `dbt_project/profiles.yml:9`: `database: "{{ env_var('DBT_CATALOG') }}"`
   - `airflow/dbt_profile/profiles.yml:10`: `database: "{{ env_var('DBT_CATALOG') }}"`
   - `airflow/dags/ecommerce_cdc_daily.py:67, 122`: validation and CLI argument `--catalog`
   - `orchestration/ops_runs.py:43, 122`: Databricks Unity Catalog operational run logging
   - `compose.yaml:38`: `DBT_CATALOG: ${DBT_CATALOG:-}`
   - `.env.example:35`: `DBT_CATALOG=ecommerce_cdc`
   - `README.md:235, 273`: documentation
   - **Crucial finding**: There are ZERO references to `DBT_CATALOG` in any SQL model, snapshot, test, or macro file.

---

## 5. Recommended Edits and Exact Unified Diff

### Target File:
`dbt_project/models/staging/sources.yml`

### Proposed Content:
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

### Unified Diff:
```diff
--- a/dbt_project/models/staging/sources.yml
+++ b/dbt_project/models/staging/sources.yml
@@ -2,5 +2,4 @@ version: 2
 
 sources:
   - name: silver
-    database: "{{ env_var('DBT_CATALOG') }}"
     schema: "{{ env_var('DBT_SILVER_SCHEMA', 'silver') }}"
     tables:
```

### Impact Assessment:
- **Zero downstream model modifications**: All 12 staging models remain unchanged (`select * from {{ source('silver', 'slv_<name>') }}`).
- **Elimination of `DBT_CATALOG` requirement**: dbt parse no longer prompts or errors if `DBT_CATALOG` is absent.
- **Clean 2-tier rendering**: `source('silver', 'slv_customers')` compiles directly to `silver.slv_customers` (or `${DBT_SILVER_SCHEMA}.slv_customers`), matching AWS Glue Spark and AWS Athena table naming conventions.

---

## 6. Offline Verification Steps

When the proposed edit is applied along with the AWS Glue profile update (Milestone M2 Feature 8):

1. **Test Offline Parsing without `DBT_CATALOG`**:
   ```bash
   unset DBT_CATALOG
   dbt parse --no-version-check --project-dir dbt_project --profiles-dir dbt_project
   ```
   *Expected result*: Exit code 0, resolves all 35 models, 2 snapshots, 19 tests, and 12 sources without requiring `DBT_CATALOG`.

2. **Verify Generated `manifest.json` Source Nodes**:
   ```python
   import json
   with open("dbt_project/target/manifest.json") as f:
       manifest = json.load(f)
   for k, v in manifest["sources"].items():
       assert v["database"] is None or v["database"] == ""
       assert v["schema"] == "silver"
   ```

3. **Verify Staging SQL Compilation**:
   Each staging model compiles to `select * from silver.slv_<name>`.
