# Handoff Report: Milestone M2 `sources.yml` Decoupling & 2-Tier Source Resolution

**Agent**: `explorer_m2_2`  
**Milestone**: M2 (Gold Layer & dbt Migration to AWS)  
**Date**: 2026-11-07  
**Working Directory**: `c:\Users\ddat2\Downloads\Projects\E-commerce-CDC\.agents\teamwork\explorer_m2_2\`

---

## 1. Observation

1. **Exact Contents of `dbt_project/models/staging/sources.yml`**:
   - `dbt_project/models/staging/sources.yml:1-20`:
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

2. **Verbatim Error when `DBT_CATALOG` is Unset**:
   - Command:
     ```cmd
     set DBT_DATABRICKS_HOST=127.0.0.1&& set DBT_DATABRICKS_HTTP_PATH=/dummy&& set DATABRICKS_TOKEN=dummy&& set DBT_CATALOG=&& .venv\Scripts\dbt.exe parse --no-version-check --project-dir dbt_project --profiles-dir airflow/dbt_profile
     ```
   - Verbatim Output:
     ```
     11:45:08  Running with dbt=1.11.8
     11:45:09  Encountered an error:
     Parsing Error
       Env var required but not provided: 'DBT_CATALOG'
     ```

3. **`SparkRelation` 2-Tier Enforcement & Rejection of `database`**:
   - Source code in `.venv/Lib/site-packages/dbt/adapters/spark/relation.py:22-49`:
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
         ...
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
   - Verbatim error when testing `SparkRelation.create(database='ecommerce_cdc', schema='silver', identifier='slv_customers')`:
     ```
     dbt_common.exceptions.base.DbtRuntimeError: Runtime Error
       Cannot set database in spark!
     ```
   - Verbatim output when testing `SparkRelation.create(database=None, schema='silver', identifier='slv_customers')`:
     ```
     Rendered: silver.slv_customers
     ```

4. **All 12 Silver Source References in Staging Models**:
   - Exactly 12 staging model files in `dbt_project/models/staging/`:
     - `stg_campaigns.sql:1`: `select * from {{ source('silver', 'slv_campaigns') }}`
     - `stg_category_translation.sql:1`: `select * from {{ source('silver', 'slv_category_translation') }}`
     - `stg_clickstream_events.sql:1`: `select * from {{ source('silver', 'slv_clickstream_events') }}`
     - `stg_customers.sql:1`: `select * from {{ source('silver', 'slv_customers') }}`
     - `stg_exchange_rates.sql:1`: `select * from {{ source('silver', 'slv_exchange_rates') }}`
     - `stg_geolocation.sql:1`: `select * from {{ source('silver', 'slv_geolocation') }}`
     - `stg_order_items.sql:1`: `select * from {{ source('silver', 'slv_order_items') }}`
     - `stg_orders.sql:1`: `select * from {{ source('silver', 'slv_orders') }}`
     - `stg_payments.sql:1`: `select * from {{ source('silver', 'slv_payments') }}`
     - `stg_products.sql:1`: `select * from {{ source('silver', 'slv_products') }}`
     - `stg_reviews.sql:1`: `select * from {{ source('silver', 'slv_reviews') }}`
     - `stg_sellers.sql:1`: `select * from {{ source('silver', 'slv_sellers') }}`
   - All 12 table names match `spark_jobs/silver_schema.py` (`SILVER_NAMES` and `KEYS`).

5. **Exhaustive Grep for `DBT_CATALOG`**:
   - `ripgrep` across entire repository showed `DBT_CATALOG` exists only in:
     - `dbt_project/models/staging/sources.yml:5`
     - `dbt_project/profiles.yml.example:9` and `dbt_project/profiles.yml:9`
     - `airflow/dbt_profile/profiles.yml:10`
     - `compose.yaml:38`
     - `airflow/dags/ecommerce_cdc_daily.py:67, 122`
     - `orchestration/ops_runs.py:43, 122`
     - `.env.example:35`
     - `README.md:235, 273`
   - ZERO occurrences in any SQL model (`models/**/*.sql`), snapshot (`snapshots/**/*.sql`), test (`tests/**/*.sql`), or macro (`macros/` does not exist).

---

## 2. Logic Chain

1. From Observation 1 and 2, `sources.yml:5` defines `database: "{{ env_var('DBT_CATALOG') }}"` without a default value. When `DBT_CATALOG` is absent from the shell/environment, `dbt parse` immediately crashes with `Env var required but not provided: 'DBT_CATALOG'`.
2. From Observation 3, `SparkRelation.__post_init__` enforces that `database` cannot be specified when different from `schema` (`if self.database != self.schema and self.database: raise DbtRuntimeError("Cannot set database in spark!")`). When `database=None`, `SparkIncludePolicy` includes only `schema` and `identifier`, generating clean 2-tier SQL (`silver.<table_name>`).
3. AWS Glue Data Catalog and AWS Glue Spark are 2-tier architectures (`database.table`, which dbt maps to `schema.table`). In Glue, the Silver tables reside in Glue database `silver` (or the database specified by `DBT_SILVER_SCHEMA`).
4. If `database: "{{ env_var('DBT_CATALOG') }}"` remains in `sources.yml`, compiling against AWS Glue Spark will either fail in the dbt adapter with `Cannot set database in spark!` or produce 3-tier SQL (`ecommerce_cdc.silver.<table_name>`) that fails on Spark SQL with `AnalysisException: Cannot find catalog: ecommerce_cdc`.
5. From Observation 4 and 5, only the 12 staging models reference `{{ source('silver', 'slv_...') }}`. No other models, snapshots, tests, or macros reference `DBT_CATALOG` or `source('silver', ...)`. All downstream models reference `ref('stg_...')`.
6. Therefore, removing `database: "{{ env_var('DBT_CATALOG') }}"` from `sources.yml` completely resolves the 2-tier vs 3-tier conflict for all 12 Silver tables, eliminates the `DBT_CATALOG` environment requirement, and requires zero modifications to downstream models.

---

## 3. Caveats

- **Profile Co-dependency**: Removing `database:` from `sources.yml` resolves the source layer. However, `dbt parse` also inspects `dbt_project/profiles.yml` (and `dbt_project/dbt_project.yml`). To parse completely without `DBT_CATALOG`, the target profile in `profiles.yml` must also be migrated to `type: glue` (owned by peer explorer `explorer_m2_1`).
- **Glue Database Creation in Live AWS**: In offline testing, `silver.<table_name>` compiles as a standard Spark SQL view/table identifier. In live AWS, the Glue database corresponding to `DBT_SILVER_SCHEMA` (`silver`) must exist in the AWS Glue Data Catalog before tables can be read.

---

## 4. Conclusion

1. **Exact Recommended Edit**:
   Remove line 5 (`database: "{{ env_var('DBT_CATALOG') }}"`) from `dbt_project/models/staging/sources.yml`.

2. **Unified Diff**:
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

3. **Downstream Safety**:
   - Zero SQL models, snapshots, or tests need modification.
   - All 12 staging models cleanly resolve to `silver.slv_<table_name>` (2-tier).
   - `DBT_CATALOG` environment variable is no longer required for dbt source compilation.

---

## 5. Verification Method

1. **Check Absence of `DBT_CATALOG` in `sources.yml`**:
   ```powershell
   Select-String -Path dbt_project/models/staging/sources.yml -Pattern "DBT_CATALOG"
   ```
   *Expected outcome*: No matches found.

2. **Verify 2-Tier Spark Relation Rendering for All 12 Tables**:
   ```powershell
   .venv\Scripts\python.exe -c "from dbt.adapters.spark.relation import SparkRelation; tables = ['slv_customers', 'slv_geolocation', 'slv_category_translation', 'slv_products', 'slv_sellers', 'slv_orders', 'slv_order_items', 'slv_payments', 'slv_reviews', 'slv_clickstream_events', 'slv_exchange_rates', 'slv_campaigns']; [print(SparkRelation.create(database=None, schema='silver', identifier=t).render()) for t in tables]"
   ```
   *Expected outcome*: All 12 tables output as `silver.slv_<name>`.

3. **Offline `dbt parse` Execution**:
   Once `profiles.yml` is updated to `type: glue` (by Worker M2):
   ```powershell
   dbt parse --no-version-check --project-dir dbt_project --profiles-dir dbt_project
   ```
   *Expected outcome*: Exit code 0, resolves all 35 models, 2 snapshots, 19 tests, and 12 sources without requiring `DBT_CATALOG`.

4. **Invalidation Condition**:
   If any model or macro references `{{ source('silver', ...) }}` with an explicit database override, or if an adapter requires 3-tier catalog naming on AWS Glue, this conclusion is invalidated.
