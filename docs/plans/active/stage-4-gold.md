# Execution Plan: Stage 4 Gold and Databricks validation

Date: 2026-11-01

## Status

Active

## Outcome

Define the accepted constellation of Gold stars in dbt, prove its SQL locally, then materialize it on Databricks and test SCD2 and incremental behavior without changing the frozen source.

## Context

- `PLAN.md` stages 3–5 and the user's accepted 7-dimension/7-fact proposal define scope and grains.
- `docs/decisions/0001-gold-fact-grains.md` records the lasting grain and historical-price rules.
- `spark_jobs/silver_transform.py` and `logs/local_silver/` define the current Silver schema. The frozen baseline lacks Stage 1C `current_price` and soft-delete columns.
- The verified local Silver snapshot was published under `s3://<project-bucket>/silver/stage3_local_20261102/` and registered as 12 external Delta sources in `ecommerce_cdc.silver`; managed Gold tables use the project bucket's `gold/` storage root.
- `docs/WORKFLOW.md` requires behavior-level proof and an active plan for dependent work.

## Scope

In scope: revise stages 4–5 in `PLAN.md`, create the dbt project and its fact/dimension models, add grain and reconciliation checks, validate against local Silver, and (2026-11-02 follow-up) publish that Silver snapshot to Databricks for cloud build and isolated SCD2 proof.

Out of scope: mutating the frozen source and building the Stage 5 dashboard marts. A full Stage 1C source → Bronze → Silver → dbt change rehearsal still depends on the cloud Silver job.

## Approach

1. Record each dimension and fact grain, source, measures, and mart ownership in `PLAN.md`.
2. Add dbt staging, customer/product snapshots, intermediate aggregations, 7 dimensions, 7 facts, and optional synthetic campaign dimension.
3. Validate dbt parsing and execute the core SQL against local Delta using a focused Spark proof. Check counts, keys, and revenue against the Olist baseline.
4. Document the cloud run sequence and the unverified SCD2 price/soft-delete scenario without changing the frozen baseline.
5. Publish the local Silver snapshot to a versioned project S3 prefix; register one small Delta table and read it before registering the others. Run the full dbt build twice and compare cloud counts/revenue to the PostgreSQL baseline.
6. Test customer/product snapshot changes on copied Silver tables in `ecommerce_cdc.silver_validation`, leaving the production Silver and OLTP source unchanged. Record query metrics, generated lineage, and remaining limits.

## Risks And Recovery

- Snapshot history starts at the first observation and cannot reconstruct 2016–2018 product/customer versions. Historical item revenue remains `order_items.price`; older facts use the first observed dimension version.
- A direct many-to-many join of items, payments, or reviews would multiply measures; aggregate by order before order-level joins and retain separate fact grains.
- The current local Silver lacks Stage 1C fields. Test customer address SCD2 on an isolated source; do not claim product price SCD2 until that source reaches Silver.
- Upload every file in a Delta directory and verify S3 object sizes before registration; partial uploads must never be registered. Re-run publication into a fresh prefix if the snapshot changes, then point the external tables at that new version intentionally.
- The earlier unchanged second `fct_orders` MERGE updated all 99,441 rows. Stage 6 now excludes existing keys at the same source-ingestion time; its first full cloud build on unchanged Silver updated/inserted zero rows in all six incremental facts. Recheck with a changed-source fixture before scheduling daily builds.
- Changes are new dbt files and documentation; revert their diff if the local proof fails. Existing Silver and source data are read-only.

## Progress

- [x] Inspected the frozen baseline, Silver schema, current plan, and local runtime.
- [x] Revise `PLAN.md` stages 4–5.
- [x] Implement dbt project, snapshots, dimensions, facts, and tests.
- [x] Run dbt parse and local Spark model proof.
- [x] Record Databricks/SCD2 gaps and recovery commands in `README.md` and `PLAN.md`.
- [x] 2026-11-02 cloud follow-up: publish verified local Silver to the project S3 bucket and register 12 sources in `ecommerce_cdc.silver`; run two full dbt builds and all generic/singular tests.
- [x] Validate customer/product snapshot changes in a separate Databricks Silver schema and inspect old fact keys and historical sale price.
- [x] Generate dbt docs, inspect lineage, and compare filtered/unfiltered Databricks query metrics.
- [ ] Run the full Stage 1C isolated source change through Bronze, cloud Silver, and dbt; current SCD2 proof starts at copied Silver and does not cover that path.

## Decisions

- 2026-11-01: Keep Gold as a fact constellation: commerce, behavior, and reference facts share conformed dimensions. The campaign dimension remains synthetic and is not attributed to orders by `utm_source` alone.
- 2026-11-01: Use source-ingestion timestamps, not purchase-date recency, to select incremental changes. An updated old order must still be eligible for a MERGE.

## Validation

- Focused proof: `tests.test_gold_scd2_local` passed with two customer/product versions; old/new orders kept their proper keys and historical item prices.
- Integration or end-to-end proof: `spark_jobs.verify_gold` executed 29 dbt model SELECTs against 12 local Silver Delta tables. Six fact counts matched source; 20 sessions, BRL 13,591,643.70 item revenue, and 24 purchase months matched Stage 1B. Five singular tests passed; a duplicate fixture was rejected.
- 2026-11-02 local follow-up: Stage 5 product-mart reconciliation exposed two `dim_category` rows for `__unknown__` when a source category was NULL. The dimension now emits one row, all eight dimension keys passed uniqueness/non-null checks, and product mart revenue matched delivered item fact.
- Repository-required checks: dbt parse passed with a non-network test profile; `uv lock --check`, Python compile, and `git diff --check` passed. The final local Gold proof and two-version fixture both passed after retaining historical geography/category keys.
- 2026-11-02 cloud proof: two full `dbt build` runs each passed 122/122 nodes, including 85 data tests and both snapshots. `fct_orders` Delta history shows an initial CTAS and subsequent MERGE; counts and BRL 13,591,643.70 item price remained stable, and all 24 purchase months match the independent PostgreSQL baseline.
- All 194 local Delta files (377,347,774 bytes) were uploaded and size-checked before registration. Twelve cloud Silver source counts match local: customers/orders 99,441; geolocation 1,000,163; translation 71; products 32,951; sellers 3,095; items 112,650; payments 103,886; reviews 99,224; clickstream 80; rates 777; campaigns 3.
- Isolated SCD2 proof: customer city and product current price changes produced two versions per selected entity with one current version; the old order kept the first-version keys and BRL 44.99 item price after the catalog price reached BRL 54.99. The full Stage 1C source → Bronze → Silver → dbt change path remains untested.
- Query profile: filtered and unfiltered order queries both read one 10,111,873-byte file from cache; 837 ms and 939 ms respectively. Current data size does not warrant clustering, but the filter did not prune files.
- Efficiency finding: the unchanged second build updated all 99,441 matched orders because `>=` includes the whole snapshot ingestion-time boundary. This does not duplicate rows but should be optimized before scheduled operation.
- Stage 6 optimization proof: a full cloud `dbt build` passed 122/122 after the six incremental facts excluded existing keys at the same ingestion timestamp. Delta history of all six MERGEs reported zero updated and zero inserted rows on unchanged Silver.
- Independent day check: local PostgreSQL and Databricks both returned 81 items, BRL 6,948.21 item price, and BRL 6,818.31 delivered item revenue for 2018-01-01 in `America/Sao_Paulo`. The production customer/product snapshots still have 99,441/32,951 rows and zero closed versions after the isolated fixture.

## Result

Local Gold and Databricks materialization, generic/singular tests, baseline reconciliation, snapshot-change fixture, and incremental MERGE execution are verified. The full source 1C → Bronze → Silver → dbt change path remains open, so this plan stays active rather than claiming Stage 4 fully done.
