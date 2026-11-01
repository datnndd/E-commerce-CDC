# Execution Plan: Stage 4 local Gold model

Date: 2026-11-01

## Status

Active

## Outcome

Define the accepted constellation of Gold stars in dbt, execute its core SQL against the existing local Silver Delta data, and document the remaining Databricks and SCD2 integration proof.

## Context

- `PLAN.md` stages 3–5 and the user's accepted 7-dimension/7-fact proposal define scope and grains.
- `docs/decisions/0001-gold-fact-grains.md` records the lasting grain and historical-price rules.
- `spark_jobs/silver_transform.py` and `logs/local_silver/` define the current Silver schema. The frozen baseline lacks Stage 1C `current_price` and soft-delete columns.
- `docs/WORKFLOW.md` requires behavior-level proof and an active plan for dependent work.

## Scope

In scope: revise stages 4–5 in `PLAN.md`, create the dbt project and its fact/dimension models, add grain and reconciliation checks, validate against local Silver where possible.

Out of scope: mutating the frozen source, uploading local Silver to Databricks, and building Stage 5 dashboard marts.

## Approach

1. Record each dimension and fact grain, source, measures, and mart ownership in `PLAN.md`.
2. Add dbt staging, customer/product snapshots, intermediate aggregations, 7 dimensions, 7 facts, and optional synthetic campaign dimension.
3. Validate dbt parsing and execute the core SQL against local Delta using a focused Spark proof. Check counts, keys, and revenue against the Olist baseline.
4. Document the cloud run sequence and the unverified SCD2 price/soft-delete scenario without changing the frozen baseline.

## Risks And Recovery

- Snapshot history starts at the first observation and cannot reconstruct 2016–2018 product/customer versions. Historical item revenue remains `order_items.price`; older facts use the first observed dimension version.
- A direct many-to-many join of items, payments, or reviews would multiply measures; aggregate by order before order-level joins and retain separate fact grains.
- The current local Silver lacks Stage 1C fields. Test customer address SCD2 on an isolated source; do not claim product price SCD2 until that source reaches Silver.
- Changes are new dbt files and documentation; revert their diff if the local proof fails. Existing Silver and source data are read-only.

## Progress

- [x] Inspected the frozen baseline, Silver schema, current plan, and local runtime.
- [x] Revise `PLAN.md` stages 4–5.
- [x] Implement dbt project, snapshots, dimensions, facts, and tests.
- [x] Run dbt parse and local Spark model proof.
- [x] Record Databricks/SCD2 gaps and recovery commands in `README.md` and `PLAN.md`.

## Decisions

- 2026-11-01: Keep Gold as a fact constellation: commerce, behavior, and reference facts share conformed dimensions. The campaign dimension remains synthetic and is not attributed to orders by `utm_source` alone.
- 2026-11-01: Use source-ingestion timestamps, not purchase-date recency, to select incremental changes. An updated old order must still be eligible for a MERGE.

## Validation

- Focused proof: `tests.test_gold_scd2_local` passed with two customer/product versions; old/new orders kept their proper keys and historical item prices.
- Integration or end-to-end proof: `spark_jobs.verify_gold` executed 29 dbt model SELECTs against 12 local Silver Delta tables. Six fact counts matched source; 20 sessions, BRL 13,591,643.70 item revenue, and 24 purchase months matched Stage 1B. Five singular tests passed; a duplicate fixture was rejected.
- Repository-required checks: dbt parse passed with a non-network test profile; `uv lock --check`, Python compile, and `git diff --check` passed. The final local Gold proof and two-version fixture both passed after retaining historical geography/category keys.

## Result

Local Gold SQL and focused proof are complete. Actual dbt materialization, generic tests, snapshot updates, and incremental MERGE on Databricks remain unverified because Silver has not been loaded into the workspace. Keep this plan active until those checks pass.
