# Execution Plan: Stage 5 local business marts

Date: 2026-11-02

## Status

Completed

## Outcome

Build six dbt business marts from the Stage 4 Gold facts, reconcile their grains and measures against local Delta Gold/Silver, then validate them on Databricks.

## Context

- `PLAN.md` stages 4–5 and `docs/decisions/0001-gold-fact-grains.md` define fact grains and historical sale price.
- The owner approved four original marts plus payment mix and order experience, and chose simple quintile-based RFM labels.
- `spark_jobs/verify_gold.py` executes the current dbt SQL against local Silver Delta; the later cloud check registered the same Silver snapshot on Databricks.

## Scope

In scope: six `models/marts/business/` models, dbt tests, local verification, plan and README updates.

Out of scope for the initial implementation: publishing to Databricks. Building a dashboard, allocating payment by item/category, inferring product-level reviews or refund rate, and attributing mock campaigns to orders remain out of scope. The Databricks follow-up is recorded below.

## Approach

1. Record exact mart grains, filters, and RFM scoring in `PLAN.md`.
2. Add six dbt models and source-grain reconciliation tests.
3. Extend the local Gold verifier to execute business marts and check counts, revenue, RFM uniqueness, and funnel bounds.
4. Run dbt parse, local Spark proof, focused negative proof, and repository checks.

## Risks And Recovery

- Olist's historical dates make `current_date` RFM recency misleading. Use a reproducible `as_of_date` one day after the latest delivered purchase in the data.
- Multiple payments/reviews per order create fanout. Payment mix stays at payment grain; experience aggregates reviews to one order before joining.
- The clickstream sample is generated from purchased orders, so funnel conversion cannot be presented as a real acquisition KPI.
- All changes are model and documentation files; existing Silver, source, and cloud data remain untouched. Revert this diff if validation fails.

## Progress

- [x] Inspected Stage 4 Gold facts and Stage 5 authority.
- [x] Record six mart contracts and RFM rules.
- [x] Implement dbt models and tests.
- [x] Execute and reconcile local business marts.
- [x] Document the Databricks run sequence in `README.md`.
- [x] 2026-11-02 cloud follow-up: build all six marts twice on Databricks and run their generic/singular tests.
- [x] Compare all six mart row counts and delivered revenue with the local proof, all 12 calendar months with the independent PostgreSQL baseline, and one Sao Paulo business day directly with local PostgreSQL.

## Decisions

- 2026-11-02: Use tie-aware percentile ranks rather than `ntile(5)` so identical R/F/M values receive identical scores.
- 2026-11-02: The owner selected simple quintile-based segmentation. Tie-aware R/F/M scores 1–5 use `percent_rank`; the priority rule is Champions (all three ≥4), Loyal (R≥3 and F≥4), Potential (R≥4), At Risk (R≥2), then Churned.
- 2026-11-02: Product category NULL maps to one `__unknown__` dimension row. The previous dimension emitted two rows for that key and multiplied product-mart revenue; local dimension-grain validation now catches this.

## Validation

- Focused proof: six mart-grain SQL tests passed; a duplicate sales-row fixture was rejected. RFM tie and segment tests passed.
- Integration or end-to-end proof: six business SQL models ran against local Delta. Sales, product, RFM, payment, experience, and funnel totals matched their facts; Stage 1B delivered revenue matched all 12 calendar months at BRL 13,221,498.11.
- Repository-required checks: `dbt parse` passed; `dbt ls --select marts.business --resource-type model` resolved exactly six models. `uv lock --check`, Python compile, and `git diff --check` passed.
- Databricks proof: two full project builds each passed 122/122 nodes with zero errors, including all six marts and 85 data tests. Mart row counts match the local proof. Delivered revenue is BRL 13,221,498.11; all 12 calendar months match Stage 1B.
- Independent one-day proof: for 2018-01-01 in `America/Sao_Paulo`, local PostgreSQL and Databricks both report 81 items, BRL 6,948.21 item price, and BRL 6,818.31 delivered item revenue.

## Result

Stage 5 implementation, local and Databricks materialization, dbt tests, source/fact reconciliation, and the independent one-day PostgreSQL comparison are complete. The full source-change pipeline remains tracked by the active Stage 4 plan.
