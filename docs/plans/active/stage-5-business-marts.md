# Execution Plan: Stage 5 local business marts

Date: 2026-11-02

## Status

Active

## Outcome

Build six dbt business marts from the Stage 4 Gold facts, reconcile their grains and measures against local Delta Gold/Silver, and leave cloud dbt execution for the owner's later Databricks check.

## Context

- `PLAN.md` stages 4–5 and `docs/decisions/0001-gold-fact-grains.md` define fact grains and historical sale price.
- The owner approved four original marts plus payment mix and order experience, and chose simple quintile-based RFM labels.
- `spark_jobs/verify_gold.py` executes the current dbt SQL against local Silver Delta; Databricks Silver tables do not yet exist.

## Scope

In scope: six `models/marts/business/` models, dbt tests, local verification, plan and README updates.

Out of scope: publishing to Databricks, building a dashboard, allocating payment by item/category, inferring product-level reviews or refund rate, and attributing mock campaigns to orders.

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
- [x] Document Databricks handoff in `README.md`; the owner will run the cloud check later.

## Decisions

- 2026-11-02: Use tie-aware percentile ranks rather than `ntile(5)` so identical R/F/M values receive identical scores.
- 2026-11-02: The owner selected simple quintile-based segmentation. Tie-aware R/F/M scores 1–5 use `percent_rank`; the priority rule is Champions (all three ≥4), Loyal (R≥3 and F≥4), Potential (R≥4), At Risk (R≥2), then Churned.
- 2026-11-02: Product category NULL maps to one `__unknown__` dimension row. The previous dimension emitted two rows for that key and multiplied product-mart revenue; local dimension-grain validation now catches this.

## Validation

- Focused proof: six mart-grain SQL tests passed; a duplicate sales-row fixture was rejected. RFM tie and segment tests passed.
- Integration or end-to-end proof: six business SQL models ran against local Delta. Sales, product, RFM, payment, experience, and funnel totals matched their facts; Stage 1B delivered revenue matched all 12 calendar months at BRL 13,221,498.11.
- Repository-required checks: `dbt parse` passed; `dbt ls --select marts.business --resource-type model` resolved exactly six models. `uv lock --check`, Python compile, and `git diff --check` passed.

## Result

Local Stage 5 implementation and source/fact reconciliation are complete. Databricks materialization and dbt test execution remain for the owner's later cloud check; keep this plan active until those results are recorded.
