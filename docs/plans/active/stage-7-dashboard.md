# Execution Plan: Stage 7 Power BI dashboard and reconciliation

Date: 2026-11-02

## Status

Active — prepared; implementation has not started.

## Outcome

Deliver a five-page Power BI report over the six Databricks Gold business marts and `ops_pipeline_runs`, with five visible numbers traced back to their marts, facts, and OLTP source.

## Context

- `PLAN.md` Stage 7 defines the five pages, allowed filters, KPI formulas, and reconciliation goal. `docs/decisions/0002-power-bi-dashboard.md` records the owner's Power BI choice.
- Stage 5 built and tested six marts on Databricks. Stage 6 built the Ops table and passed an isolated end-to-end run and a negative quality test. The owner accepted Stage 6 closure; a later reported DAG success, repeated-logical-date behavior, and unattended scheduling remain unverified in local metadata.
- Databricks Free Edition has a SQL warehouse, but the exact Power BI connection, authentication, refresh, and sharing path must be tested in this workspace.

## Scope

In scope: Power BI Desktop connection proof; a report using the existing mart grains; five planned pages; page-specific filters and source-date defaults; five-number reconciliation; report instructions and captured evidence.

Out of scope: new marts, new attribution logic, real clickstream conversion, unsupported refund or product-rating metrics, and claiming scheduled refresh or public sharing before verification.

## Approach

1. Connect Power BI Desktop to the Databricks SQL warehouse and read `mart_daily_sales` plus `ops_pipeline_runs`. Record the server/HTTP path setup without secrets. Choose Import or DirectQuery from this result, then check how the report can be shared and refreshed.
2. Establish a small semantic model over the six marts and Ops. Confirm each table's grain and date column. Keep Product and RFM independent of the Executive purchase-date filter; keep funnel session date separate.
3. Build the five pages in `PLAN.md`: Executive/Sales, Product, Customer/RFM, Journey & Experience, and Data Quality/Ops. Label the funnel as synthetic and show RFM `as_of_date`.
4. Check measures at display grain. Recompute AOV, cancellation rate, and late-delivery rate from summed numerators and denominators; do not average stored rates. Default date slicers to dates present in Olist.
5. Select five visible numbers and document dashboard → mart → fact → OLTP SQL reconciliation, then have a reader inspect page 1 without explanation.

## Risks And Recovery

- Power BI authentication or Databricks Free Edition limits may block a direct connection. Keep the connection mode open until the smoke test passes; an export route requires a separate decision and reconciliation check.
- Mixing marts with different grains can multiply measures or apply false filters. Use the page-specific filter contracts in `PLAN.md` and compare displayed totals to the source marts.
- A report file may retain connection metadata. Do not commit or share credentials; inspect the artifact before publishing.
- Stage 6 carryover: before claiming an automated Ops dashboard, reconcile a final Airflow `success` run ID with `ops_pipeline_runs`, then separately test repeated-logical-date and unattended operation if those claims are needed.

## Progress

- [x] Record the owner's Power BI choice and the existing five-page/mart scope.
- [ ] Prove Power BI Desktop connection to one mart and Ops; decide Import/DirectQuery and refresh/sharing path.
- [ ] Build and inspect five report pages.
- [ ] Reconcile five dashboard numbers to mart, fact, and OLTP.
- [ ] Validate report usability and document the result.

## Decisions

- 2026-11-02: The owner selected Power BI. Connection mode and sharing remain open until a workspace-specific test.

## Validation

- Focused proof: connection smoke test and displayed measure checks at each mart grain are pending.
- Integration or end-to-end proof: five-number dashboard-to-OLTP reconciliation and reader check are pending.
- Repository-required checks: `git diff --check` passed for tracked files; new documents were reviewed for trailing whitespace.

## Result

Preparation only. Stage 7 report implementation and evidence remain open.
