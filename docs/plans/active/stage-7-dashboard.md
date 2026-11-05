# Execution Plan: Stage 7 Power BI dashboard and reconciliation

Date: 2026-11-02

## Status

Active — five-page PBIP and Import semantic model built; all five empty-data layouts reviewed in Desktop. Data load and numerical reconciliation remain blocked by Databricks compute limits.

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

1. Use Import, as selected by the owner, for the six marts and Ops. Connect Power BI Desktop to the Databricks SQL warehouse and read `mart_daily_sales` plus `ops_pipeline_runs`. Record the server/HTTP path setup without secrets, then check how the report can be shared and refreshed.
2. Establish a small semantic model over the six marts and Ops. Confirm each table's grain and date column. Keep Product and RFM independent of the Executive purchase-date filter; keep funnel session date separate.
3. Build the five pages in `PLAN.md`: Executive/Sales, Product, Customer/RFM, Journey & Experience, and Data Quality/Ops. Label the funnel as synthetic and show RFM `as_of_date`.
4. Check measures at display grain. Recompute AOV, cancellation rate, and late-delivery rate from summed numerators and denominators; do not average stored rates. Default date slicers to dates present in Olist.
5. Select five visible numbers and document dashboard → mart → fact → OLTP SQL reconciliation, then have a reader inspect page 1 without explanation.

## Risks And Recovery

- The Databricks Free Edition warehouse currently rejects the first Import refresh because this workspace is no longer eligible for Serverless Compute. Keep the report local and data-empty until a working warehouse is available. An export route requires a separate decision and reconciliation check.
- Mixing marts with different grains can multiply measures or apply false filters. Use the page-specific filter contracts in `PLAN.md` and compare displayed totals to the source marts.
- A report file may retain connection metadata. Do not commit or share credentials; inspect the artifact before publishing.
- Stage 6 carryover: before claiming an automated Ops dashboard, reconcile a final Airflow `success` run ID with `ops_pipeline_runs`, then separately test repeated-logical-date and unattended operation if those claims are needed.

## Progress

- [x] Record the owner's Power BI choice and the existing five-page/mart scope.
- [x] Build local PBIP with five pages, six marts plus Ops, and Import storage mode.
- [x] Validate PBIR structure and parse the TMDL model with Microsoft Power BI Modeling MCP.
- [x] Open the PBIP in Power BI Desktop and review screenshots of all five page layouts without imported data (2026-11-05).
- [ ] Prove Power BI Desktop connection to one mart and Ops; decide Import/DirectQuery and refresh/sharing path.
- [ ] Build and inspect five report pages.
- [ ] Reconcile five dashboard numbers to mart, fact, and OLTP.
- [ ] Validate report usability and document the result.

## Decisions

- 2026-11-02: The owner selected Power BI. Connection mode and sharing remain open until a workspace-specific test.
- 2026-11-03: The owner selected Import to avoid per-visual Databricks queries. The first load and later refresh still require SQL compute. The owner has no second workspace/warehouse; retain the PBIP and record the data-load block.
- 2026-11-05: Power BI Desktop is installed through Microsoft Store. Its executable was discovered and `PBI_DESKTOP_PATH` configured in the user environment. The warehouse start request returned `hit your free daily limit`; keep the approved Databricks Import source and retry when compute is available.

## Validation

- Focused proof: PBIR validation passed with 0 errors/0 warnings; Power BI Modeling MCP loaded 7 tables and 26 measures from TMDL. Power BI Desktop opened the PBIP and rendered five layouts with empty visuals. A targeted full refresh of `mart_daily_sales` failed with HTTP 400: serverless compute disabled in the workspace's global warehouse config. Direct Databricks SQL connection reported that the workspace is no longer eligible for Serverless Compute. No data-bearing connection smoke test or displayed measure check has passed.
- 2026-11-05 focused proof: fresh PBIR validation passed with 0 errors/0 warnings. The locally launched Microsoft Power BI Authoring MCP connected to the PBIP TMDL folder and listed seven Import tables and 26 measures. MCP also retrieved AOV, cancellation, late-delivery, weighted delivery-days/review-score, and synthetic purchase-rate DAX; their definitions match the approved mart aggregation rules. Power BI Desktop opened the current PBIP through `powerbi-report-author`; fresh screenshots of all five pages showed readable titles, slicers, KPI labels, tables, and the synthetic-funnel caveat, with no clipping observed. Every data visual is still empty. `databricks warehouses start` failed with the free daily limit message, so no current data load or numerical proof was possible.
- Integration or end-to-end proof: five-number dashboard-to-OLTP reconciliation and reader check are pending.
- Repository-required checks: `git diff --check` passed for tracked files; new documents were reviewed for trailing whitespace.

## Result

Report structure and Import model exist locally. Stage 7 remains open until data loads, five pages are reviewed with data, and 5/5 displayed numbers reconcile to Gold and OLTP.
