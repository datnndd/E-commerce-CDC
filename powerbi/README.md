# Stage 7 Power BI report

Open [EcommerceCDC.pbip](EcommerceCDC.pbip) in Power BI Desktop. The report has
five pages over six Gold business marts and `ops.ops_pipeline_runs`. Its semantic
model uses **Import** for all seven tables and the Databricks connector with
OAuth. No credential or token is stored in this project.

The business pages read `ecommerce_cdc.analytics_dev_gold`; the Ops page reads
`ecommerce_cdc.ops`. Product and RFM are all-time/current-snapshot views and do
not share the Executive purchase-date filter. The Journey page keeps payment,
delivery, and synthetic-session dates separate. The synthetic funnel is not a
real customer conversion measure.

## Current state (2026-11-03)

- The PBIR validator passed with zero errors and zero warnings. Microsoft Power
  BI Modeling MCP parsed the TMDL model with seven tables and 26 measures.
- Power BI Desktop opened the PBIP and displayed all five page layouts. The
  imported tables have **no data yet**; blank KPI values and charts are expected.
- The first Import refresh of `mart_daily_sales` reached the Databricks
  connector, but the SQL warehouse rejected it: serverless compute is disabled
  for this workspace. A direct SQL connector attempt also reported that the
  workspace is no longer eligible for Serverless Compute. The only listed SQL
  warehouse is stopped, and there is no listed cluster.
- Refresh, rendered values, five-number reconciliation, reader review,
  scheduled refresh, and sharing are **not verified**.

Import caches source data in Power BI after a successful refresh, so changing
slicers does not query Databricks for every visual. The initial load and every
later refresh still require a working Databricks SQL warehouse.

## Resume after compute is available

1. Open the PBIP and sign in to the Databricks connector with OAuth if asked.
2. Refresh the seven imported tables. Check that both `mart_daily_sales` and
   `ops_pipeline_runs` have rows before reviewing the five pages.
3. Inspect every page with data, including the date slicers, table sorting,
   KPI labels, and synthetic-clickstream warning.
4. Reconcile five displayed numbers through mart, Gold fact, and OLTP SQL as
   required by [PLAN.md](../PLAN.md). Record the queries and results before
   marking Stage 7 complete.

The report is a local PBIP project. Publishing and unattended Power BI Service
refresh have not been configured or tested.
