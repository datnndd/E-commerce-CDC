# Execution Plan: Stage 7 Power BI dashboard and reconciliation

The Databricks refresh path is historical. The report now reads Athena through
the AWS path tracked by [AWS Glue migration](../active/aws-glue-migration.md).

Date: 2026-11-02

## Status

Completed for the local Power BI Import report on 2026-11-10. All seven
Athena-backed tables loaded, five pages rendered, five Executive cards and a
separate random sample of five numbers reconciled, page-specific filters were
exercised, and the owner confirmed an independent reader understood page 1.
Power BI Service publication and gateway refresh remain unverified.

## Outcome

Deliver a five-page Power BI report over six Athena Gold business marts and
`ops_pipeline_runs`, with five visible numbers traced to marts, facts, and the
OLTP source baseline.

## Context

- `PLAN.md` Stage 7 defines the five pages, allowed filters, KPI formulas, and reconciliation goal. `docs/decisions/0002-power-bi-dashboard.md` records the owner's Power BI choice.
- Stage 5 built and tested six marts. Decision 0004 replaced the Databricks
  serving path with AWS Glue and Athena while retaining Power BI Import.
- The test-bucket AWS manual run populated the Athena marts and four current Ops
  run objects. Main-path cutover and unattended scheduling remain in the AWS
  migration plan.

## Scope

In scope: Power BI Desktop connection proof; a report using the existing mart grains; five planned pages; page-specific filters and source-date defaults; five-number reconciliation; report instructions and captured evidence.

Out of scope: new marts, new attribution logic, real clickstream conversion, unsupported refund or product-rating metrics, and claiming scheduled refresh or public sharing before verification.

## Approach

1. Use Import for the six Athena marts and Ops through the
   `EcommerceCDC_Athena` ODBC DSN. Verify all seven partitions are populated,
   and record connection settings without secrets.
2. Establish a small semantic model over the six marts and Ops. Confirm each table's grain and date column. Keep Product and RFM independent of the Executive purchase-date filter; keep funnel session date separate.
3. Build the five pages in `PLAN.md`: Executive/Sales, Product, Customer/RFM, Journey & Experience, and Data Quality/Ops. Label the funnel as synthetic and show RFM `as_of_date`.
4. Check measures at display grain. Recompute AOV, cancellation rate, and late-delivery rate from summed numerators and denominators; do not average stored rates. Default date slicers to dates present in Olist.
5. Select five visible numbers and document dashboard → mart → fact → OLTP SQL reconciliation, then have a reader inspect page 1 without explanation.

## Risks And Recovery

- The Athena ODBC DSN must use the configured catalog and schema. With driver
  2.5, `UseSingleCatalogAndSchema=1` avoids the catalog-listing `null`-to-Record
  failure; Power BI credentials must use **Use Data Source Configuration** for
  the IAM-backed DSN. Service refresh also needs an installed gateway/System
  DSN and separate live validation.
- Mixing marts with different grains can multiply measures or apply false filters. Use the page-specific filter contracts in `PLAN.md` and compare displayed totals to the source marts.
- A report file may retain connection metadata. Do not commit or share credentials; inspect the artifact before publishing.
- Stage 6 carryover: before claiming an automated Ops dashboard, reconcile a final Airflow `success` run ID with `ops_pipeline_runs`, then separately test repeated-logical-date and unattended operation if those claims are needed.

## Progress

- [x] Record the owner's Power BI choice and the existing five-page/mart scope.
- [x] Build local PBIP with five pages, six marts plus Ops, and Import storage mode.
- [x] Validate PBIR structure and parse the TMDL model with Microsoft Power BI Modeling MCP.
- [x] Open the PBIP in Power BI Desktop and review screenshots of all five page layouts without imported data (2026-11-05).
- [x] Refresh all seven Athena Import partitions and query one mart and Ops in
  Power BI Desktop.
- [x] Inspect all five report pages with populated visuals.
- [x] Reconcile five unfiltered Executive numbers to Athena marts/facts and the
  frozen OLTP baseline.
- [x] Reconcile a seeded random sample of five visible numbers across
  Executive and Product against the model, Athena, and frozen OLTP baseline.
- [x] Exercise page-specific filters and restore each to `All`; confirm Product
  and RFM stay independent of the Executive state filter.
- [x] Have an independent reader inspect page 1 without explanation (owner
  confirmation on 2026-11-10). Service publication/gateway refresh is separate.

## Decisions

- 2026-11-02: The owner selected Power BI. Connection mode and sharing remain open until a workspace-specific test.
- 2026-11-03: The owner selected Import to avoid per-visual Databricks queries. The first load and later refresh still require SQL compute. The owner has no second workspace/warehouse; retain the PBIP and record the data-load block.
- 2026-11-05: Power BI Desktop is installed through Microsoft Store. Its executable was discovered and `PBI_DESKTOP_PATH` configured in the user environment. The warehouse start request returned `hit your free daily limit`; keep the approved Databricks Import source and retry when compute is available.
- 2026-11-10: The owner retained seven Import tables on Athena. Desktop uses
  `Use Data Source Configuration` for the IAM-backed ODBC DSN; a prior cached
  AAD Authentication choice produced the `null`-to-Record refresh error.

## Validation

- Historical 2026-11-05 proof: PBIR validation passed with 0 errors/0 warnings; Power BI Modeling MCP loaded 7 tables and 26 measures from TMDL. Power BI Desktop opened the PBIP and rendered five layouts with empty visuals. A targeted full refresh of `mart_daily_sales` failed with HTTP 400: serverless compute disabled in the workspace's global warehouse config. Direct Databricks SQL connection reported that the workspace was no longer eligible for Serverless Compute. No data-bearing connection smoke test or displayed measure check had passed at that point.
- 2026-11-05 focused proof: fresh PBIR validation passed with 0 errors/0 warnings. The locally launched Microsoft Power BI Authoring MCP connected to the PBIP TMDL folder and listed seven Import tables and 26 measures. MCP also retrieved AOV, cancellation, late-delivery, weighted delivery-days/review-score, and synthetic purchase-rate DAX; their definitions match the approved mart aggregation rules. Power BI Desktop opened the current PBIP through `powerbi-report-author`; fresh screenshots of all five pages showed readable titles, slicers, KPI labels, tables, and the synthetic-funnel caveat, with no clipping observed. Every data visual is still empty. `databricks warehouses start` failed with the free daily limit message, so no current data load or numerical proof was possible.
- 2026-11-10 live Desktop proof: Modeling MCP reported all seven Athena Import
  partitions `Ready`. Row counts are daily sales 10,742; product 33,659; RFM
  93,358; funnel 20; payment 2,276; experience 10,608; Ops 4. Desktop rendered
  populated visuals on Executive, Product, Customer/RFM, Journey/Experience,
  and Data Quality/Ops. The Executive purchase-date and state slicers showed
  `All`; Product and RFM remained all-time/current views.
- Five Executive card values matched Power BI DAX, live Athena mart and
  `fct_orders` aggregates, and the frozen Olist OLTP baseline:

  | Card | Exact value | Source baseline / calculation |
  | --- | ---: | --- |
  | Delivered item revenue | BRL 13,221,498.11 | Delivered-order item price, summed across 12 OLTP calendar months |
  | Orders | 99,441 | `orders` count |
  | Delivered orders | 96,478 | `orders` where status is `delivered` |
  | AOV delivered item | BRL 137.04 | 13,221,498.11 / 96,478 |
  | Cancellation rate | 0.6285% (card: 0.6%) | 625 canceled / 99,441 orders |

  The DAX query evaluated the five card measures with both Executive slicers
  on `All`. Athena then returned the same four base values from both
  `bi_aws.mart_daily_sales` (sums of revenue, orders, delivered, canceled) and
  `bi_aws.fct_orders` (count and conditional sums by `order_status`);
  AOV and cancellation rate were recomputed from those totals. The captured
  read-only OLTP SQL results in
  `docs/baselines/olist_20261023T032906Z_99dc95d7.json` provide
  `metrics.orders_by_status` (99,441 total, 96,478 delivered, 625 canceled)
  and `exploration.delivered_item_price_by_calendar_month` (12 rows summing to
  BRL 13,221,498.11 from delivered `orders` joined to `order_items`). Local
  PostgreSQL was not running for a new source query. Power BI Service/gateway
  refresh has not been observed.
- 2026-11-10 seeded random sample: from the five Executive cards and the ten
  Product category revenues in the frozen OLTP baseline, PowerShell
  `Get-Random -Count 5 -SetSeed 20261110` selected Executive revenue,
  Product `computers_accessories`, Product `health_beauty`, Executive AOV,
  and Product `cool_stuff`. Exact values were respectively BRL 13,221,498.11,
  BRL 888,724.61, BRL 1,233,131.72, BRL 137.0415857501192, and
  BRL 610,204.10. Power BI DAX returned the exact card/category measures;
  Athena returned the three category sums from both
  `bi_aws.mart_product_performance` and `bi_aws.fct_order_items` joined to
  `analytics_aws_gold.dim_category`; the frozen OLTP baseline has the same
  category sums. Executive revenue and AOV were independently reconciled
  above. The report displays rounded values on cards and bars.
- 2026-11-10 filter check in the reopened saved PBIP: Executive customer state
  `AC` changed Orders from 99,441 to 81, revenue to about BRL 15,930, and AOV
  to BRL 199.14, while Product and RFM retained their all-time totals.
  Executive purchase date `2016-10-02` changed Orders to 1 and cancellation
  rate to 100%; Product category `art` changed Items sold to 197 and revenue
  to BRL 23,657.30; RFM `Champions` changed monetary to BRL 305,120.
  Journey payment purchase date `2016-10-02` changed Payment value from
  about BRL 16.01M to BRL 109.34 while delivery and synthetic session KPIs
  stayed unchanged. Ops run-time selection reduced the run table to one row.
  All tested slicers were restored to `All`. The five pages reopened with
  populated data and source-date defaults. The owner reported that an
  independent reader understood page 1 without explanation.
- Repository-required checks: `git diff --check` passed for tracked files; new documents were reviewed for trailing whitespace.

## Result

Stage 7's local Import report and reconciliation criteria are met. Power BI
Service publishing and refresh are separate unverified capabilities. The AWS
migration plan remains active for isolated negative/retry proof and main cutover.
