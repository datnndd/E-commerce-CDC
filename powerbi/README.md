# Stage 7 Power BI report

Open [EcommerceCDC.pbip](EcommerceCDC.pbip) in Power BI Desktop. The report
retains five pages and seven Import tables: six Gold business marts and
`ops_pipeline_runs`. The semantic model uses the Amazon Athena connector. Its
`AthenaDSN` parameter defaults to `EcommerceCDC_Athena`; configure a matching
Amazon Athena ODBC DSN on the refresh machine. `AthenaDatabase` defaults to
`bi_aws` and must match the `BI_DATABASE` used for the AWS main run. For an
isolated rehearsal, temporarily set it to that run's BI database.

The DSN needs the project AWS region, catalog `AwsDataCatalog`, Athena v3
workgroup, query-results S3 location, and working AWS authentication. No
credential is stored in this PBIP. For Power BI Service refresh, install the
Athena ODBC driver and an on-premises data gateway on the gateway machine and
configure a **System DSN** there with the same name and working credentials.
Desktop can use a user or system DSN. The [connector instructions](https://learn.microsoft.com/en-us/power-query/connectors/amazon-athena)
describe the driver, DSN, Import, and gateway requirements.

For the Athena ODBC 2.5 driver, set `UseSingleCatalogAndSchema=1` with
`Catalog=AwsDataCatalog` and `Schema=bi_aws` (or the selected BI database).
Power BI Desktop's **Data source settings → Edit Permissions → Credentials**
must use **Use Data Source Configuration** when the DSN authenticates with IAM
credentials. A cached **AAD Authentication** choice produced `We cannot convert
the value null to type Record` even after direct ODBC SELECTs worked. Keep the
AWS access key and secret out of the PBIP and this document.

Power BI source expressions navigate `AwsDataCatalog` → `AthenaDatabase` →
the six marts and Ops table. The Ops JSON timestamps are converted to UTC
datetime values during Import. Product and RFM remain all-time/current views
without the Executive purchase-date filter. Journey dates stay separate, and
the synthetic funnel remains labeled as synthetic.

## Current evidence (2026-11-10)

Power BI Desktop refreshed all seven Athena-backed Import partitions; Modeling
MCP reported each partition `Ready` and counted 10,742 daily-sales rows, 33,659
product rows, 93,358 RFM rows, 20 funnel rows, 2,276 payment rows, 10,608
experience rows, and four Ops runs. All five pages rendered with populated
visuals. The five unfiltered Executive cards matched Athena mart and fact
aggregates and the frozen Olist OLTP baseline: delivered item revenue BRL
13,221,498.11, 99,441 orders, 96,478 delivered orders, AOV BRL 137.04, and
cancellation rate 625 / 99,441 (displayed as 0.6%). The OLTP comparison uses
the recorded baseline, not a fresh PostgreSQL query.

The saved PBIP reopened with populated data. A seeded random sample of five
numbers across Executive and Product matched Power BI model measures, Athena
mart/fact aggregates, and the frozen OLTP baseline. Executive, Product,
Customer/RFM, Journey, and Ops slicers were exercised and restored to `All`;
Product and RFM remained independent of the Executive state filter. The owner
confirmed that an independent reader understood page 1 without explanation.
Values and proof are in the [completed Stage 7 plan](../docs/plans/completed/stage-7-dashboard.md).

Power BI Service publication, gateway refresh, and sharing have not been
verified. The AWS main-path cutover and unattended DAG schedule are tracked
separately in the [active migration plan](../docs/plans/active/aws-glue-migration.md).

Follow the [cutover runbook](../docs/runbooks/aws-glue-cutover.md) to refresh
all seven tables and reconcile five visible numbers through mart, Gold fact,
and OLTP for a new source snapshot or main-path cutover.
