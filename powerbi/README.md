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

Power BI source expressions navigate `AwsDataCatalog` → `AthenaDatabase` →
the six marts and Ops table. The Ops JSON timestamps are converted to UTC
datetime values during Import. Product and RFM remain all-time/current views
without the Executive purchase-date filter. Journey dates stay separate, and
the synthetic funnel remains labeled as synthetic.

## Current evidence

The model has seven Import partitions and 26 measures; its Athena TMDL was
serialized by Power BI Modeling MCP. The previous five-page layout review used
empty data. AWS credentials expired before the new source could be queried;
this machine has no Athena ODBC DSN. No Athena-backed Import refresh, populated
page review, KPI reconciliation, Service refresh, or sharing has passed yet.

Follow the [cutover runbook](../docs/runbooks/aws-glue-cutover.md) to refresh
all seven tables and reconcile five visible numbers through mart, Gold fact,
and OLTP before calling the dashboard complete.
