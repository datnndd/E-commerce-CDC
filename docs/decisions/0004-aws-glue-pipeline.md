# 0004 AWS Glue pipeline and Athena dashboard source

Date: 2026-11-07

## Status

Accepted for implementation; live validation and main cutover pending.

## Context

The Databricks Silver job was limited by Free Edition compute. The owner chose
to complete the AWS migration, including Power BI, and to prove an isolated run
before switching the main path. This replaces the execution choice in 0003 and
the Databricks source choice in 0002; their observed results remain historical.

## Decision

Airflow writes Bronze to the project S3 bucket, invokes a Glue 5.0 Delta Silver
job, then runs dbt-glue for Gold and marts. Silver and Gold use new S3 roots and
Glue databases. Silver catalog registration follows the full quality gate and
must reject any existing table at a different location. Athena engine v3 reads
Delta marts and an S3 JSON Ops run table through Glue Catalog. One stable S3
object per DAG run is the Ops source of truth; unmeasured metrics are null.

Power BI retains seven Import tables, six business marts plus Ops, and five
pages. Its source is the Amazon Athena connector through an ODBC DSN. Service
refresh requires a gateway configured for that DSN.

The main schedule remains paused until an isolated AWS rehearsal, a controlled
main run, Ops reconciliation, and a seven-table Power BI refresh pass. Existing
Databricks resources and data are retained.

## Consequences

The Glue jobs require an IAM role with S3, Glue Catalog, and Athena permissions,
packaged code, and a working Delta/GX dependency set. Power BI Desktop needs
the Athena ODBC driver and DSN; Service refresh also needs a gateway. Separate
namespaces prevent accidental overwrite, but a partial multi-table Delta run
still needs inspection and a corrected rerun.
