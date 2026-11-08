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

Airflow writes Bronze to the project S3 bucket, invokes a Glue 6.0 Delta Silver
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

Per [AWS Glue 6.0 release notes](https://docs.aws.amazon.com/glue/latest/dg/release-notes.html),
Glue 6.0 supplies Spark 4.1.1, Python 3.13, and Delta Lake 4.2.0. The Silver
job and dbt interactive sessions use Glue's bundled Delta libraries with
`datalake-formats: delta` and the Delta Spark configuration. Do not add a
second Delta JAR or Python package to those runtimes. Let Delta select its
default S3 LogStore; [Delta's storage documentation](https://docs.delta.io/delta-storage/)
marks the global `spark.delta.logStore.class` setting deprecated. This assumes
one Spark driver writes any given table at a time.
Athena access remains conditional on each table's Delta reader protocol and
reader features.

## Consequences

The Glue jobs require an IAM role with S3, Glue Catalog, and Athena permissions,
packaged code, and a working Delta/GX dependency set. Power BI Desktop needs
the Athena ODBC driver and DSN; Service refresh also needs a gateway. Separate
namespaces prevent accidental overwrite, but a partial multi-table Delta run
still needs inspection and a corrected rerun.
