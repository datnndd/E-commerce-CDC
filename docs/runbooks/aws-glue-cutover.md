# AWS Glue cutover runbook

This runbook follows [decision 0004](../decisions/0004-aws-glue-pipeline.md).
Keep `ecommerce_cdc_daily` paused until the isolated rehearsal and main run
both reconcile. Record each real run ID and the observed counts in the active
[migration plan](../plans/active/aws-glue-migration.md).

## Prerequisites

- Confirm an authenticated AWS identity in the intended account and
  `ap-southeast-1`, a project S3 bucket, an AWS Glue execution role, and an
  Athena workgroup whose effective engine is v3. The role needs read/write on
  the isolated and main Bronze/Silver/Gold/Ops paths plus Glue Catalog access.
  The Airflow identity also needs `iam:PassRole`, Glue Job run, Athena query,
  Glue Catalog, and S3 result access. Keep credentials out of Git.
- Back up the ignored `.env`, confirm the intended PostgreSQL database and
  connectivity, and configure the actual values listed in `.env.example`.
  For an isolated rehearsal set `PIPELINE_S3_PREFIX`, separate Silver/Gold/BI
  databases and S3 roots, and the `*_1c_test` source database. For the main
  run, remove the prefix and use new AWS roots, not the Databricks Silver path.
- Package and upload the exact job code using
  `uv run python -m orchestration.package_glue_job --output-root dist/glue`.
  Upload its `silver_transform.py` and `spark_jobs.zip` to versioned project
  S3 keys. Deploy `infra/glue_silver_job.yaml` with the real job name, role ARN,
  both uploaded locations, and a dedicated S3 TempDir. Inspect the deployed
  Glue version (`6.0`), `--datalake-formats=delta`, Delta `--conf`, GX module,
  and library location before starting a DAG run. `PythonVersion: '3'` is the
  Glue API selector; Glue 6.0 supplies Python 3.13. Do not install a separate
  `delta-spark` package or JAR into the Glue job or dbt session. The Delta
  `--conf` contains the extension and catalog settings; the global LogStore
  override is deprecated. Keep one active writer per Delta table.
- Confirm Airflow has parsed the updated DAG and the dbt profile has the real
  role, schema, S3 location, and Delta Spark configuration. `runtime_check`
  rejects missing values and a reused Silver path, but cannot prove IAM grants.

## Isolated rehearsal

1. Keep the main DAG paused. Use the existing isolated PostgreSQL source and
   `PIPELINE_S3_PREFIX` to run Bronze → Glue Silver → dbt build → Athena publish
   manually in Airflow. Do not use the main S3/database names for this step.
2. Match the Airflow run ID to the Glue Job run and S3 `ops/runs/` object.
   Inspect committed Bronze manifests, all 12 Silver Delta tables, quarantine,
   their 13 Glue Catalog registrations and S3 locations, dbt build results,
   13 Athena Delta SELECT counts, and the Ops row through Athena. A successful
   Ops row must have measured Silver/Gold/mart/quarantine counts. Check the
   result against source and mart grain, not only task status. Check the Delta
   reader protocol and reader features for Athena compatibility; Glue 6.0's
   bundled Delta 4.2.0 alone does not prove Athena can read each table.
3. Re-run with the same logical date and inspect duplicate keys at Silver and
   Gold grains. Run the approved negative payment fixture in the isolated
   source; Great Expectations must fail before Gold, and the Ops record must
   say `failed` with unknown downstream counts as null. Restore the fixture
   before the main run.
4. Configure an Athena ODBC DSN on the Desktop refresh machine, point the
   Power BI `AthenaDatabase` parameter at the isolated BI database, refresh all
   seven Import tables, and inspect all five pages with data. Reconcile five
   displayed values to mart, Gold fact, and OLTP queries. Use a System DSN on
   the gateway machine if Service refresh is required.

## Main cutover

1. Set the main project bucket and new AWS Silver/Gold/BI locations in `.env`,
   remove the isolated prefix, restart Airflow, and verify the parsed DAG and
   `runtime_check`. Keep the schedule paused and trigger one manual run.
2. Reconcile its manifests, quality gate, Glue Catalog locations, dbt build,
   Athena SELECTs, Ops `success` row, and source-to-mart counts. Set the Power
   BI `AthenaDatabase` parameter to the main BI database, refresh all seven
   Import tables, and inspect five pages and the five agreed KPIs.
3. Unpause the schedule only after the main run and Ops status agree. If any
   boundary fails, keep the schedule paused, preserve the run artifacts, and
   diagnose the failing task. Correct and rerun in the same isolated namespace
   before another main attempt. Multi-table Delta writes are not atomic;
   inspect table histories before targeted recovery.

No live AWS or Power BI evidence is recorded merely by following this text.
