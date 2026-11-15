# Execution Plan: Debezium and Kafka CDC upgrade

Date: 2026-11-10

## Status

Active. Local stream and replay proof is in progress; AWS identity remains expired.

## Outcome

PostgreSQL changes, including physical deletes, flow through Debezium, Kafka,
manifest-committed S3 Bronze, Delta Silver, dbt Gold/marts, Athena, and Power BI
Import. Polling and existing data remain available until a reconciled cutover.

## Context

- The owner approved the local-then-AWS, separate-EC2, end-to-end plan in this
  conversation. `PLAN.md` Stage 8 names Debezium/Kafka but its checklist remains
  unchecked; the owner says Stage 8 is otherwise complete.
- `docs/decisions/0004-aws-glue-pipeline.md` governs Glue/Athena and the paused
  main schedule. `docs/plans/active/aws-glue-migration.md` still lacks a recorded
  main cutover and negative/same-date rerun proof.
- `ingestion/cdc_extract.py` is the retained polling path;
  `spark_jobs/silver_transform.py` currently reads its manifest Parquet and
  orders by `updated_at`. Gold facts use incremental merge without source deletes.

## Scope

In scope: Debezium/Kafka source and sink, a separate Bronze path, delete-aware
Silver and Gold, isolated local and AWS proof, operational guidance, and a
reconciled dashboard cutover.

Out of scope: deleting polling code, old S3 objects, or Databricks history.

## Approach

1. Implement local Kafka/Connect, nine single-partition topics, a PostgreSQL
   connector, and a manifest-committing Kafka-to-S3 consumer.
2. Add a separate Debezium Bronze reader and offset-ordered Delta MERGE with
   hard deletes and replay-safe applied-offset state.
3. Propagate deletes through incremental Gold facts and dependent marts, then
   wire a selectable Airflow CDC path without switching the paused main DAG.
4. Prove snapshot, insert, update, delete, restart, replay, and failures locally.
5. After AWS authentication is restored, inspect RDS prerequisites and existing
   state, deploy on a separate private EC2, run isolated Glue/dbt/Athena/Power BI
   proof, then reconcile and switch the main path.

## Risks And Recovery

- A stopped logical slot retains WAL. Monitor slot lag and free storage; stop
  deployment before affecting the source. RDS logical replication may require a
  parameter-group change and reboot.
- A partially published S3 batch has no manifest and is ignored. A replay
  verifies the existing deterministic batch before committing Kafka offsets.
- Partial Delta multi-table writes are not atomic. Keep the DAG paused, inspect
  Delta histories, and rerun from the last applied offsets in isolated roots.
- Keep old polling deployment and Bronze/Silver/BI namespaces for rollback;
  never overwrite or delete them as part of this upgrade.

## Progress

- [x] Inspect repository Stage 8, current CDC/Silver/Gold/DAG, and AWS identity.
- [x] Implement local source, sink, Silver, Gold, and orchestration changes (pending integration proof).
- [x] Prove isolated nine-topic Kafka-to-Bronze snapshot, review mutation,
  restart, Silver MERGE/replay, and the Gold delete hook locally.
- [ ] Prove full Olist snapshot capacity, execute the dbt fact and dependent
  experience mart, and reconcile dashboard output in an isolated environment.
- [ ] Inspect live AWS prerequisites and deploy isolated EC2/RDS integration.
- [ ] Reconcile Glue, Gold, Athena, Ops, and Power BI; cut over main path.

## Decisions

- 2026-11-10: The owner chose local then AWS, a separate EC2, Power BI Desktop
  as the acceptance endpoint, incremental Gold delete propagation, and current
  KPI removal for hard deletes.
- 2026-11-10: Preserve the polling implementation and existing objects as the
  recovery path; the new stream receives a separate Bronze namespace.
- 2026-11-14: Use the working Docker Engine `default` context, without starting
  Docker Desktop. Use the separately seeded `ecommerce_cdc_small_1c_test` for
  fast local failure proof; retain the full Olist isolated source and volumes.
- 2026-11-14: Pin Connect to Debezium `3.7.1.Final`, the stable release listed
  by Debezium. The first Bronze snapshot was produced by `3.8.0.Alpha1`;
  a later restart on `3.7.1.Final` continued the same small connector/slot.

## Validation

- Initial AWS preflight: `aws sts get-caller-identity` and RDS read-only query
  both failed with an expired session. No live AWS claim can be made yet.
- Local isolated PostgreSQL was migrated to Alembic head and loaded with all nine
  Olist tables (99,441 customers; 1,000,163 geolocation; 71 translations;
  32,951 products; 3,095 sellers; 99,441 orders; 112,650 items; 103,886
  payments; 99,224 reviews). No source data or old Bronze object was removed.
- Final offline suite: `uv run python -m unittest discover -s tests -q` passed
  99 tests, with 21 environment-gated tests skipped. The relevant local gated
  Kafka/S3, Silver, and Gold cases were run separately and passed as below.
  `uv run python -m compileall -q ingestion orchestration sources spark_jobs
  airflow/dags tests`, `uv run dbt parse --project-dir dbt_project
  --profiles-dir dbt_project`, and `git diff --check` passed (Git emitted only
  line-ending conversion warnings).
- 2026-11-14 local stream proof: Docker Engine `default` started PostgreSQL,
  Kafka, and Connect. The full Olist connector started a snapshot but repeatedly
  rebalanced while reading the large source; full snapshot remains unproved.
  The small isolated source produced snapshot `r` records for all nine
  single-partition topics and review offsets 0–4 (`r,u,c,d,null`). The
  Kafka-to-S3 consumer committed ten manifests (nine snapshot batches and one
  review change batch) to Moto S3 under `stage9_small`. The gated
  `tests.test_cdc_local_integration` passed, checking source counts, topic
  partitions, manifest integrity/continuity, broker high-water, before/after,
  and LSN. Moto was used because the LocalStack image pull again failed with
  Docker Hub EOF. This Moto server is in-memory; the observed Bronze objects
  were copied to the WSL test root for Delta replay, not promoted to AWS S3.
- 2026-11-14 Silver proof: actual Connect JSON includes schema/payload wrappers;
  the reader now unwraps them. A nine-table replay exposed a Spark schema
  inference conflict between all-null snapshot `before` and streaming
  `before`; the reader now infers across verified files together.
  `tests.test_silver_cdc_local` passed with wrapped snapshot/delete, plain
  update/reinsert, tombstone, replay, and checkpoint. The gated
  `tests.test_silver_cdc_local_integration` passed against the copied actual
  Bronze in WSL Spark/Delta (169.889 s): all nine Silver tables had one row,
  review id 1 had score 5, review id 2 remained deleted across replay, and
  checkpoints prevented duplicate application.
- 2026-11-14 failure proof: `tests.test_kafka_to_s3` passed three cases,
  including a simulated failure after the data PUT and before manifest PUT,
  followed by a retry. After pulling `3.7.1.Final`, Kafka and Connect restarted
  on retained volumes. REST reported the small connector and task `RUNNING`
  at `3.7.1.Final`. A second source fixture committed update/insert/delete;
  the restarted sink published through review offset 8 without a gap. The
  gated `tests.test_cdc_local_integration` passed with two review cycles and
  checked source state against all nine Bronze topics. Cloud Gold/BI proof
  remains.
- 2026-11-14 resumed Silver proof: copied the new review Bronze batch to WSL.
  `tests.test_silver_cdc_restart_local` passed (54.541 s), starting from
  checkpoint 4, applying offsets 5–8, and reaching checkpoint 8. Review id 1
  changed from score 5 to 4 at offset 5; ids 2 and 3 stayed absent. A rerun
  returned no new work. The isolated containers were stopped afterward with
  volumes retained.
- The gated `tests.test_gold_cdc_local` passed (33.201 s), executing the actual dbt
  `sync_deleted_keys` macro against local Delta SQL twice. A stale review fact
  was deleted, the current review score average became 5.0, and replay kept
  one row. This is a Gold delete-hook fixture, not an AWS dbt model or full
  experience mart run.
- AWS CLI still reports an expired session (`aws login` required), so RDS,
  Glue, Athena, EC2 and Power BI cutover remain unattempted. No AWS run ID
  exists for this upgrade.

## Result

The isolated local CDC stream, delete-aware Silver replay, and Gold delete hook
have executable proof. Full Olist capacity, AWS deployment, dbt mart, Athena,
Ops and Power BI reconciliation remain open; the main polling path and objects
are retained, and no cutover has occurred.
