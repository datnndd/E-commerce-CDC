# Debezium/Kafka CDC upgrade

This runbook implements the owner-approved local-then-AWS upgrade in
[`debezium-kafka-upgrade.md`](../plans/active/debezium-kafka-upgrade.md).
Keep the existing polling DAG paused during rehearsal and retain its S3/Glue/BI
locations. Do not set `CDC_MODE=debezium` on the main Airflow deployment until
the isolated path and Power BI Import reconcile.

## Local isolated proof

Use `docker --context default compose` on this host; Docker Desktop does not
need to be started. The Connect image is pinned to Debezium `3.7.1.Final`.
The first 2026-11-14 snapshot ran on the previously cached `3.8.0.Alpha1`
container. A later Kafka/Connect restart on `3.7.1.Final` resumed the isolated
small connector and emitted another review mutation cycle through offset 8.

1. Copy `.env.example` to the ignored `.env` if needed. Keep the existing
   PostgreSQL password. Run `uv run python -m ingestion.debezium_setup
   --write-secret`; this writes an ignored `logs/cdc-secrets/postgres.properties`
   for Kafka Connect. Do not display or commit that file.
2. Start `docker compose --profile cdc up -d cdc-postgres kafka connect
   localstack cdc-sink`. This uses a separate PostgreSQL volume on host port
   5434, nine CDC topics on a single Kafka broker, and an S3-compatible local
   bucket under `stage9_local/`.
3. In the shell running the following commands, set `POSTGRES_HOST=127.0.0.1`,
   `POSTGRES_PORT=5434`, and `POSTGRES_DB=ecommerce_cdc_1c_test`. Run
   `uv run alembic -c sources/oltp/alembic.ini upgrade head` and
   `uv run python -m sources.oltp.load_olist --dataset olist_dataset` on this
   **empty isolated database only**. The loader rejects nonempty source tables.
4. Set `DEBEZIUM_DATABASE_HOST=cdc-postgres` and
   `DEBEZIUM_DATABASE_PORT=5432` for the connector. Run
   `uv run python -m ingestion.debezium_setup`. Check the connector and all
   nine topics; wait for snapshot records and committed manifests in LocalStack.
   Confirm each topic has one partition and that snapshot `r` events precede
   streaming changes.
5. Run `uv run python -m sources.oltp.simulate_cdc_review` against the isolated
   database to update an order review, insert a new review with a valid order,
   then hard-delete the new review. Confirm Debezium `u`, `c`, and `d`
   events and the subsequent null tombstone, S3 manifest offset continuity,
   Delta Silver deletion, Gold fact deletion, and the changed experience mart.
   Use a separate Silver/Gold/BI namespace and refresh the seven-table Power BI
   Import model only after Athena counts match.
6. Re-run the same Silver/Gold inputs and restart Connect and the sink once
   before and once after a manifest upload. Check that Kafka offsets and S3
   manifests have no gaps, Silver/Gold keys have no duplicates, and a deleted
   review stays absent. Stop only the isolated containers created for this run.

For a fast smoke test, create a separate `ecommerce_cdc_small_1c_test`
database in the isolated `cdc-postgres` server, migrate it, and run
`python -m sources.oltp.seed_cdc_small` with `POSTGRES_DB` set to that exact
name. Set distinct `CDC_CONNECTOR_NAME`, `CDC_TOPIC_PREFIX`,
`CDC_PUBLICATION`, `CDC_SLOT_NAME`, and `CDC_SINK_GROUP`, and use a
separate `PIPELINE_S3_PREFIX`. The seed provides one linked row in each of the
nine tables; it cannot establish full Olist snapshot capacity. If LocalStack
cannot be pulled, a local S3-compatible Moto server can verify the sink and
manifest contract with a test bucket and explicit fake credentials. Do not
reuse the production prefix or credentials.

If a large snapshot is stalled and a second isolated connector needs a slot,
use Connect's `PUT /connectors/<name>/stop` after checking its status.
`pause` may leave the snapshot task's transaction open. Record the offset and
slot state before resuming; do not delete a slot as a shortcut.

## AWS preflight and deployment

1. Authenticate AWS CLI and inspect the current account, `ap-southeast-1`
   RDS instance/parameter group, existing Airflow/Glue/BI locations, DAG pause
   state, IAM, and budget. The 2026-11-10 CLI session was expired; no AWS CDC
   prerequisites or deployment are yet verified.
2. On the RDS **primary**, verify `rds.logical_replication=1` is applied,
   `wal_level=logical`, available replication slots/WAL senders, and enough
   storage headroom. Apply a parameter-group change and reboot only in a
   planned maintenance window if needed. Use a dedicated replication account
   with `rds_replication` and `SELECT` on the nine tables. Create the publication
   with an owner/admin account via `--publication-only`; then use the replication
   account for the connector. Check `pg_replication_slots` and retained WAL
   (`pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)`) throughout rehearsal.
3. Provision a separate private EC2 with persistent EBS storage and an instance
   role limited to the isolated project S3 path. Allow broker 9092 only from
   Airflow/approved VPC clients, Connect 8083 only from the operator/Airflow
   private address, and RDS 5432 from the EC2 security group. Do not expose
   broker or Connect REST to the public internet. Ensure container access to
   IMDSv2 if the sink uses the instance role.
4. Place the ignored connector secret file and RDS CA bundle outside Git on
   that EC2; set `CDC_SECRET_DIR` to the mounted directory and mode 0600 on
   `postgres.properties`. Set `EC2_PRIVATE_IP`, stable `CDC_KAFKA_CLUSTER_ID`,
   `CDC_DATA_DIR` on EBS, project `S3_BUCKET`, region, and an isolated
   `PIPELINE_S3_PREFIX`. Use distinct connector, topic prefix, slot,
   publication, and sink group names for isolated and main sources so their
   Kafka offsets cannot be confused. Run
   `docker compose -f compose.cdc-ec2.yaml up -d --build`. Register the
   connector from the EC2 with `DEBEZIUM_DATABASE_HOST` set to the RDS primary
   and `DEBEZIUM_DATABASE_SSLMODE=verify-full`. Do not include a database
   password in the REST request; the config uses Kafka Connect FileConfigProvider.
5. Run the isolated Airflow path with `CDC_MODE=debezium`, private Kafka/Connect
   endpoints, separate Glue Silver/Gold/BI locations, and the isolated S3
   prefix. The CDC gate requires a running connector and committed Bronze
   through Kafka offsets observed at DAG start. Inspect source slot lag too;
   Kafka-offset readiness alone does not prove the source has caught up to a
   transaction committed moments before the gate.
6. Record Airflow/Glue run IDs; reconcile all nine source tables, 12 Silver
   tables, Gold facts, six marts, Athena, Ops, and all seven Power BI Import
   partitions. Run the insert/update/hard-delete review fixture and a failed
   sink/Glue recovery. Only then switch the main DAG and BI parameter to new
   namespaced roots. Keep the polling revision and old objects available for
   rollback; if any gate fails, pause the DAG and restore the old settings.

## Monitoring and recovery

- Check Connect task state, Kafka consumer lag, sink logs, manifest offset
  continuity, `pg_replication_slots.active`, retained WAL bytes, RDS free
  storage, and EC2 EBS free space. A stopped slot can retain WAL without bound.
- A data object without `_manifest.json` is uncommitted and ignored. Restart
  the sink; it verifies a previously published batch before committing its
  Kafka offset. Do not delete a replication slot or S3 object during recovery
  until the event range and downstream checkpoint are reconciled.
- Glue stores each table's applied offset only after the full Silver gate.
  Inspect Delta history after a partial run, correct the failure, and rerun the
  same isolated input. Gold and Power BI must not advance after Silver fails.
