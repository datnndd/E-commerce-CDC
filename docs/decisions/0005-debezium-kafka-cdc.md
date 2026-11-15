# 0005 Debezium/Kafka CDC beside polling

Date: 2026-11-10

## Status

Accepted for implementation; isolated and main live validation pending.

## Context

The PostgreSQL timestamp-polling extractor cannot observe physical deletes.
The owner chose to upgrade to log-based CDC, prove it locally before AWS, run
Kafka/Connect on a separate EC2, and reconcile through Power BI Desktop.

## Decision

Debezium captures the nine OLTP tables from PostgreSQL logical replication into
one Kafka partition per table. A consumer writes immutable event batches under
`bronze/postgres_debezium/<table>/`; `_manifest.json` is the only Bronze commit
signal, and Kafka offsets are committed only after it can be read back. Event
offset, operation, row key, before/after image, and source LSN are preserved.

Glue Silver applies events in Kafka offset order and physically deletes rows
for source hard deletes. Its applied-offset checkpoint advances only after the
full Silver quality gate and Catalog/metrics work. Gold incremental facts remove
keys absent from their current Silver source, and dependent marts rebuild so
current Power BI numbers exclude deleted rows. Original events remain in Bronze.

The polling extractor, its S3 objects, and current Glue/BI locations remain
available until an isolated RDS/AWS run, failure recovery, main reconciliation,
and Power BI refresh pass. The Airflow schedule stays paused through cutover.

## Consequences

The RDS primary needs logical replication, a publication, and a monitored slot.
A stopped connector may retain WAL. One broker on a separate EC2 is sufficient
for this portfolio proof but is not highly available. The new path requires
separate S3/Glue/BI namespaces and explicit recovery from partial Delta writes.
