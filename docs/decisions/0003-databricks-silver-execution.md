# 0003 Databricks Silver execution

Date: 2026-11-05

## Status

Superseded by [0004](0004-aws-glue-pipeline.md) for the active execution path.

## Context

The previous Stage 6 DAG downloaded committed S3 Bronze to local Airflow, ran Spark/Delta and Great Expectations there, then uploaded Silver to S3. This caused redundant transfer and required a JVM in the orchestration container.

## Decision

Airflow uploads Bronze, triggers and waits for a Databricks Spark Python Job, then runs dbt only when the Job succeeds. The Job reads committed Bronze directly from S3, validates the manifest payloads, merges Delta Silver on S3, runs the Silver quality gate, and registers the target Unity Catalog tables. The local Silver path remains available for development proof.

## Alternatives Considered

1. Continue running Spark inside local Airflow and syncing files in both directions.

## Consequences

Positive:

- Airflow no longer performs Spark processing or Silver file transfer.
- Databricks executes Silver next to its Delta/Unity Catalog consumers.

Tradeoffs:

- The Job needs its own compute, code, libraries, identity, S3 access, and Unity Catalog permissions.
- Multi-table Delta MERGEs are not atomic as a group. Gold is gated by Job success, but some Silver tables can advance before a later failure. Recovery is a corrected rerun and, if necessary, targeted Delta rollback.

## Follow-Up

- Validate the Job and full DAG against the isolated 1C prefix/schema, including a failure and retry.
- Reduce the Airflow image's unused Spark Python dependencies after the remote path is proven.
