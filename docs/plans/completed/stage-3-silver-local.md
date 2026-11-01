# Execution Plan: Local Silver

Date: 2026-11-01

## Status

Completed

## Outcome

Local Bronze runs with committed manifests become 12 deduplicated Delta Silver tables and a quarantine table. Replaying the same or overlapping runs does not create duplicate logical rows, and older updates do not replace newer source versions.

## Context

- `PLAN.md`, Stage 3, defines the 12 tables, cleanup, Delta MERGE, quarantine, and validation criteria.
- `ingestion/cdc_extract.py`, `ingestion/ingest_logs.py`, and `ingestion/ingest_api.py` define the three Bronze manifest and data layouts.
- The owner wants implementation and proof locally before Databricks/S3 deployment.

## Scope

In scope: local manifest reader, Spark normalization/deduplication, local Delta MERGE, quarantine, focused local integration proof, and runbook.

Out of scope: EC2, RDS, Databricks catalog creation, Unity Catalog storage paths, and deployment to S3.

## Approach

1. Select only committed local manifests and their named files; validate file size, SHA-256, and row counts before processing.
2. Normalize all 12 datasets with table-specific keys and checks. Quarantine invalid source rows with a reason rather than discarding them silently.
3. Deduplicate within each ingest by source version and ingestion time; MERGE into local Delta with a newer-version guard.
4. Prove snapshot, overlap, replay, late update, malformed input, clickstream, and both API shapes with local fixtures.

## Risks And Recovery

- The accepted Stage 3 plan requires Delta, but local Spark needs a compatible Delta runtime. Pin compatible dependencies and prove a real local MERGE before claiming completion.
- A manifest can be committed while a file is missing or altered. Fail the run before writing Silver.
- Do not overwrite existing local Silver data during normal processing; test output uses disposable directories.
- Cloud catalog and external-location behavior remain unverified until the owner starts cloud testing.

## Progress

- [x] Pin PySpark 4.1 and Delta 4.3, build a Java 17 Docker runtime, and prove local Delta MERGE.
- [x] Implement manifest-bound Bronze reads and all 12 dataset transformations.
- [x] Implement and test local Delta MERGE, replay, overlap, late update, soft delete, and quarantine.
- [x] Materialize a full Bronze input from local PostgreSQL in a read-only transaction; run and verify all 12 Delta tables.
- [x] Prove a product price update end to end through an isolated local PostgreSQL database, the CDC extractor, and Delta Silver.
- [x] Document local execution and update `PLAN.md` with observed status.

## Decisions

- 2026-11-01: Use local filesystem Bronze fixtures and local Delta paths for Stage 3 proof; cloud deployment is deferred by the owner.

## Validation

- Focused proof: two Docker Spark/Delta integration tests passed. They cover all 12 datasets, a second identical run, overlap, newer and older order/product updates, customer soft delete, five invalid records quarantined, uncommitted/unlisted files ignored, and bad SHA-256 rejected.
- Full local proof: nine PostgreSQL tables materialized as Parquet snapshots with committed manifests without writing to the database; 80 clickstream events and two API responses materialized locally. All 12 Silver Delta tables were built and replayed with unchanged counts. `verify_silver` found zero duplicate/null primary keys, 20 matching purchase joins, and zero quarantine rows on this clean snapshot. OLTP Silver counts match the nine local source counts (customers 99,441; geolocation 1,000,163; category translation 71; products 32,951; sellers 3,095; orders 99,441; order items 112,650; payments 103,886; reviews 99,224).
- End-to-end local proof: one disposable database produced a committed product snapshot at price 10.00, then a committed delta after a PostgreSQL update to 20.00. Silver reflected 10.00 and then 20.00; the watermark reached the new source timestamp. The disposable database was dropped. Docker Desktop retained some ignored test artifacts because Windows denied immediate bind-mount cleanup.
- Repository checks: `uv lock --check`, `git diff --check`, compile, and Windows unittest discovery passed; five disposable-PostgreSQL tests, two Docker-only Silver tests, and the opt-in PostgreSQL/Silver end-to-end test are skipped in default Windows discovery. The two Silver tests passed in Docker, and the opt-in end-to-end test passed against local Compose PostgreSQL plus Docker.

## Result

Local Stage 3 implementation and proof completed. Databricks catalog/external-location deployment and the same end-to-end path in cloud remain open in the main `PLAN.md`.
