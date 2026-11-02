# Execution Plan: Stage 6 orchestration and data quality

Date: 2026-11-02

## Status

Completed by owner acceptance; repeatability and unattended operation remain unverified.

## Outcome

Run the existing Bronze, Silver, and full dbt Gold/business-mart pipeline from local Airflow, stop before Gold when Silver quality fails, and record run status for the future Ops dashboard.

## Context

- `PLAN.md` Stage 6 authorizes Airflow, Great Expectations, retries, alerts, and Delta run metadata. The owner chose local Airflow calling Databricks with a separately configured noninteractive token.
- `spark_jobs/silver_transform.py` currently reads local Bronze and writes local Delta. `ecommerce_cdc.silver` currently points at a one-time published snapshot; cloud publication must be explicit before dbt.
- The current PostgreSQL poll replays an unchanged 15-minute window, and the second `fct_orders` build rewrote unchanged rows. Address these before enabling unattended daily runs.

## Scope

In scope: update Stage 6 in `PLAN.md`, wire Airflow's local runtime and DAG, move committed Bronze to local Spark input, validate Silver with GX, publish verified Silver Delta to S3/Databricks, build all dbt resources, record Delta run metadata, add focused proof and runbook.

Out of scope: a real clickstream collector, dashboard implementation, and a production service-principal setup. Keep the frozen main RDS/OLTP source unchanged; use an isolated source for change-path rehearsal.

## Approach

1. Establish the runtime contract and correct Stage 6 order and source column names in `PLAN.md`.
2. Add the smallest local Airflow runtime that can execute the existing repository commands with Java, Python dependencies, and environment credentials.
3. Add committed-Bronze synchronization, Silver GX gate, and safe cloud Silver publication before full `dbt build`.
4. Add failure alert and `ops_pipeline_runs` metadata, then test task order, quality failure, retry/idempotency, and an isolated full run where credentials and compute permit.

## Risks And Recovery

- The Databricks OAuth profile is interactive. Airflow uses a separate token provided at runtime; never store it in Git or log it. The current `.env` has no unattended token or warehouse settings yet.
- Silver publication changes external table locations. Upload all referenced Delta files and verify them before changing a table location. A failed DAG must not run dbt; rerun publication to recover an incomplete upload.
- Existing unchanged-window polling and `>=` Gold incremental predicates can do large redundant work. Do not claim a scheduled no-change run is efficient until measured after fixes.
- Stop only local containers started during this plan. Do not mutate the frozen main source for a test.

## Progress

- [x] Inspected Stage 6 authority, source commands, cloud/dbt boundary, and current runtime.
- [x] Update `PLAN.md` and execution documentation.
- [x] Implement Airflow DAG, local runtime, committed Bronze sync, Silver quality gate, publication, and ops metadata.
- [x] Prove the positive 1C run, negative GX gate, DAG ordering, and local-to-cloud recovery. The owner reports a later successful Airflow run and accepts Stage 6 closure. A second complete run with the same logical date and the unattended token path remain unverified.

## Decisions

- 2026-11-02: Local Airflow will call Databricks with a separate token supplied through ignored runtime configuration, as selected by the owner.
- 2026-11-02: Run the Silver quality checkpoint before publishing Silver to Gold consumers; use `dbt build` for the complete Stage 4-5 graph and tests.
- 2026-11-02: The owner chose an isolated 1C test. Use `PIPELINE_S3_PREFIX` inside the existing project bucket and distinct Databricks Silver/Gold schemas, so the existing bucket external location can be reused without mixing snapshot and simulator data.
- 2026-11-02: The owner chose to close Stage 6 based on the tested pipeline and their report of a successful later DAG run, without waiting for the repeated-logical-date check.

## Validation

- Focused proof: CDC on disposable PostgreSQL 11/11; Stage 6 helpers, API, DAG graph, and GX positive/negative fixture passed. A repeated `orders` poll on isolated 1C scanned and published zero rows.
- Integration or end-to-end proof: isolated `airflow dags test` succeeded. `dbt build` passed 122/122, Ops recorded Bronze/Silver 1,551,109, Gold 415,385, marts 150,672, quarantine 0. Silver and Gold each have 99,461 orders; the main Silver table location is unchanged. An isolated negative payment failed GX three times, did not reach publication/dbt, sent one local webhook alert, and left Gold unchanged. Concurrent `dags test` runs with the same logical date conflicted in SQLite; Docker Desktop stopped during the following recovery run after GX passed. The payment was restored to 99.33. The recovered local Silver has 103,906 payments, no duplicate keys, and no negative values. Host publish and full dbt build passed; Databricks Gold has 99,461 orders, 103,906 payments, and zero negative payments. Both interrupted Ops records are `failed`. The owner reports a later successful full DAG run, but no run ID was supplied. In the local Airflow volume inspected on 2026-11-02, one manual run remained `running` at `verify_silver` and another was `queued`; this volume does not corroborate the later success report. A second complete Airflow DagRun with the same logical date still needs proof.
- Repository-required checks: `uv lock --check`, `docker compose config --quiet`, DAG import, `git diff --check` passed; rerun after final edits.

## Result

Stage 6 implementation and its positive/negative path were validated, and the owner accepted closure. The later reported DAG success is not independently verified in the local Airflow metadata. Repeated-logical-date idempotency and unattended scheduling with a separate token, private-RDS access, and a real webhook are carryover checks; do not present them as proven behavior.
