# Execution Plan: AWS Glue lakehouse migration

Date: 2026-11-07

## Status

Active — repository implementation and live AWS validation are in progress. The
existing Databricks implementation remains the historical baseline; this plan
supersedes its execution path without deleting its data or resources.

## Outcome

Airflow ingests Bronze to S3, waits for an AWS Glue 6.0 Silver Delta job and its
quality gate, builds Gold and marts through dbt-glue, publishes six marts and
run metadata to Athena engine v3, and refreshes the seven-table Power BI Import
model without a Databricks dependency.

## Context

- Owner decision in this conversation: complete the AWS pipeline and Power BI,
  use S3 plus Glue Catalog for Ops, validate an isolated live run, then switch
  the main path after reconciliation.
- The uncommitted migration in the working tree has offline tests but no live
  Glue proof. Review found a Glue argument mismatch, missing Silver catalog
  registration, incomplete Delta dbt config, and zero-valued Ops metrics.
- `docs/decisions/0003-databricks-silver-execution.md` and the older Databricks
  plan record the prior architecture and validation, not the new target.

## Scope

In scope: Glue job packaging/configuration, Silver catalog and metrics, dbt
profiles and runtime proof, Athena/ Ops storage, Power BI source migration,
runbooks and tests, isolated and main AWS validation.

Out of scope: deleting Databricks resources or historical data and changing
unrelated OLTP/Bronze transformations.

## Approach

1. Record the replacement architecture and preflight requirements.
2. Repair Glue arguments, job configuration, quality-gated catalog registration,
   and per-run Silver counts.
3. Repair dbt Delta settings, publish Athena-readable marts, and make S3 the
   durable Ops source with measured counts and clear failure semantics.
4. Rebind Power BI Import partitions to Athena while preserving model semantics.
5. Run offline, isolated AWS, and then controlled main-path proof. Keep the DAG
   paused until the main run, Ops, and Power BI reconcile.

## Risks And Recovery

- Never repoint an existing catalog table to another S3 root. Use separate AWS
  Silver/Gold/BI namespaces and fail on a location mismatch.
- Preserve the Databricks path and PBIP source revision in Git for rollback.
- A partial Silver or Gold run can leave Delta tables advanced; rerun idempotent
  merges after fixing the cause, and inspect Delta history before rollback.
- If AWS permissions, budget, ODBC, or gateway access is unavailable, stop at
  the last observed proof and report the exact gap.
- Glue 6.0 changes Spark SQL defaults and Delta runtime. Recheck casts,
  date parsing, Delta table protocol, and Athena SELECT on a real isolated run
  before trusting the local Spark and dbt parse results.

## Progress

- [x] Inspect current migration, accepted user choices, and repository workflow.
- [x] Implement Glue/Delta/catalog contract and focused offline proof.
- [x] Implement dbt, Athena, Ops, and Power BI code and focused offline proof.
- [x] Update architecture, runbook, and repository-wide offline validation.
- [x] Align Glue job and dbt sessions to Glue 6.0 and its Delta configuration;
  verify the configuration contract offline.
- [ ] Validate isolated live run, negative fixture, and same-date rerun.
- [ ] Reconcile and switch main path and Power BI, then verify scheduling.

## Decisions

- 2026-11-07: S3 run objects plus a Glue/Athena Ops table are the shared Ops
  source. Local SQLite and JSONL are not authoritative.
- 2026-11-07: Power BI remains Import and reads six marts plus Ops via Athena.
- 2026-11-07: Use new AWS S3 and Catalog namespaces for the main path; keep the
  Databricks-backed data untouched.
- 2026-11-08: Use AWS Glue 6.0 for Silver and dbt interactive sessions. Keep
  native Delta integration; PythonVersion `3` is the Glue API selector for
  Spark jobs, while GlueVersion selects Python 3.13. Pin local Delta 4.2.0 to
  match Glue's bundled release; local PySpark remains 4.1.1.

## Validation

- Focused proof: Glue argument, Catalog location, per-run Ops, Athena publish,
  DAG preflight, Glue 6.0/Delta runtime contract, and dbt profile tests pass.
  An in-memory negative check rejected both Glue 5.0 and the deprecated global
  LogStore setting. Power BI Modeling MCP serialized
  and reloaded seven Import tables and 26 measures.
- Integration or end-to-end proof: `dbt compile` compiled 122/122 resources
  with offline placeholders, but emitted AWS credential-refresh warnings. No
  Glue/Athena/Power BI data run has passed. AWS CLI `sts get-caller-identity`
  reports an expired `aws login` session, and this Windows machine has no
  Athena ODBC DSN.
- Repository-required checks: `uv run python -m unittest discover -s tests
  -p 'test_*.py' -q` passed 85 tests with 16 skipped after the Glue 6.0
  configuration update; Python compileall and
  `git diff --check` passed. Line-ending normalization warnings were emitted
  by Git on Windows; no whitespace errors remain.

## Result

Repository implementation is in place. Live isolated validation, main cutover,
and populated Power BI verification remain pending authenticated AWS access
and Athena ODBC setup. Keep the DAG paused.
