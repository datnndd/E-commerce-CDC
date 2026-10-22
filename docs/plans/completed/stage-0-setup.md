# Execution Plan: Stage 0 environment setup

Date: 2026-10-22

## Status

Completed

## Outcome

From this checkout, start local Postgres 15 and Airflow, verify Postgres logical WAL and the Airflow UI, install the Python tooling, and verify access to the agreed AWS S3 and Databricks setup before any cloud workload runs.

## Context

- `PLAN.md`, stage 0, supplies the requested setup outcome and checklist.
- `docs/WORKFLOW.md` defines validation and recovery expectations.
- The GitHub repository already exists at `datnndd/E-commerce-CDC`.
- The user selected AWS Region `ap-southeast-1`, S3, RDS PostgreSQL, Databricks Delta Lake, and dbt-databricks. Dashboard choice remains open.

## Scope

In scope:

- Local Compose, Python tooling, structure, secrets hygiene, setup guidance, and focused smoke checks.
- AWS access, scoped S3 bucket, Databricks workspace access, and S3/Databricks smoke checks.

Out of scope:

- OLTP schema and seed, ingestion, dbt models, Airflow DAGs, and production deployment.

## Approach

1. Implement the local setup and validate it through Docker and Python commands.
2. Verify access to the selected AWS account and Region; configure scoped resources and verify access without running data workloads.
3. Record evidence, update README, and close this plan only after all stage 0 criteria are met.

## Risks And Recovery

- Cloud resources can incur cost. Remove only resources created for this run if recovery is needed.
- `docker compose down` stops local containers while retaining named volumes. Do not delete volumes during routine cleanup.
- AWS credentials and Databricks tokens must remain outside Git; revoke any leaked credential immediately.

## Progress

- [x] Local Compose and locked Python tooling installed (`dbt-databricks` 1.11.8, PySpark 4.2.0, Faker 38.3.0).
- [x] Local Postgres, WAL, and Airflow UI verified.
- [x] AWS Console/CloudShell identity and `ap-southeast-1` verified.
- [x] Project S3 bucket created with Block Public Access, ACLs disabled and SSE-S3; 14-byte smoke object uploaded, inspected and deleted through CloudShell.
- [x] Databricks CLI OAuth authenticated, SQL warehouse `SELECT 1 AS ok` returned `1`, and the warehouse was stopped again after the check. The Free Edition metastore reports `aws:us-east-2`.
- [x] User chose S3/RDS in `ap-southeast-1` with the Free Edition workspace in `us-east-2`, accepting cross-Region transfer. The new project storage credential and external location validate against S3.
- [x] IAM role/policy scopes S3 permissions to the project bucket, trusts the Databricks Unity Catalog principal with external ID, and self-assumes. Credential validation passed READ, LIST, WRITE, DELETE and PATH_EXISTS. File events were disabled.
- [x] README and Git state checked; stage 0 result recorded. Set lifecycle policy before scaling demo data and configure local AWS CLI/temporary credentials before Stage 2 ingestion.

## Decisions

- 2026-10-22: Use Airflow standalone with its local development database for stage 0, keeping the OLTP Postgres instance separate. Later orchestration work can replace this if its workload requires a different executor or metadata backend.
- 2026-10-22: Use Databricks and Delta Lake for Silver/Gold; Athena cannot write Delta tables, so dbt-databricks is the matching adapter. Dashboard tool is deferred to stage 7.
- 2026-10-22: Use AWS CLI in CloudShell for stage 0; local CLI authentication is needed before ingestion. Free Edition does not support custom workspace storage, while project S3 external location access has passed validation.
- 2026-10-22: Pin dbt-databricks below 1.12 for Windows setup. Its current 1.12 dependency fetched a parser wheel from GitHub and failed on this network; 1.11.8 installed and reports the Databricks adapter.
- 2026-10-22: CLI inspection showed the Free Edition metastore is in `us-east-2`, while the project bucket is in `ap-southeast-1`. The user explicitly chose to keep AWS in Singapore and accept cross-Region transfer. Track its charges and latency during later workload tests.
- 2026-10-22: Created separate IAM role, Unity Catalog storage credential and external location for this bucket. Validation passed across Regions. File events are disabled to avoid unnecessary queue setup for Stage 0.

## Validation

- Focused proof: `docker compose config`, `docker compose ps`, Postgres query for `wal_level`, Airflow HTTP response, `uv sync --frozen`, Python imports.
- Integration or end-to-end proof: Databricks and S3 smoke checks.
- Repository-required checks: `git diff --check`, ignored credential paths, status inspection.

## Result

Local setup, AWS S3 smoke checks, Databricks `SELECT 1`, and the cross-Region external location passed. The initial project commit reached GitHub before the user requested local-only commits; subsequent commits remain local until requested otherwise.
