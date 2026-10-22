# Execution Plan: Stage 1 OLTP source

Date: 2026-10-22

## Status

Active

## Outcome

Load the Olist snapshot into local PostgreSQL, record baseline checks, and generate valid source changes for later ingestion. Create RDS only after the local data model and access path are verified.

## Context

- `PLAN.md`, Stage 1, defines the requested source data, validation and simulation behavior.
- `docs/WORKFLOW.md` defines repository authority and proof.
- `docs/README.md` reserves root `scripts/` for Harness bootstrap and validation.

## Scope

In scope:

- Organize product source code under `sources/oltp/` and `sources/clickstream/`.
- Inspect Olist CSV headers and counts, finish SQLAlchemy models and Alembic migrations, load the snapshot, validate joins and baselines, then simulate changes.
- Plan RDS connectivity and reproduce the verified local load in AWS when access is ready.

Out of scope:

- Bronze ingestion, Silver/Gold transformations and dashboard work.
- Scaling synthetic data before the small pipeline is correct.

## Approach

1. Keep root `scripts/` for Harness tasks; place Stage 1 product files under `sources/`.
2. Use the first `customers` model and migration as a provisional example; inspect the downloaded Olist CSVs before finalizing types, keys or status mappings for the whole schema.
3. Implement and verify models and migrations locally, then the loader, baseline queries and simulator in that order.
4. Resolve the deletion and price-change model before implementing simulator behavior.

## Risks And Recovery

- Olist `customer_id` is order-specific; retain `customer_unique_id` to identify returning people.
- A hard delete is invisible to `updated_at` polling and may violate order foreign keys. Decide the Stage 1 deletion representation before adding a deletion column or simulator logic.
- Keep original CSVs outside Git and retain the PostgreSQL volume; use isolated test state for destructive loader rehearsals.

## Progress

- [x] Product source paths separated from Harness `scripts/` in the plan and README.
- [x] First `customers` SQLAlchemy model and Alembic migration added as a worked example.
- [ ] Olist CSVs inspected, including headers, row counts and key uniqueness.
- [ ] Complete models and migrations applied and verified on local PostgreSQL.
- [ ] Loader, baseline queries and simulator implemented and validated.
- [ ] RDS access and load verified if included in this stage.

## Decisions

- 2026-10-22: Place source-system code in `sources/oltp/` and `sources/clickstream/`; leave root `scripts/` to the Harness as documented in `docs/README.md`.
- 2026-10-22: Use SQLAlchemy models and reviewed Alembic migrations for OLTP schema changes. The first `customers` table is an example pending CSV profiling; do not add deletion semantics yet.

## Validation

- 2026-10-22: `customers` migration upgraded, passed `alembic check`, downgraded and upgraded again in a temporary local PostgreSQL database. The temporary database was removed; the main `ecommerce` database was not migrated.
- Focused proof: migration round trip and model/schema agreement, then source-to-target row counts, quarantine accounting and simulator invariants.
- Integration proof: run the load and simulator against local PostgreSQL, then verify RDS separately when provisioned.
- Repository checks: `git diff --check`, path references and clean credential handling.

## Result

Pending Stage 1 implementation and validation.
