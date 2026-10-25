# Execution Plan: Stage 1 OLTP source

Date: 2026-10-22

## Status

Active

## Outcome

Load the Olist snapshot into local PostgreSQL, record versioned baseline checks, and validate source-change generation in an isolated database. Keep the main source snapshot unchanged until Bronze/Silver/Gold reconciliation. Create RDS only after the local data model and access path are verified.

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
2. Inspect the downloaded Olist CSVs and review all models, keys and status mappings before generating the first Alembic revision.
3. Implement and verify models and migrations locally, then the loader and baseline queries. Record a `snapshot_id`, CSV identity, row/quarantine counts, SQL definitions and results before changing the main source.
4. Resolve the deletion and price-change model before implementing simulator behavior.
5. Develop and verify the simulator in an isolated database loaded from the same snapshot. Run it on the main source only after downstream initial-snapshot reconciliation succeeds; use watermark-bounded checks for subsequent changes.

## Risks And Recovery

- Olist `customer_id` is order-specific; retain `customer_unique_id` to identify returning people.
- A hard delete is invisible to `updated_at` polling and may violate order foreign keys. Decide the Stage 1 deletion representation before adding a deletion column or simulator logic.
- Keep original CSVs outside Git and retain the PostgreSQL volume; use isolated test state for destructive loader rehearsals.

## Progress

- [x] Product source paths separated from Harness `scripts/` in the plan and README.
- [x] SQLAlchemy/Alembic scaffolding is in place; the user is editing models and will generate the first revision.
- [x] Reviewed the nine source models, their core foreign keys, composite keys and indexes; CSV-level validation remains pending.
- [x] Olist CSVs inspected, including headers, row counts and key uniqueness.
- [x] Initial models and migration applied and verified on local PostgreSQL.
- [x] Snapshot loader passed full load and orphan quarantine checks in isolated PostgreSQL databases.
- [x] Main local database loaded and row counts reconciled.
- [x] Stage 1B baseline queries, CSV identities and snapshot metadata recorded under `olist_20261023T032906Z_99dc95d7`.
- [x] Simulator implemented and validated on an isolated database without changing the main source.
- [ ] RDS access and load verified if included in this stage.

## Decisions

- 2026-10-22: Place source-system code in `sources/oltp/` and `sources/clickstream/`; leave root `scripts/` to the Harness as documented in `docs/README.md`.
- 2026-10-22: Use SQLAlchemy models and reviewed Alembic migrations for OLTP schema changes. Do not add deletion semantics until decided.
- 2026-10-22: Removed the example `customers` revision so the user can generate the initial revision from their own reviewed models. The main `ecommerce` database is empty and has no Alembic version table.
- 2026-10-23: User applied the initial nine-table revision. Interpret naive Olist timestamps in `America/Sao_Paulo` when loading into `timestamptz`.
- 2026-10-23: Stage 1B records a versioned source baseline; Stage 1C tests the simulator in isolation. Main-source simulation waits for initial Bronze/Silver/Gold reconciliation.
- 2026-10-23: Stage 1C uses `products.current_price` for future sales without rewriting historical order-item prices, and `customers.is_deleted/deleted_at` for observable soft deletion. The user approved both choices. Main-source schema and snapshot remain unchanged pending reconciliation.

## Validation

- 2026-10-22: The earlier `customers` example revision upgraded, passed `alembic check`, downgraded and upgraded again in a temporary local PostgreSQL database. That revision has since been removed; the temporary database was removed and the main `ecommerce` database was not migrated.
- 2026-10-22: All nine current models configured successfully; six foreign keys and both composite keys were checked, and every table compiled for PostgreSQL. No migration or database change was run.
- 2026-10-23: The nine CSVs have 99,441 customers, 1,000,163 geolocation rows, 71 translations, 32,951 products, 3,095 sellers, 99,441 orders, 112,650 items, 103,886 payments and 99,224 reviews. Source keys for the core tables and six FK relationships have no duplicates/orphans. `review_id` repeats 814 times; two product categories lack translations.
- 2026-10-23: Full `COPY` load in an isolated migrated PostgreSQL database matched every CSV count with zero quarantine rows. A separate orphan-order test quarantined one invalid row and retained its valid parent. Both test databases were removed; the main database was empty at that point and was loaded afterward.
- 2026-10-23: Main `ecommerce` database has all nine CSV row counts; its load report lists zero quarantined rows. The 1B report records nine CSV SHA-256 hashes and all SQL/results in one repeatable-read transaction. Eight raw statuses sum to 99,441 orders; 24 purchase months sum to 112,650 items and BRL 13,591,643.70 in item price; six orphan counts are zero. No row has `updated_at` after the load snapshot timestamp.
- 2026-10-23: A deliberately mismatched load-report row count was rejected without writing a baseline file.
- 2026-10-23: Restored the frozen source into `ecommerce_1c_test`, applied the Stage 1C migration, and simulated 20 orders. Orders rose 99,441 to 99,461; 989 product prices changed; 99 customers were soft-deleted; 80 clickstream events were emitted. New-order payment totals matched items plus freight, FK orphan counts were zero, and original item-price total stayed BRL 13,591,643.70. The main database still has 99,441 orders.
- 2026-10-25: The snapshot loader now skips the Stage 1C product-price backfill when the initial schema has no `current_price` column. An isolated PostgreSQL 18.6 container at initial revision `b5e79439fda1` loaded all nine CSVs with zero quarantined rows; its four required baseline result sets exactly matched the recorded local 1B baseline. The test container and its separate empty Compose test database were removed. RDS load remains unverified.
- Focused proof: migration round trip and model/schema agreement, then source-to-target row counts, quarantine accounting and simulator invariants.
- Integration proof: run the load on the main local PostgreSQL database, capture its baseline, and test the simulator in a separate database. Verify RDS separately when provisioned; downstream reconciliation precedes main-source simulation.
- Repository checks: `git diff --check`, path references and clean credential handling.

## Result

Pending Stage 1 implementation and validation.
