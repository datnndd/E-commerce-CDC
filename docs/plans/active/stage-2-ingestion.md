# Execution Plan: Stage 2 ingestion

Date: 2026-10-25

## Status

Active

## Outcome

The frozen RDS snapshot and later source changes reach S3 Bronze as Parquet, with a per-table watermark and no new objects when an unchanged source is polled again. Clickstream and API sources complete the remaining Stage 2 scope.

## Context

- `PLAN.md`, Stage 2, defines Bronze paths, metadata, watermark timing, and rerun behavior.
- `README.md` documents the frozen main source and the isolated Stage 1C simulator database.
- `sources/oltp/migrations/versions/d79b3fd3ea12_add_cdc_watermark.py` provides the control table on RDS.

## Scope

In scope: implement and verify PostgreSQL timestamp polling to S3 Bronze, then the remaining log and API ingesters.

Out of scope: running the simulator on the main RDS source before downstream snapshot reconciliation.

## Approach

1. Keep each table's newest `updated_at` group in a separate content-addressed Parquet object. Re-read that group on the next run to catch timestamp ties and avoid a new object when it is unchanged.
2. Stream source rows in batches, upload to S3, then commit the table's watermark. Retry against existing object keys after a failed database commit.
3. Check the initial extract, an unchanged rerun, a same-timestamp addition, and upload failure using isolated source and fake S3 state before running against RDS.
4. Add clickstream and API Bronze ingestion, then prove all three sources on S3.

## Risks And Recovery

- Timestamp polling cannot see physical deletes or writes backdated below the watermark; Stage 1C uses soft delete and updates `updated_at`.
- An S3 upload can succeed while a database commit fails. A retry finds the same object key and advances the watermark without creating another object.
- S3 objects are not removed automatically by this script; investigate failed runs before manual cleanup.

## Progress

- [x] Add the `cdc_watermark` migration. The user reports upgrading EC2/RDS to `d79b3fd3ea12`; RDS state has not been observed locally.
- [x] Implement PostgreSQL extraction and validate against a disposable PostgreSQL database with a fake S3 client.
- [ ] Implement clickstream and API ingestion.
- [ ] Verify the full Stage 2 acceptance criteria.

## Decisions

- 2026-10-25: Use a content-addressed file for the maximum `updated_at` group. This reconciles the plan's timestamp-tie warning with its zero-new-file rerun criterion without adding another control table.

## Validation

- Focused proof: `tests/test_cdc_extract.py` passed on disposable PostgreSQL 18.6 for initial extraction, unchanged rerun, same-timestamp addition, upload failure, and retry. The failed upload did not advance the watermark.
- Full snapshot proof: loaded all nine Olist CSVs with zero quarantine into a separate disposable database at `d79b3fd3ea12`. All nine tables extracted into 11 Parquet objects totaling 66,269,013 bytes through a fake S3 client; the immediate rerun created zero new objects. The fake client checked object keys and file sizes; no AWS upload occurred.
- Integration proof: EC2/RDS to the project S3 bucket remains pending.
- Repository checks: compile, `uv lock --check`, and `git diff --check`.

## Result

Pending Stage 2 implementation and validation.
