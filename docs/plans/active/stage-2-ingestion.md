# Execution Plan: Stage 2 ingestion

Date: 2026-10-25

## Status

Active

## Outcome

The frozen RDS snapshot and later source changes reach S3 Bronze as manifest-committed Parquet batches, with a per-table watermark and no new objects when an unchanged source is polled again. Clickstream and API sources complete the remaining Stage 2 scope.

## Context

- `PLAN.md`, Stage 2, defines Bronze paths, metadata, watermark timing, and rerun behavior.
- `README.md` documents the frozen main source and the isolated Stage 1C simulator database.
- `sources/oltp/migrations/versions/d79b3fd3ea12_add_cdc_watermark.py` provides the control table on RDS.

## Scope

In scope: implement and verify PostgreSQL timestamp polling to S3 Bronze, then the remaining log and API ingesters.

Out of scope: running the simulator on the main RDS source before downstream snapshot reconciliation.

## Approach

1. Fingerprint each table's saved `updated_at` group and compare it with `cdc_watermark.last_boundary_hash`. If unchanged, write only newer rows; if changed, include the boundary group. An unchanged source creates no new objects.
2. Write one Parquet file per changed batch, then upload one committed manifest as the final S3 object. Encode the lower/upper UTC hour and minute in the batch ID, with a random suffix for retries; keep exact window timestamps in the manifest. Advance watermark and fingerprint only after that upload. Silver deduplicates overlapping committed batches.
3. Check the initial extract, an unchanged rerun, a same-timestamp addition, and upload failure using isolated source and fake S3 state before running against RDS.
4. Add clickstream and API Bronze ingestion, then prove all three sources on S3.

## Risks And Recovery

- Timestamp polling cannot see physical deletes or writes backdated below the watermark; Stage 1C uses soft delete and updates `updated_at`.
- An S3 upload can succeed while a database commit fails. A retry creates another committed batch; Silver must deduplicate source rows.
- A failed upload can leave unreferenced Parquet. A separately specified cleanup job is needed before automatic deletion; this extractor does not delete S3 objects.

## Progress

- [x] Add the `cdc_watermark` migration. The user reports upgrading EC2/RDS to `d79b3fd3ea12`; RDS state has not been observed locally.
- [ ] Apply `6c4d9b7e2a11` to RDS before running the new extractor; the 1C test branch can use merge revision `94f012a6c7b3`.
- [x] Implement PostgreSQL extraction and validate against a disposable PostgreSQL database with a fake S3 client.
- [x] Create a separate test bucket in `ap-southeast-1` and verify local SDK access with temporary Console-backed credentials; bucket name and login cache remain outside Git.
- [ ] Implement clickstream and API ingestion.
- [ ] Verify the full Stage 2 acceptance criteria.

## Decisions

- 2026-10-25: Use a content-addressed file for the maximum `updated_at` group. This reconciles the plan's timestamp-tie warning with its zero-new-file rerun criterion without adding another control table.
- 2026-10-26: The owner chose manifest-committed at-least-once batches with a new ID per retry, and one boundary fingerprint in the existing watermark. The previous content-addressed object scheme and persistent attempt ledger are superseded.
- 2026-10-26: Encode the actual lower/upper timestamp window in the batch ID. Check the saved boundary hash before Parquet writing, so an unchanged boundary is excluded from the new batch.
- 2026-10-26: Shorten both batch-ID window labels to UTC `HHMM`; the outer `dt=` supplies the upper date, and the manifest retains exact dates and sub-minute precision.

## Validation

- Focused proof on the new layout: `tests/test_cdc_extract.py` passed 3 tests on disposable PostgreSQL 18.6 for snapshot, unchanged rerun, same-timestamp addition, upload failure, manifest failure, and a committed manifest followed by database failure and retry. The latter produces overlapping committed batches as designed.
- Migration proof: a disposable DB upgraded from `d79b3fd3ea12` to `6c4d9b7e2a11` and had `last_boundary_hash` without Stage 1C's `current_price`; a second disposable DB reached merge head `94f012a6c7b3`.
- Full snapshot proof on the new layout: loaded all nine Olist CSVs with zero quarantine into a disposable database at `6c4d9b7e2a11`. All nine tables extracted to 9 committed manifests and 9 Parquet files through a fake S3 client; each manifest row count matched its source table and each file SHA-256 matched its bytes. An immediate rerun of all nine tables created zero new objects. No AWS upload occurred in this proof.
- 2026-10-26 local S3 proof: a separate bucket showed Block Public Access on, ACLs disabled, and SSE-S3. AWS CLI `head-bucket` confirmed `ap-southeast-1`; boto3 using a temporary CLI-backed profile passed PUT, GET, LIST, and DELETE for an isolated smoke object. The ignored `.env` selects this bucket and profile.
- 2026-10-26 manifest smoke proof: the new extractor published a two-row dummy `products` batch from disposable local PostgreSQL to the test S3 bucket. A GET of its manifest and Parquet verified committed status, source row count, exact file size, and SHA-256. Both smoke objects were deleted afterward. This was not an RDS extraction.
- 2026-10-26 window optimization proof: 3 focused PostgreSQL tests passed. A changed saved boundary replayed its rows; an unchanged boundary followed by a newer row produced a one-row Parquet file. A fresh full Olist snapshot again produced 9 manifests for 9 tables; the immediate rerun scanned each boundary group and created zero objects.
- 2026-10-26 HHMM naming proof: the same 3 focused PostgreSQL tests passed with minute-only batch labels, including a cross-midnight `2359_0000` batch whose manifest retained both full UTC timestamps.
- Integration proof: EC2/RDS to the project S3 bucket remains pending.
- Local CDC extraction against private RDS is pending a network path; the local `.env` currently names its private endpoint and local Docker PostgreSQL is stopped.
- Repository checks: compile, `uv lock --check`, and `git diff --check`.

## Result

Pending Stage 2 implementation and validation.
