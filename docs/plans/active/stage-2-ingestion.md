# Execution Plan: Stage 2 ingestion

Date: 2026-10-25

## Status

Active

## Outcome

The frozen RDS snapshot and later source changes reach S3 Bronze as Parquet runs with a committed manifest written last. Later polls replay a 15-minute overlap from the per-table watermark. Clickstream and API sources complete the remaining Stage 2 scope.

## Context

- `PLAN.md`, Stage 2, defines Bronze paths, metadata, watermark timing, and rerun behavior.
- `README.md` documents the frozen main source and the isolated Stage 1C simulator database.
- `sources/oltp/migrations/versions/d79b3fd3ea12_add_cdc_watermark.py` provides the control table on RDS.

## Scope

In scope: implement and verify PostgreSQL timestamp polling to S3 Bronze, then the remaining log and API ingesters.

Out of scope: running the simulator on the main RDS source before downstream snapshot reconciliation.

## Approach

1. Take an initial snapshot, then poll `(last_updated_at - 15 minutes, max(updated_at)]` for each table. Each poll with rows writes a distinct run even if its overlap contains only previously seen rows.
2. Upload Parquet, then a committed manifest as the final object under `load_date=<UTC ingestion day>/run_id=<UTC start timestamp>/`. Advance the watermark only after the manifest upload. Silver reads only committed manifests and deduplicates overlapping source rows.
3. Check snapshot, unchanged-source replay, same-timestamp addition, and each upload failure using an isolated source and fake S3 state before running against RDS.
4. Add clickstream and API Bronze ingestion, then prove all three sources on S3.

## Risks And Recovery

- Timestamp polling cannot see physical deletes or writes backdated more than 15 minutes below the watermark; Stage 1C uses soft delete and updates `updated_at`.
- An S3 upload can succeed while a database commit fails. A retry creates another committed run; Silver must deduplicate source rows.
- A failed upload can leave an unreferenced Parquet object without a committed manifest. A separately specified cleanup job is needed before automatic deletion; this extractor does not delete S3 objects.
- The EC2 checkout revision is unverified. Compare it with the current repository before deployment and keep the EC2 `.env` when synchronizing code.
- Seven Olist tables share their initial `snapshot_at` as `updated_at`. On the current local snapshot, every unchanged-source poll selects all 1,352,257 rows across those tables. Fix this amplification before scheduling recurring polls.

## Progress

- [x] Add the `cdc_watermark` migration. The user reports upgrading EC2/RDS to `d79b3fd3ea12`; RDS state has not been observed locally.
- [x] Simplify the extractor to use the control table already at `d79b3fd3ea12`; the older boundary-hash migration remains in history but is no longer a prerequisite.
- [x] Implement PostgreSQL extraction and validate against a disposable PostgreSQL database with a fake S3 client.
- [x] Create a separate test bucket in `ap-southeast-1` and verify local SDK access with temporary Console-backed credentials; bucket name and login cache remain outside Git.
- [ ] Compare the EC2 checkout with this revision without overwriting its `.env`; confirm the RDS Alembic revision and the EC2 instance role.
- [ ] The user reports all nine initial snapshots from EC2/RDS to S3. The project bucket has committed snapshots for all nine tables; compare each source count and upper watermark with its manifest before marking RDS reconciliation verified. The `orders` snapshot has 99,441 rows and its Parquet SHA-256 matches the manifest; its later one-row run is a delta.
- [ ] Compare the remaining eight RDS source counts and watermarks with committed snapshot manifests. The user reports the unchanged-source polling issue fixed, but the current local extractor still has the 15-minute full-boundary replay; reconcile the EC2 code with this checkout before scheduling it.
- [ ] Validate the second polling run on an isolated `*_1c_test` source after `simulate_new_day --orders 100`; keep the main RDS snapshot frozen.
- [x] Implement clickstream JSONL compression and manifest-committed upload; prove one 80-event run on the local S3 test bucket.
- [x] Implement API JSON ingestion with three retries; choose free Frankfurter BRL/USD historical rates and a synthetic campaign HTTP fixture. Prove both on the local S3 test bucket.
- [x] Deploy and verify clickstream/API runs against the project bucket. The user chose clickstream from existing RDS orders; the generator reads their committed Bronze snapshot without changing the database.
- [ ] Verify the full Stage 2 acceptance criteria.

## Decisions

- 2026-10-25: Use a content-addressed file for the maximum `updated_at` group. This reconciles the plan's timestamp-tie warning with its zero-new-file rerun criterion without adding another control table.
- 2026-10-26: The owner chose manifest-committed at-least-once batches with a new ID per retry, and one boundary fingerprint in the existing watermark. The previous content-addressed object scheme and persistent attempt ledger are superseded.
- 2026-10-26: Encode the actual lower/upper timestamp window in the batch ID. Check the saved boundary hash before Parquet writing, so an unchanged boundary is excluded from the new batch.
- 2026-10-26: Shorten both batch-ID window labels to UTC `HHMM`; the manifest retains exact dates and sub-minute precision.
- 2026-10-27: Partition PostgreSQL Bronze by the UTC date of ingestion `started_at`. The source `updated_at` window, including its upper date, belongs to the manifest; `dt=` does not encode source time.
- 2026-10-27: Replace batch IDs and boundary hashing with `load_date=/run_id=/`, a 15-minute polling overlap, and a final `_SUCCESS` marker. The owner chose to retain the existing `bronze/postgres_oltp/<table>/` source prefix. An unchanged source can produce overlap files; Silver deduplication is required.
- 2026-10-27: Remove `_SUCCESS` at the owner's request. The committed manifest is the sole readiness signal, written after Parquet; watermark advancement follows its successful upload.
- 2026-10-30: The owner chose a free real exchange-rate API and a mock campaign API. Use Frankfurter's historical BRL/USD JSON and serve the synthetic campaign fixture over local HTTP; preserve both raw responses in Bronze.
- 2026-10-31: The owner chose to derive project clickstream from orders already present in the RDS snapshot. Use the committed `orders` and `order_items` Bronze snapshot as the read-only source; keep isolated simulator order IDs out of the project clickstream run.

## Validation

- 2026-10-27 manifest-only proof: 4 focused tests passed on disposable PostgreSQL 18.6. A run contains only Parquet and a committed manifest, uploaded in that order. Upload failures leave the watermark unchanged; a database-update failure after manifest upload is retried as a separate run. The 15-minute overlap and late-row boundary still pass.
- 2026-10-30 clickstream/API proof: 5 focused ingestion tests and 6 credential tests passed. The local test bucket contains one clickstream run with 80 events and committed manifest; decompression, byte size, and SHA-256 matched. The same bucket contains a Frankfurter response with 773 historical BRL/USD rates and a synthetic campaign response with 3 entries; both committed manifests and file hashes matched. No project-bucket or EC2 execution was observed for these new ingesters.
- 2026-10-31 project-bucket proof: API manifests `20261031T155421634605` and `20261031T155425440256` are committed, with 777 historical rates and 3 mock campaigns. Clickstream manifest `20261031T155746196601` is committed for 80 events from 20 distinct RDS snapshot orders. S3 object sizes and SHA-256 hashes matched all three manifests. Each purchase matched its snapshot order/customer, an order-item product, and the original purchase timestamp. Six focused clickstream/API tests passed, including repeatable event IDs.
- 2026-10-27 previous marker layout proof: 4 focused tests passed on disposable PostgreSQL 18.6 with the `d79b3fd3ea12` control-table shape. They covered snapshot, repeated 15-minute overlap, a late row inside the overlap, upload/manifest/`_SUCCESS` failures, and retry after database-update failure. The watermark stayed unchanged on S3 failure.
- The results below are historical proof for earlier layouts; they do not establish the current manifest-only completion rule.
- Earlier batch-layout proof: `tests/test_cdc_extract.py` passed 3 tests on disposable PostgreSQL 18.6 for snapshot, unchanged rerun, same-timestamp addition, upload failure, manifest failure, and a committed manifest followed by database failure and retry. The latter produced overlapping committed batches as designed.
- Migration proof: a disposable DB upgraded from `d79b3fd3ea12` to `6c4d9b7e2a11` and had `last_boundary_hash` without Stage 1C's `current_price`; a second disposable DB reached merge head `94f012a6c7b3`.
- Full snapshot proof on an earlier layout: loaded all nine Olist CSVs with zero quarantine into a disposable database at `6c4d9b7e2a11`. All nine tables extracted to 9 committed manifests and 9 Parquet files through a fake S3 client; each manifest row count matched its source table and each file SHA-256 matched its bytes. An immediate rerun of all nine tables created zero new objects. No AWS upload occurred in this proof.
- 2026-10-26 local S3 proof: a separate bucket showed Block Public Access on, ACLs disabled, and SSE-S3. AWS CLI `head-bucket` confirmed `ap-southeast-1`; boto3 using a temporary CLI-backed profile passed PUT, GET, LIST, and DELETE for an isolated smoke object. The ignored `.env` selects this bucket and profile.
- 2026-10-26 earlier-layout manifest smoke proof: the extractor published a two-row dummy `products` batch from disposable local PostgreSQL to the test S3 bucket. A GET of its manifest and Parquet verified committed status, source row count, exact file size, and SHA-256. Both smoke objects were deleted afterward. This was not an RDS extraction.
- 2026-10-26 window optimization proof: 3 focused PostgreSQL tests passed. A changed saved boundary replayed its rows; an unchanged boundary followed by a newer row produced a one-row Parquet file. A fresh full Olist snapshot again produced 9 manifests for 9 tables; the immediate rerun scanned each boundary group and created zero objects.
- 2026-10-26 HHMM naming proof: the same 3 focused PostgreSQL tests passed with minute-only batch labels, including a cross-midnight `2359_0000` batch whose manifest retained both full UTC timestamps.
- 2026-10-27 ingestion-date proof: 3 focused PostgreSQL tests passed. Each fake-S3 batch key's `dt=` matched the UTC date in its committed manifest's `started_at`, while source `updated_at` values remained in 2020.
- Integration proof: the user reports an EC2/RDS `orders` upload to the project S3 bucket; independent source/manifest/watermark reconciliation remains pending.
- The current local checkout uses Docker PostgreSQL and the separate S3 test bucket. All nine local source snapshots were reconciled against committed manifests, file hashes, and watermarks; this does not establish the RDS state.
- Repository checks: compile, `uv lock --check`, and `git diff --check`.
- 2026-10-27 documentation check: the EC2 first-run sequence, source/S3 reconciliation, and Silver overlap criterion were aligned with the current extractor. `git bundle create origin/main..HEAD`, `list-heads`, and `verify` succeeded locally; the current EC2 checkout revision remains unverified.

## Next Actions

1. Verify the user-reported EC2/RDS `orders` run against its S3 manifest and source watermark; compare checkout revision and IAM access.
2. The user reports all nine RDS snapshots and the unchanged-source polling fix complete. Compare the EC2 checkout and source watermarks with committed Bronze manifests, then update repository evidence for that work.
3. Build Silver ingestion to deduplicate overlap and retries; verify idempotency there before claiming the project has no logical duplicates.

## Result

Pending Stage 2 implementation and validation.
