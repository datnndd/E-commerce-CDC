# Execution Plan: Databricks Silver orchestration

Date: 2026-11-05

## Status

Active — the Spark Connect first-write incompatibility is fixed and a full isolated Databricks Job passed all 12 Silver tables, quality checks, and Unity Catalog registration. The main Bronze prefix still contains 20 stale purchase events that fail the accepted join gate; handling that input and a full Airflow DAG run remain open.

## Outcome

Airflow uploads Bronze to S3, triggers a Databricks Spark Python Job that reads committed Bronze directly from S3 and writes Delta Silver there, waits for its quality gate, then runs dbt Gold and marts. No Airflow worker downloads Bronze or executes Spark.

## Context

- Owner decision (2026-11-05): use Databricks Spark instead of pulling S3 data into local Airflow for processing.
- `PLAN.md` Stage 6 and `README.md` specify the Bronze → Silver quality gate → dbt order and isolated S3/schema settings.
- The initial migration had a successful placeholder when the Databricks provider was absent and passed notebook parameters to a Python command entrypoint.
- The initial migration skipped the manifest size/SHA check required by the Stage 2 Bronze contract. It also used `boto3`, whose credentials are distinct from Unity Catalog external-location access.

## Scope

In scope: make the remote Job invocation and parameter contract executable, restore manifest integrity checks for local and S3 inputs, preserve the Silver quality gate, remove dead migration code, align the runbook and focused tests.

Out of scope: changing the frozen main source, provisioning credentials or compute, replacing the historical local integration path, and claiming an unobserved cloud run.

## Approach

1. Remove the DAG success placeholder and require a configured Databricks Job ID. Pass Spark Python task arguments for the selected bucket, prefix, catalog, and Silver schema.
2. Make the Spark command read those exact arguments, use Spark for S3 manifest listing/checksums under Unity Catalog access, and gate known invalid payments/events before their merge.
3. Update the Job setup/runbook and tests. Run local DAG import, focused Silver tests, and repository checks where available.

## Risks And Recovery

- A failed multi-table Silver run can leave some Delta tables advanced. Gold remains gated by Job success. Recover by fixing the source/input and rerunning the idempotent Job; inspect Silver Delta history if a rollback is required.
- Existing live Silver tables may already point to locations changed by earlier runs. Test only with the isolated prefix/schema until a full Job and dbt run are observed.
- A Databricks Spark Python task must have this repository code and its non-Spark Python dependencies available in the Job environment. Do not trigger the main DAG until that Job is configured and a standalone isolated run passes.

## Progress

- [x] Inspect current authority, DAG, Spark code, local tests, and runbook.
- [x] Implement the remote Job contract, restore Great Expectations inside the Job, remove duplicate/dead migration code, and correct the Airflow connection contract.
- [x] Run focused unit tests, DAG import/graph proof, CLI guard, lock/Compose/syntax/diff checks.
- [x] Reproduce the Databricks Spark Python task's missing `__file__` under `exec(...)`, remove the path bootstrap, and pass the focused regression test.
- [x] Confirm committed `customers` manifests exist at the Job's S3 root, enable Spark's hidden-file option for `_manifest.json`, and pass the focused reader test.
- [x] Reproduce the serverless Spark Connect rejection of `errorifexists`, use supported append mode for an absent Delta table, and validate the initial write on Databricks with 99,441 customers.
- [x] Trace the full Job's purchase-join failure to the 2026-10-30 clickstream manifest: all 20 purchases in that run miss the current order/customer and order-item/product pairs; the eight 2026-11-05 clickstream manifests each have 20 matching purchases. Preserve the Bronze source and the quality gate.
- [x] Pin `great-expectations==1.24.1` in the Databricks Job serverless environment, matching the `uv.lock` package version, after the clean-source run exposed a namespace without `gx.get_context`. Leave Spark to the Databricks base environment.
- [x] Run a full isolated Job through Delta MERGE, purchase joins, Great Expectations, and all 13 Unity Catalog table registrations; Job `922817689219073` terminated `SUCCESS`.
- [ ] Run Spark/Delta/GX integration tests in the Linux Silver image; Docker Hub base-image retrieval failed twice, and local Windows JDK 24 cannot start the Spark gateway.
- [ ] Validate a real isolated Databricks Job and Airflow run when workspace compute is available.

## Decisions

- 2026-11-05: Use a preconfigured Databricks Spark Python Job; Airflow supplies run-specific arguments via `python_params`. This follows the owner's Databricks Spark choice without adding a second scheduler.
- 2026-11-05: Use Spark `binaryFile` for remote manifest discovery and checksum verification, so the Job uses Unity Catalog S3 access rather than a separate `boto3` credential.
- 2026-11-05: Compose derives the Airflow Databricks connection from `DBT_DATABRICKS_HOST` and `DATABRICKS_TOKEN`; the owner should enter the token only once in `.env`.

## Validation

- Focused proof: 11 host unit tests passed, including schema-key parity, S3 Spark manifest/checksum positive and negative cases, and ingestion tests. `--dataset` plus `--catalog` is rejected before Spark starts. The Airflow DAG test passed twice in the rebuilt provider image: main and isolated S3 prefix/schema graph contracts.
- Connection proof: Compose generated a JSON connection from a fake workspace host/token, Airflow 3.3.2 resolved its host/password, and the DAG tests passed after the duplicate connection requirement was removed. No real authentication was attempted.
- Integration or end-to-end proof: no successful Spark/Delta/GX or full cloud run. Linux Silver image build failed twice on Docker Hub proxy/base-image fetch; Windows Java 24 fails to start the local Spark gateway. Databricks CLI read-only access failed because its OAuth refresh token expired.
- Runtime feedback: the owner's Workspace Git-folder Job imported the updated script but raised `NameError` because Databricks executed it without `__file__`. The local `exec(...)` regression test failed with the same exception before the fix and passed afterward with all five Silver manifest tests. The fixed revision has not yet been run on Databricks.
- Runtime feedback: the next Job reported no `customers` manifests, while read-only S3 inspection found two committed manifests with 99,441 rows each under the exact requested root. Spark's default hidden-file filter skips `_manifest.json`; the new `ignoredPathSegmentRegex` option passed a red/green focused reader test and all five Silver manifest tests. Databricks Runtime 19 supports the option; the current serverless Job environment is version 6, and a live rerun is still required.
- Runtime feedback: `errorifexists` failed on the first Delta write in the serverless Spark Connect Job. A red/green unit regression and Databricks Job run `744622764828265` proved the append-mode fix, with 99,441 valid customers and no quarantine. Git-folder head was synced to `ec42a651139302d4fc3c83060ad5b663e96975d0`.
- Full isolated Job `731156643675057` wrote all 12 datasets, then reported `Clickstream purchase joins: 20 missing orders, 20 missing products`. S3 manifest/data inspection isolated all 20 to run `20261030T101916305309`; later clickstream runs joined correctly. The failed Job was canceled after the error surfaced. A copy of 90 Bronze objects, excluding the stale clickstream run, is under `codex_validation_20261105/` for full Job validation; the main Bronze prefix was not changed. Clean Job `535556821998931` was started against separate Silver and Unity Catalog schema locations and is awaiting its final result.
- Full isolated Job `731156643675057` wrote all 12 datasets, then reported `Clickstream purchase joins: 20 missing orders, 20 missing products`. S3 manifest/data inspection isolated all 20 to run `20261030T101916305309`; later clickstream runs joined correctly. The failed Job was canceled after the error surfaced. A copy of 90 Bronze objects, excluding the stale clickstream run, is under `codex_validation_20261105/` for full Job validation; the main Bronze prefix was not changed.
- Clean Job `535556821998931` wrote all 12 datasets and passed the purchase joins, then failed because the serverless Job imported a `great_expectations` module without `get_context`. The repo lock resolves Great Expectations 1.24.1. A first attempt to install `great-expectations[spark]==1.24.1` failed dependency resolution before code startup in Job `332107028696682`; the Spark extra requires PySpark below 4.2, while serverless environment 6 ships Databricks Connect 19.1. The Job environment was corrected via the Jobs API to `great-expectations==1.24.1` without the Spark extra and read back. The first clean run was canceled after its retry began.
- Clean Job `922817689219073` terminated `SUCCESS`: all 12 Silver tables written, purchase joins and Great Expectations passed, zero quarantine rows, and all 12 Silver tables plus `silver_quarantine` registered in `ecommerce_cdc.silver_validation_clean_codex_20261105`. A Unity Catalog API readback found exactly 13 tables and confirmed all 13 storage locations point to the isolated Silver root. The main Bronze prefix remains unchanged and still fails on run `20261030T101916305309`; choose a documented treatment for that historical input before running the main DAG.
- Repository-required checks: `uv lock --check`, `docker compose config --quiet`, `compileall`, and `git diff --check` passed; rerun after final edits.

## Result

Repository contract and an isolated Databricks Silver Job are validated. Keep this plan active until the main Bronze input is reconciled and an Airflow DAG run verifies Silver table locations/counts, dbt success, and Ops state. The Airflow image still installs unused PySpark/Delta/GX dependencies; reduce it after DAG proof. Current Silver processing scans and verifies all historical manifests on every run, so cost grows with Bronze history; design a durable processed-run checkpoint before claiming daily scale.
