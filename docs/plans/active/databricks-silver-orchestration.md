# Execution Plan: Databricks Silver orchestration

Date: 2026-11-05

## Status

Active — a Databricks Job reached script startup but failed before Spark processing; a successful Spark/Delta/GX and DAG run remain open.

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
- Repository-required checks: `uv lock --check`, `docker compose config --quiet`, `compileall`, and `git diff --check` passed; rerun after final edits.

## Result

Repository contract and DAG are implemented, but no Databricks Silver Job has completed successfully. Keep this plan active until an isolated Job and DAG run verify quality failure/retry, Silver table locations/counts, dbt success, and Ops state. The Airflow image still installs unused PySpark/Delta/GX dependencies; reduce it after remote proof. Current Silver processing scans and verifies all historical manifests on every run, so cost grows with Bronze history; design a durable processed-run checkpoint before claiming daily scale.
