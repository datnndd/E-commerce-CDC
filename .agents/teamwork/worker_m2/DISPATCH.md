## 2026-11-07T11:56:27Z
You are worker_m2, an implementation worker for Milestone M2 (Gold Layer & dbt Migration to AWS).
Your working directory is: c:\Users\ddat2\Downloads\Projects\E-commerce-CDC\.agents\teamwork\worker_m2\
The authoritative request is located at: c:\Users\ddat2\Downloads\Projects\E-commerce-CDC\.agents\teamwork\ORIGINAL_REQUEST.md
The project master plan is located at: c:\Users\ddat2\Downloads\Projects\E-commerce-CDC\.agents\teamwork\orchestrator\PROJECT.md
You MUST read both files before starting work.

Review the handoff reports from the M2 Explorers:
- c:\Users\ddat2\Downloads\Projects\E-commerce-CDC\.agents\teamwork\explorer_m2_1\handoff.md
- c:\Users\ddat2\Downloads\Projects\E-commerce-CDC\.agents\teamwork\explorer_m2_2\handoff.md
- c:\Users\ddat2\Downloads\Projects\E-commerce-CDC\.agents\teamwork\explorer_m2_3\handoff.md

## Exclusive File Ownership
You exclusively own and may edit or create the following files:
- `dbt_project/dbt_project.yml`
- `dbt_project/profiles.yml`
- `dbt_project/profiles.yml.example`
- `airflow/dbt_profile/profiles.yml`
- `dbt_project/models/staging/sources.yml`
- `pyproject.toml`
- `tests/test_dbt_offline.py`

DO NOT edit any other repository files.

## MANDATORY INTEGRITY WARNING
DO NOT CHEAT. All implementations must be genuine. DO NOT hardcode test results, create dummy/facade implementations, or circumvent the intended task. A teamwork_auditor will independently verify your work. Integrity violations WILL be detected and your work WILL be rejected.

## Detailed Tasks
1. Update `pyproject.toml`:
   - Replace `"dbt-databricks>=1.10,<1.12"` with `"dbt-glue>=1.8,<1.10"`.
   - Update lockfile if needed via `uv lock`.

2. Update `dbt_project/dbt_project.yml`:
   - Change `profile: ecommerce_databricks` to `profile: ecommerce_aws`.

3. Update `dbt_project/profiles.yml` and `dbt_project/profiles.yml.example`:
   - Configure profile `ecommerce_aws` targeting `type: glue` with safe defaults:
     ```yaml
     ecommerce_aws:
       target: dev
       outputs:
         dev:
           type: glue
           query_type: spark
           region: "{{ env_var('AWS_REGION', 'ap-southeast-1') }}"
           role_arn: "{{ env_var('GLUE_ROLE_ARN', 'arn:aws:iam::123456789012:role/GlueDbtExecutionRole') }}"
           schema: "{{ env_var('DBT_TARGET_SCHEMA', 'analytics_dev') }}"
           database: "{{ env_var('GLUE_DATABASE', 'default') }}"
           location: "{{ env_var('DBT_S3_LOCATION', 's3://ecommerce-lakehouse/dbt') }}"
           datalake_formats: "delta"
           workers: 2
           worker_type: G.1X
           idle_timeout: 10
     ```
   - Completely remove Databricks host, token, http_path, and cluster requirements.

4. Update `airflow/dbt_profile/profiles.yml`:
   - Replace the Databricks profile with `ecommerce_aws` matching the container AWS Glue configuration.

5. Update `dbt_project/models/staging/sources.yml`:
   - Remove `database: "{{ env_var('DBT_CATALOG') }}"` (line 5).
   - Ensure all 12 Silver sources resolve cleanly using 2-tier namespace (`silver.<table_name>`).

6. Create `tests/test_dbt_offline.py`:
   - Implement unit tests verifying:
     - `dbt parse` succeeds with returncode 0 offline without live AWS credentials or Databricks environment variables.
     - All 35 models, 2 snapshots, and 19 custom data tests are indexed in `manifest.json`.
     - Zero references to `dbt-databricks`, `DBT_DATABRICKS_HOST`, or `DBT_DATABRICKS_HTTP_PATH` remain in `dbt_project/`.
     - Sources resolve without requiring `DBT_CATALOG`.

7. Run Verification Commands:
   - `uv run dbt parse --project-dir dbt_project --profiles-dir dbt_project --no-version-check --no-send-anonymous-usage-stats`
   - `python -m compileall dbt_project tests`
   - `python -m unittest tests/test_dbt_offline.py`
   - `python -m unittest discover -s tests -p "test_*.py"`
   - Ensure all offline tests pass with returncode 0.

8. Deliver Handoff Report:
   - Write `c:\Users\ddat2\Downloads\Projects\E-commerce-CDC\.agents\teamwork\worker_m2\handoff.md`.
   - Send completion message via `send_message` to your orchestrator.
