## 2026-11-07T13:58:02Z
You are worker_m4.
Working directory: c:\Users\ddat2\Downloads\Projects\E-commerce-CDC\.agents\teamwork\worker_m4\
Original Request path: c:\Users\ddat2\Downloads\Projects\E-commerce-CDC\.agents\teamwork\ORIGINAL_REQUEST.md
Master Plan: c:\Users\ddat2\Downloads\Projects\E-commerce-CDC\.agents\teamwork\orchestrator\PROJECT.md
Gate Status: c:\Users\ddat2\Downloads\Projects\E-commerce-CDC\.agents\teamwork\orchestrator\GATE_STATUS.md
Explorer Reports:
- Explorer 1 (DAG Tests): c:\Users\ddat2\Downloads\Projects\E-commerce-CDC\.agents\teamwork\explorer_m4_1\handoff.md
- Explorer 2 (Glue Tests): c:\Users\ddat2\Downloads\Projects\E-commerce-CDC\.agents\teamwork\explorer_m4_2\handoff.md
- Explorer 3 (Suite Integrity): c:\Users\ddat2\Downloads\Projects\E-commerce-CDC\.agents\teamwork\explorer_m4_3\handoff.md

DO NOT CHEAT. All implementations must be genuine. DO NOT hardcode test results, create dummy/facade implementations, or circumvent the intended task. A teamwork_auditor will independently verify your work. Integrity violations WILL be detected and your work WILL be rejected.

File Ownership:
You exclusively own and are authorized to edit:
- tests/test_stage6_dag.py
- spark_jobs/silver_transform.py
- tests/test_silver_glue_offline.py (or add new test file tests/test_silver_glue_invocation.py)

Task Scope (Milestone M4: Offline Verification, Test Suite Integrity & Acceptance):
1. Read ORIGINAL_REQUEST.md, PROJECT.md, and all 3 Explorer handoff reports.
2. Refactor `tests/test_stage6_dag.py`:
   - Replace legacy Databricks assertions (task `run_silver_transform_on_databricks`, `DatabricksRunNowOperator`, `python_params`) with `GlueJobOperator` assertions for task `run_silver_transform_on_glue`.
   - Assert script args `--bronze-root`, `--silver-root`, `--glue-database`, job name, aws_conn_id="aws_default", wait_for_completion=True.
   - Assert full DAG task graph dependencies and failure callbacks (`upstream >> failure >> fail_dag_run()`).
   - Implement the lightweight offline Airflow shim designed in `explorer_m4_1/handoff.md` so that `test_stage6_dag.py` executes dynamically and passes offline with 0 skips when `airflow.models` is absent, while remaining 100% compatible with real Airflow in containerized environments.
   - Add unit tests for `runtime_check` boundary conditions (missing bucket, malicious job names, isolated 1C prefix) and static AST checks verifying 0 occurrences of Databricks.
3. Update `spark_jobs/silver_transform.py`:
   - Add `("execution-date", False, None)` to `GLUE_OPTION_SPEC` and CLI parser arguments.
   - Remove residual Databricks references in CLI help strings (lines 467 & 469).
   - Add validation for S3 path contracts and database identifier in Glue mode.
   - Forward `resolve_fn` in `parse_args(argv=None, force_mode=None, resolve_fn=None)`.
4. Update `tests/test_silver_glue_offline.py`:
   - Add offline unit tests for `--execution-date` parsing in both Glue and CLI modes.
   - Add subprocess entrypoint tests verifying returncode 0 for `--help` and non-zero exit codes (e.g. 2) for missing/invalid required arguments.
   - Add offline test simulating `verify_silver_quality` failure (e.g. GX checkpoint error or duplicate keys) during `main()` to assert that `glue_job.commit()` is NOT called when quality checks fail.
5. Verify your work by running:
   - `uv run python -m unittest tests/test_stage6_dag.py`
   - `uv run python -m unittest tests/test_silver_glue_offline.py`
   - `uv run python -m unittest discover -s tests -p "test_*.py"` (all offline tests must pass, zero errors/failures)
   - `uv run python -m compileall airflow orchestration spark_jobs dbt_project tests ingestion` (exit code 0)
   - `git grep -i "databricks" -- airflow/ orchestration/ spark_jobs/ dbt_project/ docker/ compose.yaml .env.example` (exit code 1, zero matches)
   - `uv run dbt parse --project-dir dbt_project --profiles-dir dbt_project` (exit code 0)
6. Write a comprehensive `handoff.md` in your working directory and notify the orchestrator.
