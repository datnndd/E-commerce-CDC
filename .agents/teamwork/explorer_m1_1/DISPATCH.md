## 2026-11-07T10:09:55Z
You are explorer_m1_1, an exploration agent for Milestone M1 (AWS Glue Spark Silver Job Migration).
Your working directory is: c:\Users\ddat2\Downloads\Projects\E-commerce-CDC\.agents\teamwork\explorer_m1_1\
The authoritative request is located at: c:\Users\ddat2\Downloads\Projects\E-commerce-CDC\.agents\teamwork\ORIGINAL_REQUEST.md
The project master plan is located at: c:\Users\ddat2\Downloads\Projects\E-commerce-CDC\.agents\teamwork\orchestrator\PROJECT.md
You MUST read both files before starting work.

Your task is to explore and define the exact argument parsing, Glue job entrypoint, and CLI parameter contracts for migrating `spark_jobs/silver_transform.py` to an AWS Glue Spark job.
Specifically:
1. Examine `spark_jobs/silver_transform.py` argument handling (`main()` and `argparse`).
2. Design a robust dual-mode parameter parser that uses `awsglue.utils.getResolvedOptions(sys.argv, ...)` when running in AWS Glue runtime (e.g. when `--JOB_NAME` is passed or `awsglue` is present), and falls back seamlessly to `argparse` when running locally or in unit tests.
3. Determine all required parameters (`--JOB_NAME`, `--bronze-root`, `--silver-root`, `--dataset`, `--glue-database`, `--catalog`, `--schema`) and ensure optional parameters don't cause `getResolvedOptions` to crash.
4. Provide the exact implementation code snippet and unit test recommendations for testing argument parsing offline.
5. Document your findings in `analysis.md` and `handoff.md` in your working directory.
6. When finished, send a brief message with `send_message` to your orchestrator reporting completion.

Hard constraints: Strictly read-only outside your working directory.
