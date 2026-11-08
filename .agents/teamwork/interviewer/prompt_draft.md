Migrate the lakehouse data pipeline from Databricks to AWS Glue and AWS-native services to eliminate Databricks free edition compute limits for an end-to-end portfolio showcase project. Completely remove all Databricks dependencies across the Spark Silver layer, Gold/dbt transformations, and Airflow orchestration.

Integrity mode: benchmark
The team must NOT: copy code from existing open-source projects for core logic, use pre-built libraries or frameworks for core functionality, run external scripts or delegate execution to other tools, read test source code to understand expected behaviour before implementing

## Requirements

### R1. AWS Glue Spark Silver Job
Migrate the Spark Silver processing logic (cleaning, deduplication, CDC MERGE, and quality checks) from Databricks to an AWS Glue Spark job. The job must read Bronze data from S3, perform all Silver transformations and validations, and write clean Silver tables directly to S3 without calling Databricks APIs or Unity Catalog endpoints.

### R2. Gold Layer and Data Marts Migration
Migrate the Gold layer and business data marts transformation pipeline to an AWS-compatible data processing engine (such as AWS Athena or AWS Glue) to eliminate reliance on Databricks SQL Warehouse and `dbt-databricks`. Update the dbt project configuration, profiles, and models so that Gold models and business marts can be built and tested without Databricks.

### R3. Airflow Orchestration and Pipeline Decoupling
Update the Airflow DAG (`airflow/dags/ecommerce_cdc_daily.py`) and associated orchestration configurations to run without Databricks operators. Replace Databricks tasks with AWS Glue / AWS operators to trigger the Silver Glue job and downstream Gold transformations.

### R4. Offline Verification and Test Suite Integrity
Provide and update repository verification assets—including unit tests, Airflow DAG integrity tests, and syntax/compilation checks—to verify all migrated components, schemas, and orchestration contracts offline without requiring live AWS cloud credentials.

## Acceptance Criteria

### Silver Layer (AWS Glue Spark)
- [ ] AWS Glue Spark job script is created and compatible with AWS Glue runtime, preserving all Silver data transformations, CDC deduplication, and quality validation logic.
- [ ] The AWS Glue job reads source Bronze manifests and data from S3 and writes Silver tables without calling Databricks-specific APIs or Unity Catalog endpoints.

### Gold Layer & dbt
- [ ] dbt project configuration and profiles are updated to replace `dbt-databricks` with an AWS-compatible adapter (such as `dbt-athena` or AWS Glue/Spark), removing all Databricks host, token, and cluster requirements.
- [ ] All dbt models (staging, intermediate, core facts/dimensions, and business marts) compile successfully under the updated profile and adapter configuration.

### Airflow Orchestration
- [ ] Airflow DAG `ecommerce_cdc_daily` is updated to replace Databricks operators with AWS Glue / AWS operators for Silver and downstream steps.
- [ ] DAG integrity and graph tests (`tests/test_stage6_dag.py` or equivalent) pass, verifying task dependencies and operator parameter contracts.
- [ ] All Databricks connection requirements and environment variables are removed or replaced in Docker Compose, environment examples, and Airflow initialization.

### Repository Verification
- [ ] Repository test suite (unit tests, DAG tests, manifest tests) passes completely offline without live AWS credentials.
- [ ] Code syntax and compilation checks (`python -m compileall`, format/diff checks) pass with zero errors.
