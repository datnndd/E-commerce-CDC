"""Airflow graph proof: AWS Glue Silver must gate publication and dbt."""

from __future__ import annotations

import ast
import importlib.util
import json
import os
import re
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch

DAG_PATH = Path(__file__).resolve().parents[1] / "airflow" / "dags" / "ecommerce_cdc_daily.py"


# --- Lightweight Airflow Offline Shim ---
def _ensure_airflow_mock() -> None:
    """Provide minimal Airflow objects when apache-airflow is not installed locally."""
    if "airflow" in sys.modules and hasattr(sys.modules["airflow"], "__file__") and sys.modules["airflow"].__file__:
        return

    airflow = types.ModuleType("airflow")
    airflow_sdk = types.ModuleType("airflow.sdk")
    airflow_providers = types.ModuleType("airflow.providers")
    airflow_providers_amazon = types.ModuleType("airflow.providers.amazon")
    airflow_providers_amazon_aws = types.ModuleType("airflow.providers.amazon.aws")
    airflow_providers_amazon_aws_operators = types.ModuleType("airflow.providers.amazon.aws.operators")
    airflow_providers_amazon_aws_operators_glue = types.ModuleType("airflow.providers.amazon.aws.operators.glue")
    airflow_providers_standard = types.ModuleType("airflow.providers.standard")
    airflow_providers_standard_operators = types.ModuleType("airflow.providers.standard.operators")
    airflow_providers_standard_operators_bash = types.ModuleType("airflow.providers.standard.operators.bash")
    airflow_dag_processing = types.ModuleType("airflow.dag_processing")
    airflow_dag_processing_dagbag = types.ModuleType("airflow.dag_processing.dagbag")

    class MockDAG:
        _current_dag = None

        def __init__(self, dag_id: str, **kwargs):
            self.dag_id = dag_id
            self.task_dict = {}
            self.tags = kwargs.get("tags", [])
            self.params = kwargs.get("params", {})
            self.max_active_runs = kwargs.get("max_active_runs", 16)
            self.default_args = kwargs.get("default_args", {})
            self.__dict__.update(kwargs)

        @property
        def task_ids(self) -> list[str]:
            return list(self.task_dict.keys())

        def __enter__(self):
            MockDAG._current_dag = self
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            MockDAG._current_dag = None

    class MockTask:
        def __init__(self, task_id: str, **kwargs):
            self.task_id = task_id
            self.upstream_task_ids = set()
            self.downstream_task_ids = set()
            self.trigger_rule = kwargs.get("trigger_rule", "all_success")
            self.retries = kwargs.get("retries", None)
            self.env = kwargs.get("env", {})
            self.script_args = kwargs.get("script_args", {})
            self.job_name = kwargs.get("job_name", None)
            self.aws_conn_id = kwargs.get("aws_conn_id", None)
            self.wait_for_completion = kwargs.get("wait_for_completion", False)
            self.python_callable = None
            self.__dict__.update(kwargs)
            if MockDAG._current_dag is not None:
                MockDAG._current_dag.task_dict[task_id] = self

        def __rshift__(self, other):
            others = other if isinstance(other, (list, tuple, set)) else [other]
            for o in others:
                o.upstream_task_ids.add(self.task_id)
                self.downstream_task_ids.add(o.task_id)
            return other

        def __rrshift__(self, other):
            others = other if isinstance(other, (list, tuple, set)) else [other]
            for o in others:
                self.upstream_task_ids.add(o.task_id)
                o.downstream_task_ids.add(self.task_id)
            return self

    class MockGlueJobOperator(MockTask):
        pass

    class MockBashOperator(MockTask):
        pass

    class MockDagBag:
        def __init__(self, dag_folder: str | Path | None = None):
            self.import_errors = {}
            self.dags = {}
            folder = Path(dag_folder) if dag_folder else Path(__file__).resolve().parents[1] / "airflow" / "dags"
            for f in folder.glob("*.py"):
                spec = importlib.util.spec_from_file_location(f.stem, f)
                if spec and spec.loader:
                    mod = importlib.util.module_from_spec(spec)
                    try:
                        spec.loader.exec_module(mod)
                    except Exception as e:
                        self.import_errors[str(f)] = str(e)
                        continue
                    for val in vars(mod).values():
                        if isinstance(val, MockDAG):
                            self.dags[val.dag_id] = val

    def mock_task(func=None, **task_kwargs):
        def decorator(fn):
            def wrapper(*args, **kwargs):
                t = MockTask(fn.__name__, **task_kwargs)
                t.python_callable = fn
                return t
            wrapper.raw_func = fn
            return wrapper
        if func is not None:
            return decorator(func)
        return decorator

    airflow_sdk.DAG = MockDAG
    airflow_sdk.task = mock_task
    airflow_sdk.get_current_context = lambda: {}
    airflow_providers_amazon_aws_operators_glue.GlueJobOperator = MockGlueJobOperator
    airflow_providers_standard_operators_bash.BashOperator = MockBashOperator
    airflow_dag_processing_dagbag.DagBag = MockDagBag

    modules_map = {
        "airflow": airflow,
        "airflow.sdk": airflow_sdk,
        "airflow.providers": airflow_providers,
        "airflow.providers.amazon": airflow_providers_amazon,
        "airflow.providers.amazon.aws": airflow_providers_amazon_aws,
        "airflow.providers.amazon.aws.operators": airflow_providers_amazon_aws_operators,
        "airflow.providers.amazon.aws.operators.glue": airflow_providers_amazon_aws_operators_glue,
        "airflow.providers.standard": airflow_providers_standard,
        "airflow.providers.standard.operators": airflow_providers_standard_operators,
        "airflow.providers.standard.operators.bash": airflow_providers_standard_operators_bash,
        "airflow.dag_processing": airflow_dag_processing,
        "airflow.dag_processing.dagbag": airflow_dag_processing_dagbag,
        "airflow.models": airflow,
    }
    for mod_name, mod_obj in modules_map.items():
        sys.modules.setdefault(mod_name, mod_obj)


_ensure_airflow_mock()


class Stage6DagTest(unittest.TestCase):
    """Offline DAG integrity, graph topology, and contract test suite."""

    def setUp(self) -> None:
        self.env_patcher = patch.dict(os.environ, {
            "AIRFLOW_S3_BUCKET": "test-lakehouse-bucket",
            "GLUE_SILVER_JOB_NAME": "ecommerce-silver-transform",
            "GLUE_ROLE_ARN": "arn:aws:iam::111122223333:role/GlueDbtExecutionRole",
            "SILVER_S3_ROOT": "s3://test-lakehouse-bucket/silver/glue_v1",
            "DBT_SILVER_SCHEMA": "silver_aws",
            "DBT_TARGET_SCHEMA": "analytics_aws",
            "DBT_S3_LOCATION": "s3://test-lakehouse-bucket/dbt/glue_v1",
            "BI_DATABASE": "bi_aws",
            "ATHENA_WORKGROUP": "ecommerce_v3",
            "ATHENA_RESULTS_S3": "s3://test-lakehouse-bucket/athena/results/",
        })
        self.env_patcher.start()

        from airflow.dag_processing.dagbag import DagBag
        folder = Path(__file__).resolve().parents[1] / "airflow" / "dags"
        self.bag = DagBag(dag_folder=str(folder))
        self.assertFalse(self.bag.import_errors, f"DAG import errors: {self.bag.import_errors}")
        self.dag = self.bag.dags["ecommerce_cdc_daily"]

    def tearDown(self) -> None:
        self.env_patcher.stop()

    def test_quality_precedes_gold_and_failure_branch_sees_all_tasks(self):
        dag = self.dag
        self.assertEqual(dag.max_active_runs, 1)
        self.assertIn("glue", dag.tags)
        self.assertIn("aws", dag.tags)

        # Trigger rule & failure routing
        self.assertEqual(dag.task_dict["record_pipeline_failure"].trigger_rule, "one_failed")
        execution_tasks = {
            "runtime_check", "record_pipeline_start", "extract_postgres_cdc_to_bronze",
            "ingest_exchange_rates_to_bronze", "ingest_campaign_fixture_to_bronze",
            "generate_demo_clickstream_to_bronze", "run_silver_transform_on_glue",
            "build_gold_and_marts_dbt", "publish_athena_tables", "record_pipeline_success",
        }
        self.assertEqual(dag.task_dict["record_pipeline_failure"].upstream_task_ids, execution_tasks)
        self.assertEqual(dag.task_dict["fail_dag_run"].upstream_task_ids, {"record_pipeline_failure"})
        self.assertEqual(dag.task_dict["fail_dag_run"].retries, 0)

        # Pipeline dependency graph
        self.assertEqual(dag.task_dict["record_pipeline_start"].upstream_task_ids, {"runtime_check"})
        self.assertEqual(dag.task_dict["extract_postgres_cdc_to_bronze"].upstream_task_ids, {"record_pipeline_start"})
        self.assertEqual(dag.task_dict["generate_demo_clickstream_to_bronze"].upstream_task_ids, {"extract_postgres_cdc_to_bronze"})
        self.assertEqual(dag.task_dict["ingest_exchange_rates_to_bronze"].upstream_task_ids, {"record_pipeline_start"})
        self.assertEqual(dag.task_dict["ingest_campaign_fixture_to_bronze"].upstream_task_ids, {"record_pipeline_start"})

        self.assertEqual(dag.task_dict["run_silver_transform_on_glue"].upstream_task_ids, {
            "ingest_exchange_rates_to_bronze", "ingest_campaign_fixture_to_bronze", "generate_demo_clickstream_to_bronze",
        })
        self.assertEqual(dag.task_dict["build_gold_and_marts_dbt"].upstream_task_ids, {"run_silver_transform_on_glue"})
        self.assertEqual(dag.task_dict["publish_athena_tables"].upstream_task_ids, {"build_gold_and_marts_dbt"})
        self.assertEqual(dag.task_dict["record_pipeline_success"].upstream_task_ids, {"publish_athena_tables"})

        # Parameters
        self.assertEqual(dag.params["base_currency"], "BRL")
        self.assertEqual(dag.params["quote_currencies"], "USD")

        # GlueJobOperator contracts
        from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
        silver_task = dag.task_dict["run_silver_transform_on_glue"]
        self.assertIsInstance(silver_task, GlueJobOperator)
        self.assertEqual(silver_task.aws_conn_id, "aws_default")
        self.assertTrue(silver_task.wait_for_completion)

        expected_job_name = os.environ.get("GLUE_SILVER_JOB_NAME", "ecommerce-silver-transform")
        self.assertEqual(silver_task.job_name, expected_job_name)

        # Parameter mapping
        prefix = os.environ.get("PIPELINE_S3_PREFIX", "").strip("/")
        bucket = os.environ.get("AIRFLOW_S3_BUCKET") or os.environ.get("S3_BUCKET", "")
        expected_bronze_root = f"s3://{bucket}/{prefix}" if prefix else f"s3://{bucket}"
        expected_silver_root = os.environ["SILVER_S3_ROOT"]
        expected_glue_db = os.environ["DBT_SILVER_SCHEMA"]

        self.assertEqual(silver_task.script_args["--bronze_root"], expected_bronze_root)
        self.assertEqual(silver_task.script_args["--silver_root"], expected_silver_root)
        self.assertEqual(silver_task.script_args["--glue_database"], expected_glue_db)
        self.assertEqual(silver_task.script_args["--pipeline_run_id"], "{{ run_id }}")
        self.assertTrue(all("-" not in key[2:] for key in silver_task.script_args))

        # Exchange rate parameters in BashOperator
        rates_task = dag.task_dict["ingest_exchange_rates_to_bronze"]
        self.assertEqual(rates_task.env["EXCHANGE_BASE_CURRENCY"], "{{ params.base_currency }}")
        self.assertEqual(rates_task.env["EXCHANGE_QUOTE_CURRENCIES"], "{{ params.quote_currencies }}")

    def test_runtime_check_boundary_conditions(self):
        """Test preflight boundary validations in runtime_check."""
        raw_runtime_check = self.dag.task_dict["runtime_check"].python_callable

        # 1. Missing bucket
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(ValueError) as ctx:
                raw_runtime_check()
            self.assertIn("Configure AIRFLOW_S3_BUCKET or S3_BUCKET", str(ctx.exception))

        valid = {
            "AIRFLOW_S3_BUCKET": "test-bucket",
            "GLUE_SILVER_JOB_NAME": "ecommerce-silver-transform",
            "GLUE_ROLE_ARN": "arn:aws:iam::111122223333:role/GlueDbtExecutionRole",
            "SILVER_S3_ROOT": "s3://test-bucket/silver/glue_v1",
            "DBT_S3_LOCATION": "s3://test-bucket/dbt/glue_v1",
            "DBT_SILVER_SCHEMA": "silver_aws",
            "DBT_TARGET_SCHEMA": "analytics_aws",
            "BI_DATABASE": "bi_aws",
            "ATHENA_WORKGROUP": "ecommerce_v3",
            "ATHENA_RESULTS_S3": "s3://test-bucket/athena/results/",
        }

        # 2. Malicious / invalid Glue job names
        bad_names = ["bad job name", "job;rm -rf /", "job$injection", "job*wildcard", ""]
        for bad_name in bad_names:
            with patch.dict(os.environ, {**valid, "GLUE_SILVER_JOB_NAME": bad_name}, clear=True):
                with self.assertRaises(ValueError) as ctx:
                    raw_runtime_check()
                self.assertIn("GLUE_SILVER_JOB_NAME must contain only safe characters", str(ctx.exception))

        with patch.dict(os.environ, {**valid, "DBT_S3_LOCATION": ""}, clear=True):
            with self.assertRaisesRegex(ValueError, "DBT_S3_LOCATION"):
                raw_runtime_check()

        with patch.dict(os.environ, {**valid, "DBT_S3_LOCATION": "s3://test-bucket/silver/glue_v1/gold"}, clear=True):
            with self.assertRaisesRegex(ValueError, "must be separate"):
                raw_runtime_check()

        with patch.dict(os.environ, {**valid, "BI_DATABASE": "analytics_aws_gold"}, clear=True):
            with self.assertRaisesRegex(ValueError, "BI_DATABASE must be separate"):
                raw_runtime_check()

        # 3. Isolated PIPELINE_S3_PREFIX boundary conditions
        fn_globals = raw_runtime_check.__globals__
        orig_prefix = fn_globals.get("S3_PREFIX")
        try:
            fn_globals["S3_PREFIX"] = "isolated_test_prefix"

            isolated = {**valid, "SILVER_S3_ROOT": "s3://test-bucket/isolated_test_prefix/silver/glue_v1",
                        "DBT_SILVER_SCHEMA": "silver_isolated", "DBT_TARGET_SCHEMA": "analytics_isolated",
                        "BI_DATABASE": "bi_isolated", "POSTGRES_DB": "tenant_1c_test"}
            with patch.dict(os.environ, {**isolated, "DBT_SILVER_SCHEMA": "silver"}, clear=True):
                with self.assertRaises(ValueError) as ctx:
                    raw_runtime_check()
                self.assertIn("requires separate DBT_SILVER_SCHEMA and DBT_TARGET_SCHEMA", str(ctx.exception))

            with patch.dict(os.environ, {**isolated, "DBT_TARGET_SCHEMA": "analytics_dev"}, clear=True):
                with self.assertRaises(ValueError) as ctx:
                    raw_runtime_check()
                self.assertIn("requires separate DBT_SILVER_SCHEMA and DBT_TARGET_SCHEMA", str(ctx.exception))

            with patch.dict(os.environ, {**isolated, "POSTGRES_DB": "ecommerce_prod"}, clear=True):
                with self.assertRaises(ValueError) as ctx:
                    raw_runtime_check()
                self.assertIn("requires a *_1c_test PostgreSQL database", str(ctx.exception))

            with patch.dict(os.environ, isolated, clear=True):
                raw_runtime_check()
        finally:
            fn_globals["S3_PREFIX"] = orig_prefix

        with patch.dict(os.environ, valid, clear=True):
            raw_runtime_check()

    def test_ast_zero_databricks_references(self):
        """Statically verify DAG file contains zero Databricks imports, operators, or references."""
        source = DAG_PATH.read_text(encoding="utf-8")
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    self.assertNotIn("databricks", alias.name.lower())
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    self.assertNotIn("databricks", node.module.lower())
            elif isinstance(node, ast.Name):
                self.assertNotIn("databricks", node.id.lower())
            elif isinstance(node, ast.Constant) and isinstance(node.value, str):
                self.assertNotIn("databricks", node.value.lower())

    def test_webhook_alert_contains_task_context_without_secret(self):
        from types import SimpleNamespace

        spec = importlib.util.spec_from_file_location(
            "ecommerce_cdc_daily_alert_test",
            DAG_PATH,
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        ti = SimpleNamespace(
            dag_id="ecommerce_cdc_daily",
            run_id="scheduled__test",
            task_id="run_silver_transform_on_glue",
        )
        with patch.dict("os.environ", {"AIRFLOW_ALERT_WEBHOOK_URL": "https://example.test/webhook"}), \
             patch.object(module, "urlopen") as urlopen:
            urlopen.return_value.__enter__.return_value.status = 200
            module.alert_failure({"task_instance": ti})
        urlopen.assert_called_once()
        request = urlopen.call_args.args[0]
        self.assertIn(b"run_silver_transform_on_glue", request.data)
        self.assertNotIn(b"example.test/webhook", request.data)


if __name__ == "__main__":
    unittest.main()
