"""Airflow graph proof: Silver quality must gate publication and dbt."""

import importlib.util
import unittest
from pathlib import Path
from unittest.mock import patch


@unittest.skipUnless(importlib.util.find_spec("airflow.models"), "run in the Airflow Docker image")
class Stage6DagTest(unittest.TestCase):
    def test_quality_precedes_gold_and_failure_branch_sees_all_tasks(self):
        from airflow.dag_processing.dagbag import DagBag

        folder = Path(__file__).resolve().parents[1] / "airflow" / "dags"
        bag = DagBag(dag_folder=str(folder))
        self.assertFalse(bag.import_errors)
        dag = bag.dags["ecommerce_cdc_daily"]
        self.assertEqual(dag.max_active_runs, 1)
        self.assertEqual(dag.task_dict["record_pipeline_failure"].trigger_rule, "one_failed")
        self.assertEqual(
            dag.task_dict["record_pipeline_failure"].upstream_task_ids,
            set(dag.task_ids) - {"record_pipeline_success", "record_pipeline_failure", "fail_dag_run"},
        )
        self.assertEqual(dag.task_dict["fail_dag_run"].upstream_task_ids, {"record_pipeline_failure"})
        self.assertEqual(dag.task_dict["fail_dag_run"].retries, 0)
        self.assertEqual(dag.task_dict["record_pipeline_success"].upstream_task_ids, {"build_gold_and_marts_dbt"})
        self.assertEqual(dag.task_dict["build_gold_and_marts_dbt"].upstream_task_ids, {"run_silver_transform_on_databricks"})
        self.assertEqual(dag.task_dict["run_silver_transform_on_databricks"].upstream_task_ids, {
            "ingest_exchange_rates_to_bronze", "ingest_campaign_fixture_to_bronze", "generate_demo_clickstream_to_bronze",
        })
        self.assertEqual(dag.task_dict["generate_demo_clickstream_to_bronze"].upstream_task_ids, {"extract_postgres_cdc_to_bronze"})
        self.assertEqual(dag.params["base_currency"], "BRL")
        self.assertEqual(dag.params["quote_currencies"], "USD")
        self.assertIn("databricks_silver_job_id", dag.params)
        rates_task = dag.task_dict["ingest_exchange_rates_to_bronze"]
        self.assertEqual(rates_task.env["EXCHANGE_BASE_CURRENCY"], "{{ params.base_currency }}")
        self.assertEqual(rates_task.env["EXCHANGE_QUOTE_CURRENCIES"], "{{ params.quote_currencies }}")

    def test_webhook_alert_contains_task_context_without_secret(self):
        from types import SimpleNamespace

        spec = importlib.util.spec_from_file_location(
            "ecommerce_cdc_daily_test",
            Path(__file__).resolve().parents[1] / "airflow" / "dags" / "ecommerce_cdc_daily.py",
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        ti = SimpleNamespace(dag_id="ecommerce_cdc_daily", run_id="scheduled__test",
                             task_id="silver_quality_gate")
        with patch.dict("os.environ", {"AIRFLOW_ALERT_WEBHOOK_URL": "https://example.test/webhook"}), \
             patch.object(module, "urlopen") as urlopen:
            urlopen.return_value.__enter__.return_value.status = 200
            module.alert_failure({"task_instance": ti})
        urlopen.assert_called_once()
        request = urlopen.call_args.args[0]
        self.assertIn(b"silver_quality_gate", request.data)
        self.assertNotIn(b"example.test/webhook", request.data)


if __name__ == "__main__":
    unittest.main()
