"""Airflow graph proof: Silver quality must gate publication and dbt."""

import importlib.util
import unittest
from pathlib import Path
from unittest.mock import patch


@unittest.skipUnless(importlib.util.find_spec("airflow"), "run in the Airflow Docker image")
class Stage6DagTest(unittest.TestCase):
    def test_quality_precedes_gold_and_failure_branch_sees_all_tasks(self):
        from airflow.dag_processing.dagbag import DagBag

        folder = Path(__file__).resolve().parents[1] / "airflow" / "dags"
        bag = DagBag(dag_folder=str(folder))
        self.assertFalse(bag.import_errors)
        dag = bag.dags["ecommerce_cdc_daily"]
        self.assertEqual(dag.max_active_runs, 1)
        self.assertEqual(dag.task_dict["record_failure"].trigger_rule, "one_failed")
        self.assertEqual(dag.task_dict["record_failure"].upstream_task_ids,
                         set(dag.task_ids) - {"record_success", "record_failure", "mark_failed"})
        self.assertEqual(dag.task_dict["mark_failed"].upstream_task_ids, {"record_failure"})
        self.assertEqual(dag.task_dict["mark_failed"].retries, 0)
        self.assertEqual(dag.task_dict["record_success"].upstream_task_ids, {"dbt_build"})
        self.assertEqual(dag.task_dict["publish_silver"].upstream_task_ids,
                         {"verify_silver", "silver_quality_gate"})
        self.assertEqual(dag.task_dict["dbt_build"].upstream_task_ids, {"publish_silver"})
        self.assertEqual(dag.task_dict["silver_quality_gate"].upstream_task_ids, {"load_silver"})
        self.assertEqual(dag.task_dict["sync_committed_bronze"].upstream_task_ids, {
            "ingest_exchange_rates", "ingest_campaign_fixture", "ingest_demo_clickstream",
        })
        self.assertEqual(dag.task_dict["ingest_demo_clickstream"].upstream_task_ids, {"cdc_extract"})

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
