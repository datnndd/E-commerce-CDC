"""S3 Ops persistence and measured-count regression tests."""

import io
import json
import unittest
from unittest.mock import patch

from botocore.exceptions import ClientError

from orchestration import ops_runs
from spark_jobs.ops_key import silver_metrics_key


class FakeS3:
    def __init__(self):
        self.objects = {}

    def put_object(self, Bucket, Key, Body, **kwargs):
        self.objects[Key] = Body

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey", "Message": "missing"}}, "GetObject")
        return {"Body": io.BytesIO(self.objects[Key])}

    def get_paginator(self, name):
        return self

    def paginate(self, Bucket, Prefix):
        return [{"Contents": [{"Key": key} for key in self.objects if key.startswith(Prefix)]}]


class OpsRunsTest(unittest.TestCase):
    def setUp(self):
        self.s3 = FakeS3()
        self.client = patch("orchestration.ops_runs.s3_client_and_bucket", return_value=(self.s3, "bucket"))
        self.client.start()

    def tearDown(self):
        self.client.stop()

    def test_start_and_success_are_one_s3_object_with_real_counts(self):
        ops_runs.start("dag", "run", namespace_value="main")
        key = ops_runs.run_key("dag", "run", "main")
        self.assertEqual(len(self.s3.objects), 1)
        metrics = {"run_id": "run", "silver_counts": {"slv_orders": 10, "slv_customers": 4},
                   "quarantine_rows": 2}
        self.s3.objects[silver_metrics_key("", "run")] = json.dumps(metrics).encode()
        with patch("orchestration.athena_publish.count_gold_tables", return_value=({"fct_orders": 10}, {"mart_daily_sales": 5})):
            record = ops_runs.finish("dag", "run", "success", namespace_value="main")
        self.assertEqual(record["silver_rows"], 14)
        self.assertEqual(record["gold_rows"], 10)
        self.assertEqual(record["mart_rows"], 5)
        self.assertEqual(record["quarantine_rows"], 2)
        self.assertEqual(json.loads(self.s3.objects[key])["status"], "success")
        self.assertEqual(len([k for k in self.s3.objects if k.startswith("ops/runs/")]), 1)

    def test_missing_metrics_cannot_report_success(self):
        ops_runs.start("dag", "run", namespace_value="main")
        with self.assertRaisesRegex(ValueError, "requires measured Silver"):
            ops_runs.finish("dag", "run", "success", namespace_value="main")
        self.assertEqual(json.loads(self.s3.objects[ops_runs.run_key("dag", "run", "main")])["status"], "running")

    def test_failure_keeps_unknown_counts_null(self):
        record = ops_runs.finish("dag", "run", "failed", failed_task="silver",
                                 namespace_value="isolated")
        self.assertEqual(record["status"], "failed")
        self.assertIsNone(record["silver_rows"])
        self.assertIsNone(record["gold_rows"])
        self.assertIsNone(record["quarantine_rows"])
        self.assertEqual(record["failed_task"], "silver")
        self.assertEqual(len(self.s3.objects), 1)

    def test_run_key_is_stable_and_isolated(self):
        self.assertEqual(ops_runs.run_key("dag", "run", "main"), ops_runs.run_key("dag", "run", "main"))
        self.assertNotEqual(ops_runs.run_key("dag", "run", "main"), ops_runs.run_key("dag", "run", "test"))
        self.assertTrue(ops_runs.run_key("dag", "run/unsafe", "test").startswith("test/ops/runs/"))


if __name__ == "__main__":
    unittest.main()
