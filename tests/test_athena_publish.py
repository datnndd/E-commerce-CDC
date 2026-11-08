"""Offline proof for Athena Delta publication and counted Ops inputs."""

import os
import unittest
from unittest.mock import MagicMock, patch
from botocore.exceptions import ClientError

from orchestration import athena_publish as module


class AthenaPublishTest(unittest.TestCase):
    def setUp(self):
        self.env = patch.dict(os.environ, {
            "BI_DATABASE": "bi_aws", "DBT_TARGET_SCHEMA": "analytics_aws",
            "ATHENA_WORKGROUP": "cdc_v3", "ATHENA_RESULTS_S3": "s3://bucket/results/",
            "S3_BUCKET": "bucket", "PIPELINE_S3_PREFIX": "rehearsal",
        })
        self.env.start()
        self.addCleanup(self.env.stop)

    def test_publishes_all_delta_tables_and_ops_after_v3_check(self):
        athena = MagicMock()
        athena.get_work_group.return_value = {"WorkGroup": {"Configuration": {
            "EngineVersion": {"EffectiveEngineVersion": "Athena engine version 3"}}}}
        glue = MagicMock()
        glue.exceptions.EntityNotFoundException = ClientError
        glue.get_table.side_effect = lambda DatabaseName, Name: (
            {"Table": {"StorageDescriptor": {"Location": f"s3://bucket/gold/{Name}/"}}}
            if DatabaseName == "analytics_aws_gold" else
            (_ for _ in ()).throw(ClientError({"Error": {"Code": "EntityNotFoundException"}}, "GetTable")))
        queries = []
        with patch.object(module.boto3, "client", side_effect=lambda name, **_: athena if name == "athena" else glue), \
             patch.object(module, "run_query", side_effect=lambda sql, *_: queries.append(sql)):
            module.publish_tables()
        self.assertEqual(sum("TBLPROPERTIES ('table_type'='DELTA')" in q for q in queries), 13)
        self.assertEqual(sum(q.startswith("SELECT count(*)") for q in queries), 13)
        self.assertTrue(any("ops_pipeline_runs" in q and "rehearsal/ops/runs/" in q for q in queries))

    def test_rejects_existing_table_at_other_location(self):
        athena = MagicMock()
        athena.get_work_group.return_value = {"WorkGroup": {"Configuration": {
            "EngineVersion": {"SelectedEngineVersion": "Athena engine version 3"}}}}
        glue = MagicMock()
        glue.get_table.side_effect = lambda DatabaseName, Name: {"Table": {"StorageDescriptor": {
            "Location": "s3://bucket/gold/fct_orders/" if DatabaseName == "analytics_aws_gold"
            else "s3://bucket/old/fct_orders/"}}}
        with patch.object(module.boto3, "client", side_effect=lambda name, **_: athena if name == "athena" else glue), \
             patch.object(module, "run_query"):
            with self.assertRaisesRegex(ValueError, "points to"):
                module.publish_tables()

    def test_requires_engine_v3(self):
        athena = MagicMock()
        athena.get_work_group.return_value = {"WorkGroup": {"Configuration": {
            "EngineVersion": {"EffectiveEngineVersion": "Athena engine version 2"}}}}
        with patch.object(module.boto3, "client", return_value=athena):
            with self.assertRaisesRegex(ValueError, "engine version 3"):
                module.publish_tables()

    def test_rejects_existing_non_delta_table(self):
        athena = MagicMock()
        athena.get_work_group.return_value = {"WorkGroup": {"Configuration": {
            "EngineVersion": {"EffectiveEngineVersion": "Athena engine version 3"}}}}
        glue = MagicMock()
        glue.get_table.side_effect = lambda DatabaseName, Name: {"Table": {"StorageDescriptor": {
            "Location": f"s3://bucket/gold/{Name}/"}}}
        with patch.object(module.boto3, "client", side_effect=lambda name, **_: athena if name == "athena" else glue), \
             patch.object(module, "run_query"):
            with self.assertRaisesRegex(ValueError, "not a Delta table"):
                module.publish_tables()

    def test_rejects_invalid_query_output(self):
        with patch.dict(os.environ, {"ATHENA_RESULTS_S3": "s3:///missing-bucket"}):
            with self.assertRaisesRegex(ValueError, "must be an S3 URI"):
                module.settings()

    def test_gold_counts_come_from_query_results(self):
        with patch.object(module, "run_query", return_value=[
            {"Data": [{"VarCharValue": "_col0"}]},
            {"Data": [{"VarCharValue": "17"}]},
        ]) as query:
            facts, marts = module.count_gold_tables()
        self.assertEqual(len(facts), 7)
        self.assertEqual(len(marts), 6)
        self.assertTrue(all(value == 17 for value in (*facts.values(), *marts.values())))
        self.assertEqual(query.call_count, 13)


if __name__ == "__main__":
    unittest.main()
