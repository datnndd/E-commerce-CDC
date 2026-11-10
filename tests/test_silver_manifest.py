"""Manifest payload integrity for AWS Glue / S3 Bronze reads."""

import hashlib
from io import BytesIO
import json
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from ingestion.cdc_extract import TABLES
from spark_jobs.silver_transform import KEYS, load_manifests_from_s3, merge_delta, verify_manifest_file


class SilverManifestTest(unittest.TestCase):
    def test_initial_delta_write_uses_connect_supported_mode(self):
        spark, frame = MagicMock(), MagicMock()
        with patch("spark_jobs.silver_transform.DeltaTable.isDeltaTable", return_value=False):
            merge_delta(spark, frame, "s3://project/silver/slv_customers", ["customer_id"])
        writer = frame.write.format.return_value
        frame.write.format.assert_called_once_with("delta")
        writer.mode.assert_called_once_with("append")
        writer.mode.return_value.save.assert_called_once_with("s3://project/silver/slv_customers")

    def test_aws_glue_entrypoint_without_file_global(self):
        script = Path(__file__).resolve().parents[1] / "spark_jobs" / "silver_transform.py"
        namespace = {"__name__": "aws_glue_job"}
        exec(compile(script.read_bytes(), str(script), "exec"), namespace)
        self.assertEqual(namespace["KEYS"], KEYS)
        self.assertNotIn("DATABRICKS_RUNTIME_VERSION", namespace)

    def test_oltp_keys_match_source_schema(self):
        for dataset, model in TABLES.items():
            self.assertEqual(KEYS[dataset], [column.name for column in model.__table__.primary_key.columns])

    def test_s3_payload_matches_manifest(self):
        body = b"committed Bronze payload"
        spark = MagicMock()
        spark.read.format.return_value.load.return_value.select.return_value.first.return_value = SimpleNamespace(
            size=len(body), sha=hashlib.sha256(body).hexdigest())
        file = {"size_bytes": len(body), "sha256": hashlib.sha256(body).hexdigest()}
        with patch("spark_jobs.silver_transform.F.length"), patch("spark_jobs.silver_transform.F.sha2"):
            verify_manifest_file("s3://project/bronze/orders/part-0000.parquet", file, spark)
        spark.read.format.assert_called_once_with("binaryFile")
        spark.read.format.return_value.load.assert_called_once_with(
            "s3://project/bronze/orders/part-0000.parquet")

    @patch("spark_jobs.silver_transform.boto3.client")
    def test_s3_manifest_listing_reads_hidden_files_and_preserves_run_path(self, client):
        spark = MagicMock()
        prefix = "test/bronze/postgres_oltp/orders/"
        first = prefix + "run_id=1/_manifest.json"
        second = prefix + "run_id=2/_manifest.json"
        manifests = {
            first: {"started_at": "2026-11-05T00:00:00Z", "status": "committed"},
            second: {"started_at": "2026-11-06T00:00:00Z", "status": "committed"},
        }
        s3 = client.return_value
        s3.get_paginator.return_value.paginate.return_value = [
            {"Contents": [{"Key": second}, {"Key": prefix + "part-0000.parquet"}]},
            {"Contents": [{"Key": first}]},
        ]
        s3.get_object.side_effect = lambda **kwargs: {
            "Body": BytesIO(json.dumps(manifests[kwargs["Key"]]).encode())}

        self.assertEqual(load_manifests_from_s3(spark, "s3://project/test", "orders"), [
            (manifests[first], "s3://project/" + first.rsplit("/", 1)[0]),
            (manifests[second], "s3://project/" + second.rsplit("/", 1)[0]),
        ])
        s3.get_paginator.return_value.paginate.assert_called_once_with(
            Bucket="project", Prefix=prefix)
        spark.read.format.assert_not_called()

    def test_s3_corruption_fails_before_spark_read(self):
        body = b"committed Bronze payload"
        spark = MagicMock()
        spark.read.format.return_value.load.return_value.select.return_value.first.return_value = SimpleNamespace(
            size=7, sha=hashlib.sha256(b"corrupt").hexdigest())
        file = {"size_bytes": len(body), "sha256": hashlib.sha256(body).hexdigest()}
        with patch("spark_jobs.silver_transform.F.length"), patch("spark_jobs.silver_transform.F.sha2"):
            with self.assertRaisesRegex(ValueError, "size or SHA-256 differs from manifest"):
                verify_manifest_file("s3://project/bronze/orders/part-0000.parquet", file, spark)


if __name__ == "__main__":
    unittest.main()
