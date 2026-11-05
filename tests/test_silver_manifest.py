"""Manifest payload integrity for Databricks S3 Bronze reads."""

import hashlib
import json
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from ingestion.cdc_extract import TABLES
from spark_jobs.silver_transform import KEYS, load_manifests_from_s3, verify_manifest_file


class SilverManifestTest(unittest.TestCase):
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

    def test_manifest_listing_uses_spark_and_preserves_run_path(self):
        spark = MagicMock()
        manifest = {"started_at": "2026-11-05T00:00:00Z", "status": "committed"}
        row = SimpleNamespace(
            path="s3://project/test/bronze/postgres_oltp/orders/run_id=1/_manifest.json",
            content=json.dumps(manifest).encode())
        spark.read.format.return_value.option.return_value.option.return_value.load.return_value.select.return_value.collect.return_value = [row]
        self.assertEqual(load_manifests_from_s3(spark, "s3://project/test", "orders"), [
            (manifest, "s3://project/test/bronze/postgres_oltp/orders/run_id=1")])
        spark.read.format.return_value.option.return_value.option.return_value.load.assert_called_once_with(
            "s3://project/test/bronze/postgres_oltp/orders")

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
