"""Manifest payload integrity for Databricks S3 Bronze reads."""

import hashlib
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

    def test_spark_python_entrypoint_without_file_global(self):
        script = Path(__file__).resolve().parents[1] / "spark_jobs" / "silver_transform.py"
        namespace = {"__name__": "databricks_spark_python_task"}
        exec(compile(script.read_bytes(), str(script), "exec"), namespace)
        self.assertEqual(namespace["KEYS"], KEYS)

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
        reader = spark.read.format.return_value
        recursive = reader.option.return_value
        manifests_only = recursive.option.return_value
        include_hidden = manifests_only.option.return_value
        include_hidden.load.return_value.select.return_value.collect.return_value = [row]
        self.assertEqual(load_manifests_from_s3(spark, "s3://project/test", "orders"), [
            (manifest, "s3://project/test/bronze/postgres_oltp/orders/run_id=1")])
        reader.option.assert_called_once_with("recursiveFileLookup", "true")
        recursive.option.assert_called_once_with("pathGlobFilter", "_manifest.json")
        manifests_only.option.assert_called_once_with("ignoredPathSegmentRegex", "")
        include_hidden.load.assert_called_once_with(
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
