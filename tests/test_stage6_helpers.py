"""Isolated proof for committed Bronze sync and Delta publication ordering."""

import hashlib
import io
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from botocore.exceptions import ClientError

from ingestion.sync_bronze import sync_committed
from ingestion.s3 import pipeline_s3_prefix
from orchestration.publish_silver import identifier, upload_table


class FakeS3:
    def __init__(self, objects=None):
        self.objects = objects or {}
        self.uploaded = []
        self.downloads = []
        self.meta = {}

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return self

    def paginate(self, *, Bucket, Prefix):
        return [{"Contents": [{"Key": key} for key in self.objects if key.startswith(Prefix)]}]

    def get_object(self, *, Bucket, Key):
        return {"Body": io.BytesIO(self.objects[Key])}

    def download_file(self, bucket, key, filename):
        self.downloads.append(key)
        Path(filename).write_bytes(self.objects[key])

    def head_object(self, *, Bucket, Key):
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "404", "Message": "not found"}}, "HeadObject")
        return {"ContentLength": len(self.objects[Key]), "Metadata": self.meta.get(Key, {})}

    def upload_file(self, filename, bucket, key, ExtraArgs):
        self.objects[key] = Path(filename).read_bytes()
        self.meta[key] = ExtraArgs["Metadata"]
        self.uploaded.append(key)


class Stage6HelpersTest(unittest.TestCase):
    def test_sync_only_committed_manifest_files_and_repairs_corrupt_local_cache(self):
        payload = b"valid parquet bytes"
        prefix = "bronze/postgres_oltp/orders/load_date=2026-11-02/run_id=20261102T010000/"
        manifest = {
            "status": "committed", "files": [{"name": "part-0000.parquet",
                "size_bytes": len(payload), "sha256": hashlib.sha256(payload).hexdigest()}],
        }
        s3 = FakeS3({
            prefix + "part-0000.parquet": payload,
            prefix + "_manifest.json": json.dumps(manifest).encode(),
            prefix + "orphan.parquet": b"ignored",
            "bronze/api/campaign/run_id=bad/_manifest.json": b'{"status":"writing"}',
        })
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.assertEqual(sync_committed(s3, "bucket", root), (1, 1))
            target = root / prefix / "part-0000.parquet"
            self.assertEqual(target.read_bytes(), payload)
            self.assertFalse((root / prefix / "orphan.parquet").exists())
            target.write_bytes(b"bad")
            self.assertEqual(sync_committed(s3, "bucket", root), (1, 1))
            self.assertEqual(target.read_bytes(), payload)
            self.assertEqual(s3.downloads, [prefix + "part-0000.parquet"] * 2)

    def test_bad_remote_file_cannot_create_committed_local_run(self):
        prefix = "bronze/postgres_oltp/orders/load_date=2026-11-02/run_id=bad/"
        manifest = {"status": "committed", "files": [{"name": "part-0000.parquet",
                    "size_bytes": 3, "sha256": hashlib.sha256(b"good").hexdigest()}]}
        s3 = FakeS3({prefix + "part-0000.parquet": b"bad",
                     prefix + "_manifest.json": json.dumps(manifest).encode()})
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, "differs from committed manifest"):
                sync_committed(s3, "bucket", Path(directory))
            self.assertFalse((Path(directory) / prefix / "_manifest.json").exists())

    def test_delta_data_published_before_log_and_retry_is_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "slv_orders"
            (path / "_delta_log").mkdir(parents=True)
            (path / "part-0000.parquet").write_bytes(b"data")
            (path / "_delta_log" / "00000000000000000000.json").write_bytes(b"log")
            s3 = FakeS3()
            self.assertEqual(upload_table(s3, "bucket", "slv_orders", path), 2)
            self.assertEqual(s3.uploaded, [
                "silver/pipeline_live/slv_orders/part-0000.parquet",
                "silver/pipeline_live/slv_orders/_delta_log/00000000000000000000.json",
            ])
            self.assertEqual(upload_table(s3, "bucket", "slv_orders", path), 0)
            (path / "_delta_log" / "00000000000000000000.json").write_bytes(b"changed")
            with self.assertRaisesRegex(ValueError, "stop rather than overwrite"):
                upload_table(s3, "bucket", "slv_orders", path)

    def test_sql_identifier_rejects_injection(self):
        self.assertEqual(identifier("ecommerce_cdc"), "`ecommerce_cdc`")
        with self.assertRaisesRegex(ValueError, "Unsafe SQL identifier"):
            identifier("silver; DROP SCHEMA x")

    def test_isolated_prefix_is_stripped_for_local_silver_input(self):
        payload = b"isolated"
        prefix = "stage6_1c_test/bronze/postgres_oltp/orders/load_date=2026-11-02/run_id=test/"
        manifest = {"status": "committed", "files": [{"name": "part-0000.parquet",
                    "size_bytes": len(payload), "sha256": hashlib.sha256(payload).hexdigest()}]}
        s3 = FakeS3({prefix + "part-0000.parquet": payload,
                     prefix + "_manifest.json": json.dumps(manifest).encode(),
                     "bronze/postgres_oltp/orders/run_id=main/_manifest.json": b'{"status":"committed"}'})
        with tempfile.TemporaryDirectory() as directory, \
             patch.dict(os.environ, {"PIPELINE_S3_PREFIX": "stage6_1c_test"}):
            self.assertEqual(pipeline_s3_prefix(), "stage6_1c_test/")
            self.assertEqual(sync_committed(s3, "bucket", Path(directory)), (1, 1))
            self.assertEqual((Path(directory) / prefix.removeprefix("stage6_1c_test/") /
                              "part-0000.parquet").read_bytes(), payload)
        with patch.dict(os.environ, {"PIPELINE_S3_PREFIX": "../unsafe"}):
            with self.assertRaisesRegex(ValueError, "safe path segments"):
                pipeline_s3_prefix()


if __name__ == "__main__":
    unittest.main()
