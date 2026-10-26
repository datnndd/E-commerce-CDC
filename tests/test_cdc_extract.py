"""Integration proof for CDC polling; run against a disposable cdc_test PostgreSQL DB."""

import io
import hashlib
import json
import os
import unittest
from datetime import datetime, timezone
from pathlib import Path

import psycopg
import pyarrow.parquet as pq

from ingestion.cdc_extract import extract_table


class FakeS3:
    def __init__(self):
        self.objects = {}
        self.fail_upload = False
        self.fail_manifest = False
        self.events = []

    def upload_file(self, filename, bucket, key):
        if self.fail_upload:
            raise RuntimeError("injected upload failure")
        self.objects[key] = Path(filename).read_bytes()
        self.events.append(("data", key))

    def put_object(self, *, Bucket, Key, Body, ContentType):
        if self.fail_manifest:
            raise RuntimeError("injected manifest failure")
        self.objects[Key] = Body
        self.events.append(("manifest", Key))


@unittest.skipUnless(os.environ.get("CDC_TEST_PG_PORT"), "set CDC_TEST_PG_PORT for disposable PostgreSQL")
class CdcExtractTest(unittest.TestCase):
    def setUp(self):
        self.settings = {
            "POSTGRES_HOST": "127.0.0.1",
            "POSTGRES_PORT": os.environ["CDC_TEST_PG_PORT"],
            "POSTGRES_DB": "cdc_test",
            "POSTGRES_USER": "cdc_test",
            "POSTGRES_PASSWORD": "cdc_test_password",
            "POSTGRES_SSLMODE": "disable",
        }
        with psycopg.connect(
            host="127.0.0.1", port=self.settings["POSTGRES_PORT"], dbname="cdc_test",
            user="cdc_test", password="cdc_test_password",
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute("DROP TABLE IF EXISTS cdc_watermark")
                cursor.execute("DROP TABLE IF EXISTS products")
                cursor.execute("CREATE TABLE products (product_id varchar(32) PRIMARY KEY, updated_at timestamptz NOT NULL)")
                cursor.execute(
                    "CREATE TABLE cdc_watermark (table_name varchar(63) PRIMARY KEY, "
                    "last_updated_at timestamptz, last_run_at timestamptz, last_boundary_hash varchar(64))"
                )
                cursor.execute(
                    "INSERT INTO products VALUES (%s, %s), (%s, %s)",
                    ("p1", datetime(2020, 1, 1, tzinfo=timezone.utc), "p2", datetime(2020, 1, 2, tzinfo=timezone.utc)),
                )
        self.s3 = FakeS3()

    def watermark(self):
        with psycopg.connect(
            host="127.0.0.1", port=self.settings["POSTGRES_PORT"], dbname="cdc_test",
            user="cdc_test", password="cdc_test_password",
        ) as connection:
            return connection.execute("SELECT last_updated_at FROM cdc_watermark WHERE table_name = 'products'").fetchone()[0]

    def manifests(self):
        return [json.loads(data) for key, data in self.s3.objects.items() if key.endswith("/_manifest.json")]

    def assert_batch(self, manifest, expected_count):
        self.assertEqual(manifest["status"], "committed")
        self.assertEqual(manifest["source_system"], "postgres_oltp")
        self.assertEqual(manifest["dataset"], "products")
        self.assertEqual(manifest["row_count"], expected_count)
        self.assertEqual(len(manifest["files"]), 1)
        self.assertNotIn("attempt", manifest)
        batch_keys = [key for key in self.s3.objects if f"batch_id={manifest['batch_id']}/" in key]
        self.assertEqual(len(batch_keys), 2)
        data_key = next(key for key in batch_keys if key.endswith("/part-0000.parquet"))
        data = self.s3.objects[data_key]
        self.assertEqual(manifest["files"][0]["size_bytes"], len(data))
        self.assertEqual(manifest["files"][0]["sha256"], hashlib.sha256(data).hexdigest())
        self.assertEqual(pq.read_table(io.BytesIO(data)).num_rows, expected_count)
        return pq.read_table(io.BytesIO(data))

    def test_snapshot_unchanged_rerun_and_same_timestamp(self):
        extract_table(self.settings, self.s3, "test-bucket", "products")
        self.assertEqual(len(self.s3.objects), 2)
        self.assertEqual(self.watermark(), datetime(2020, 1, 2, tzinfo=timezone.utc))
        first = self.manifests()[0]
        self.assertEqual(first["load_type"], "snapshot")
        self.assertIsNone(first["window"]["lower_boundary_exclusive"])
        self.assertEqual(first["window"]["upper_boundary_inclusive"], "2020-01-02T00:00:00Z")
        self.assertRegex(first["batch_id"], r"^products_snapshot_20200102T000000000000Z_[0-9a-f]{8}$")
        self.assert_batch(first, 2)
        self.assertEqual([event[0] for event in self.s3.events], ["data", "manifest"])

        extract_table(self.settings, self.s3, "test-bucket", "products")
        self.assertEqual(len(self.s3.objects), 2)

        with psycopg.connect(
            host="127.0.0.1", port=self.settings["POSTGRES_PORT"], dbname="cdc_test",
            user="cdc_test", password="cdc_test_password",
        ) as connection:
            connection.execute("INSERT INTO products VALUES (%s, %s)", ("p3", datetime(2020, 1, 2, tzinfo=timezone.utc)))
        extract_table(self.settings, self.s3, "test-bucket", "products")
        self.assertEqual(len(self.s3.objects), 4)
        second = self.manifests()[1]
        self.assertNotEqual(first["batch_id"], second["batch_id"])
        self.assertEqual(second["load_type"], "delta")
        self.assertEqual(second["window"]["lower_boundary_exclusive"], "2020-01-01T23:59:59.999999Z")
        self.assertRegex(
            second["batch_id"],
            r"^products_20200101T235959999999Z_20200102T000000000000Z_[0-9a-f]{8}$",
        )
        table = self.assert_batch(second, 2)
        self.assertIn("ingested_at", table.column_names)
        self.assertIn("source_table", table.column_names)
        extract_table(self.settings, self.s3, "test-bucket", "products")
        self.assertEqual(len(self.s3.objects), 4)

        with psycopg.connect(
            host="127.0.0.1", port=self.settings["POSTGRES_PORT"], dbname="cdc_test",
            user="cdc_test", password="cdc_test_password",
        ) as connection:
            connection.execute("INSERT INTO products VALUES (%s, %s)", ("p4", datetime(2020, 1, 3, tzinfo=timezone.utc)))
        extract_table(self.settings, self.s3, "test-bucket", "products")
        third = self.manifests()[2]
        self.assertEqual(third["window"]["lower_boundary_exclusive"], "2020-01-02T00:00:00Z")
        self.assertRegex(
            third["batch_id"],
            r"^products_20200102T000000000000Z_20200103T000000000000Z_[0-9a-f]{8}$",
        )
        self.assert_batch(third, 1)

    def test_failed_upload_and_manifest_leave_watermark_unchanged(self):
        extract_table(self.settings, self.s3, "test-bucket", "products")
        with psycopg.connect(
            host="127.0.0.1", port=self.settings["POSTGRES_PORT"], dbname="cdc_test",
            user="cdc_test", password="cdc_test_password",
        ) as connection:
            connection.execute("INSERT INTO products VALUES (%s, %s)", ("p3", datetime(2020, 1, 3, tzinfo=timezone.utc)))
        self.s3.fail_upload = True
        with self.assertRaisesRegex(RuntimeError, "injected upload failure"):
            extract_table(self.settings, self.s3, "test-bucket", "products")
        self.assertEqual(self.watermark(), datetime(2020, 1, 2, tzinfo=timezone.utc))
        self.assertEqual(len(self.s3.objects), 2)

        self.s3.fail_upload = False
        self.s3.fail_manifest = True
        with self.assertRaisesRegex(RuntimeError, "injected manifest failure"):
            extract_table(self.settings, self.s3, "test-bucket", "products")
        self.assertEqual(self.watermark(), datetime(2020, 1, 2, tzinfo=timezone.utc))
        self.assertEqual(len(self.manifests()), 1)
        self.assertEqual(len(self.s3.objects), 3)

        self.s3.fail_manifest = False
        extract_table(self.settings, self.s3, "test-bucket", "products")
        self.assertEqual(self.watermark(), datetime(2020, 1, 3, tzinfo=timezone.utc))
        self.assertEqual(len(self.manifests()), 2)
        self.assertEqual(len(self.s3.objects), 5)
        self.assert_batch(self.manifests()[1], 1)
        count = len(self.s3.objects)
        extract_table(self.settings, self.s3, "test-bucket", "products")
        self.assertEqual(len(self.s3.objects), count)

    def test_committed_manifest_survives_database_failure_and_retry(self):
        extract_table(self.settings, self.s3, "test-bucket", "products")
        with psycopg.connect(
            host="127.0.0.1", port=self.settings["POSTGRES_PORT"], dbname="cdc_test",
            user="cdc_test", password="cdc_test_password",
        ) as connection:
            connection.execute("INSERT INTO products VALUES (%s, %s)", ("p3", datetime(2020, 1, 3, tzinfo=timezone.utc)))
            connection.execute(
                "CREATE OR REPLACE FUNCTION fail_watermark_update() RETURNS trigger LANGUAGE plpgsql AS $$ "
                "BEGIN RAISE EXCEPTION 'injected watermark failure'; END $$"
            )
            connection.execute(
                "CREATE TRIGGER fail_watermark_update BEFORE UPDATE ON cdc_watermark "
                "FOR EACH ROW EXECUTE FUNCTION fail_watermark_update()"
            )
        with self.assertRaisesRegex(psycopg.Error, "injected watermark failure"):
            extract_table(self.settings, self.s3, "test-bucket", "products")
        self.assertEqual(self.watermark(), datetime(2020, 1, 2, tzinfo=timezone.utc))
        self.assertEqual(len(self.manifests()), 2)

        with psycopg.connect(
            host="127.0.0.1", port=self.settings["POSTGRES_PORT"], dbname="cdc_test",
            user="cdc_test", password="cdc_test_password",
        ) as connection:
            connection.execute("DROP TRIGGER fail_watermark_update ON cdc_watermark")
            connection.execute("DROP FUNCTION fail_watermark_update()")
        extract_table(self.settings, self.s3, "test-bucket", "products")
        self.assertEqual(self.watermark(), datetime(2020, 1, 3, tzinfo=timezone.utc))
        self.assertEqual(len(self.manifests()), 3)
        self.assertNotEqual(self.manifests()[1]["batch_id"], self.manifests()[2]["batch_id"])
        self.assert_batch(self.manifests()[2], 1)


if __name__ == "__main__":
    unittest.main()
