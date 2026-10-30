"""Integration proof for CDC polling; run against a disposable cdc_test PostgreSQL DB."""

import io
import hashlib
import json
import os
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import psycopg

from ingestion.cdc_extract import extract_table, s3_session


class CdcCredentialsTest(unittest.TestCase):
    def test_env_file_keys_take_priority_over_profile(self):
        with patch.dict(os.environ, {}, clear=True), patch("ingestion.cdc_extract.boto3.Session") as session:
            s3_session({
                "AWS_ACCESS_KEY_ID": "test-id",
                "AWS_SECRET_ACCESS_KEY": "test-secret",
                "AWS_PROFILE": "login-profile",
            })
        session.assert_called_once_with(
            aws_access_key_id="test-id", aws_secret_access_key="test-secret", aws_session_token=None,
        )

    def test_environment_key_pair_and_token_take_priority_over_env_file(self):
        environment = {
            "AWS_ACCESS_KEY_ID": "environment-id",
            "AWS_SECRET_ACCESS_KEY": "environment-secret",
            "AWS_SESSION_TOKEN": "environment-token",
        }
        with patch.dict(os.environ, environment, clear=True), patch("ingestion.cdc_extract.boto3.Session") as session:
            s3_session({"AWS_ACCESS_KEY_ID": "file-id", "AWS_SECRET_ACCESS_KEY": "file-secret"})
        session.assert_called_once_with(
            aws_access_key_id="environment-id",
            aws_secret_access_key="environment-secret",
            aws_session_token="environment-token",
        )

    def test_incomplete_keys_fail_without_falling_back_to_profile(self):
        with patch.dict(os.environ, {}, clear=True), patch("ingestion.cdc_extract.boto3.Session") as session:
            with self.assertRaisesRegex(ValueError, "Set both AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY"):
                s3_session({"AWS_ACCESS_KEY_ID": "test-id", "AWS_PROFILE": "login-profile"})
            with self.assertRaisesRegex(ValueError, "Set both AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY"):
                s3_session({"AWS_ACCESS_KEY_ID": "", "AWS_SECRET_ACCESS_KEY": "", "AWS_PROFILE": "login-profile"})
        session.assert_not_called()

    def test_partial_process_keys_do_not_mix_with_file_keys(self):
        with patch.dict(os.environ, {"AWS_ACCESS_KEY_ID": "environment-id"}, clear=True):
            with self.assertRaisesRegex(ValueError, "Set both AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY"):
                s3_session({"AWS_ACCESS_KEY_ID": "file-id", "AWS_SECRET_ACCESS_KEY": "file-secret"})

    def test_profile_fallback(self):
        with patch.dict(os.environ, {}, clear=True), patch("ingestion.cdc_extract.boto3.Session") as session:
            s3_session({"AWS_PROFILE": "login-profile"})
        session.assert_called_once_with(profile_name="login-profile")


class FakeS3:
    def __init__(self, on_upload=None):
        self.objects = {}
        self.fail_upload = False
        self.fail_manifest = False
        self.events = []
        self.on_upload = on_upload

    def upload_file(self, filename, bucket, key):
        if self.on_upload is not None:
            self.on_upload(key)
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
                    "last_updated_at timestamptz, last_run_at timestamptz)"
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

    def assert_batch(self, manifest, expected_count, expected_files=1):
        self.assertEqual(manifest["status"], "committed")
        self.assertEqual(manifest["source_system"], "postgres_oltp")
        self.assertEqual(manifest["dataset"], "products")
        self.assertEqual(manifest["row_count"], expected_count)
        self.assertEqual(len(manifest["files"]), expected_files)
        self.assertNotIn("batch_id", manifest)
        self.assertRegex(manifest["run_id"], r"^\d{8}T\d{12}$")
        ingestion_day = datetime.fromisoformat(manifest["started_at"].replace("Z", "+00:00")).date()
        prefix = f"bronze/postgres_oltp/products/load_date={ingestion_day}/run_id={manifest['run_id']}/"
        expected_names = {f"part-{index:04d}.parquet" for index in range(expected_files)} | {"_manifest.json"}
        self.assertEqual(
            {key.removeprefix(prefix) for key in self.s3.objects if key.startswith(prefix)},
            expected_names,
        )
        tables = []
        for index, file_info in enumerate(manifest["files"]):
            self.assertEqual(file_info["name"], f"part-{index:04d}.parquet")
            data_key = prefix + file_info["name"]
            data = self.s3.objects[data_key]
            self.assertEqual(file_info["size_bytes"], len(data))
            self.assertEqual(file_info["sha256"], hashlib.sha256(data).hexdigest())
            table = pq.read_table(io.BytesIO(data))
            self.assertEqual(table.num_rows, file_info["row_count"])
            tables.append(table)
        combined = pa.concat_tables(tables)
        self.assertEqual(combined.num_rows, expected_count)
        return combined

    def test_snapshot_replay_and_same_timestamp(self):
        extract_table(self.settings, self.s3, "test-bucket", "products")
        self.assertEqual(len(self.s3.objects), 2)
        self.assertEqual(self.watermark(), datetime(2020, 1, 2, tzinfo=timezone.utc))
        first = self.manifests()[0]
        self.assertEqual(first["load_type"], "snapshot")
        self.assertIsNone(first["window"]["lower_boundary_exclusive"])
        self.assertEqual(first["window"]["upper_boundary_inclusive"], "2020-01-02T00:00:00Z")
        self.assert_batch(first, 2)
        self.assertEqual([event[0] for event in self.s3.events], ["data", "manifest"])

        extract_table(self.settings, self.s3, "test-bucket", "products")
        self.assertEqual(len(self.s3.objects), 4)
        replay = self.manifests()[1]
        self.assertEqual(replay["window"]["lower_boundary_exclusive"], "2020-01-01T23:45:00Z")
        self.assert_batch(replay, 1)

        with psycopg.connect(
            host="127.0.0.1", port=self.settings["POSTGRES_PORT"], dbname="cdc_test",
            user="cdc_test", password="cdc_test_password",
        ) as connection:
            connection.execute("INSERT INTO products VALUES (%s, %s)", ("p3", datetime(2020, 1, 2, tzinfo=timezone.utc)))
        extract_table(self.settings, self.s3, "test-bucket", "products")
        self.assertEqual(len(self.s3.objects), 6)
        third = self.manifests()[2]
        self.assertNotEqual(first["run_id"], third["run_id"])
        self.assertEqual(third["load_type"], "delta")
        self.assertEqual(third["window"]["lower_boundary_exclusive"], "2020-01-01T23:45:00Z")
        table = self.assert_batch(third, 2)
        self.assertIn("ingested_at", table.column_names)
        self.assertIn("source_table", table.column_names)
        extract_table(self.settings, self.s3, "test-bucket", "products")
        self.assertEqual(len(self.s3.objects), 8)

        with psycopg.connect(
            host="127.0.0.1", port=self.settings["POSTGRES_PORT"], dbname="cdc_test",
            user="cdc_test", password="cdc_test_password",
        ) as connection:
            connection.execute("INSERT INTO products VALUES (%s, %s)", ("p4", datetime(2020, 1, 3, tzinfo=timezone.utc)))
        extract_table(self.settings, self.s3, "test-bucket", "products")
        latest = self.manifests()[4]
        self.assertEqual(latest["window"]["lower_boundary_exclusive"], "2020-01-01T23:45:00Z")
        self.assert_batch(latest, 3)

    def test_splits_parquet_files_at_max_file_rows_and_releases_db_lock_before_upload(self):
        with psycopg.connect(
            host="127.0.0.1", port=self.settings["POSTGRES_PORT"], dbname="cdc_test",
            user="cdc_test", password="cdc_test_password",
        ) as connection:
            connection.execute(
                "INSERT INTO products VALUES (%s, %s), (%s, %s), (%s, %s)",
                (
                    "p3", datetime(2020, 1, 3, tzinfo=timezone.utc),
                    "p4", datetime(2020, 1, 4, tzinfo=timezone.utc),
                    "p5", datetime(2020, 1, 5, tzinfo=timezone.utc),
                ),
            )

        lock_checks = []

        def check_watermark_unlocked(_key):
            with psycopg.connect(
                host="127.0.0.1", port=self.settings["POSTGRES_PORT"], dbname="cdc_test",
                user="cdc_test", password="cdc_test_password",
            ) as check_conn:
                row = check_conn.execute(
                    "SELECT last_updated_at FROM cdc_watermark WHERE table_name = 'products' FOR UPDATE NOWAIT"
                ).fetchone()
                lock_checks.append(row[0])

        self.s3.on_upload = check_watermark_unlocked
        extract_table(self.settings, self.s3, "test-bucket", "products", max_file_rows=2)
        self.assertEqual(lock_checks, [None, None, None])
        manifest = self.manifests()[0]
        self.assertEqual([item["row_count"] for item in manifest["files"]], [2, 2, 1])
        table = self.assert_batch(manifest, 5, expected_files=3)
        self.assertEqual(table["product_id"].to_pylist(), ["p1", "p2", "p3", "p4", "p5"])
        self.assertEqual(self.watermark(), datetime(2020, 1, 5, tzinfo=timezone.utc))

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
        self.assert_batch(self.manifests()[1], 2)

    def test_late_row_inside_fifteen_minute_overlap(self):
        extract_table(self.settings, self.s3, "test-bucket", "products")
        with psycopg.connect(
            host="127.0.0.1", port=self.settings["POSTGRES_PORT"], dbname="cdc_test",
            user="cdc_test", password="cdc_test_password",
        ) as connection:
            connection.execute(
                "INSERT INTO products VALUES (%s, %s), (%s, %s)",
                ("inside", datetime(2020, 1, 1, 23, 50, tzinfo=timezone.utc),
                 "outside", datetime(2020, 1, 1, 23, 40, tzinfo=timezone.utc)),
            )
        extract_table(self.settings, self.s3, "test-bucket", "products")
        manifest = self.manifests()[1]
        self.assertEqual(manifest["window"]["lower_boundary_exclusive"], "2020-01-01T23:45:00Z")
        table = self.assert_batch(manifest, 2)
        self.assertEqual(set(table["product_id"].to_pylist()), {"inside", "p2"})
        self.assertEqual(self.watermark(), datetime(2020, 1, 2, tzinfo=timezone.utc))

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
        self.assertNotEqual(self.manifests()[1]["run_id"], self.manifests()[2]["run_id"])
        self.assert_batch(self.manifests()[2], 2)


if __name__ == "__main__":
    unittest.main()
