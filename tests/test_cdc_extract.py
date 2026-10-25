"""Integration proof for CDC polling; run against a disposable cdc_test PostgreSQL DB."""

import io
import os
import unittest
from datetime import datetime, timezone
from pathlib import Path

import psycopg
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

from ingestion.cdc_extract import extract_table


class FakeS3:
    def __init__(self):
        self.objects = {}
        self.remaining_successes = None

    def head_object(self, *, Bucket, Key):
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject")
        return {}

    def upload_file(self, filename, bucket, key):
        if self.remaining_successes == 0:
            raise RuntimeError("injected upload failure")
        if self.remaining_successes is not None:
            self.remaining_successes -= 1
        self.objects[key] = Path(filename).read_bytes()


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
                cursor.execute("CREATE TABLE cdc_watermark (table_name varchar(63) PRIMARY KEY, last_updated_at timestamptz, last_run_at timestamptz)")
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

    def test_initial_rerun_tie_and_failed_upload(self):
        extract_table(self.settings, self.s3, "test-bucket", "products")
        self.assertEqual(len(self.s3.objects), 2)
        self.assertEqual(self.watermark(), datetime(2020, 1, 2, tzinfo=timezone.utc))
        extract_table(self.settings, self.s3, "test-bucket", "products")
        self.assertEqual(len(self.s3.objects), 2)

        with psycopg.connect(
            host="127.0.0.1", port=self.settings["POSTGRES_PORT"], dbname="cdc_test",
            user="cdc_test", password="cdc_test_password",
        ) as connection:
            connection.execute("INSERT INTO products VALUES (%s, %s)", ("p3", datetime(2020, 1, 2, tzinfo=timezone.utc)))
        extract_table(self.settings, self.s3, "test-bucket", "products")
        self.assertEqual(len(self.s3.objects), 3)
        boundary = [data for key, data in self.s3.objects.items() if "boundary-" in key and pq.read_table(io.BytesIO(data)).num_rows == 2]
        self.assertEqual(len(boundary), 1)
        self.assertIn("ingested_at", pq.read_table(io.BytesIO(boundary[0])).column_names)
        self.assertIn("source_table", pq.read_table(io.BytesIO(boundary[0])).column_names)

        with psycopg.connect(
            host="127.0.0.1", port=self.settings["POSTGRES_PORT"], dbname="cdc_test",
            user="cdc_test", password="cdc_test_password",
        ) as connection:
            connection.execute("INSERT INTO products VALUES (%s, %s)", ("p4", datetime(2020, 1, 3, tzinfo=timezone.utc)))
        self.s3.remaining_successes = 1
        with self.assertRaisesRegex(RuntimeError, "injected upload failure"):
            extract_table(self.settings, self.s3, "test-bucket", "products")
        self.assertEqual(self.watermark(), datetime(2020, 1, 2, tzinfo=timezone.utc))
        self.assertEqual(len(self.s3.objects), 4)
        self.s3.remaining_successes = None
        extract_table(self.settings, self.s3, "test-bucket", "products")
        self.assertEqual(self.watermark(), datetime(2020, 1, 3, tzinfo=timezone.utc))
        self.assertEqual(len(self.s3.objects), 5)
        count = len(self.s3.objects)
        extract_table(self.settings, self.s3, "test-bucket", "products")
        self.assertEqual(len(self.s3.objects), count)


if __name__ == "__main__":
    unittest.main()
