"""Isolated local PostgreSQL -> Bronze -> Delta Silver price-update proof."""

import json
import os
import subprocess
import tempfile
import unittest
from contextlib import nullcontext
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import psycopg
from psycopg import sql

from ingestion.cdc_extract import extract_table
from ingestion.local_bronze import LocalObjectStore
from sources.oltp.load_olist import connection_settings, database_settings


ROOT = Path(__file__).resolve().parents[1]


@unittest.skipUnless(os.environ.get("SILVER_PG_E2E") == "1", "set SILVER_PG_E2E=1 with local PostgreSQL and Docker")
class SilverPostgresE2ETest(unittest.TestCase):
    def docker(self, data_root: Path, *command: str) -> str:
        args = [
            "docker", "run", "--rm",
            "--mount", f"type=bind,source={ROOT},target=/workspace",
            "--mount", f"type=bind,source={data_root},target=/data",
            "--mount", "type=volume,source=ecommerce-silver-ivy,target=/root/.ivy2.5.2",
            "ecommerce-silver-local", "uv", "run", "--frozen", "python", *command,
        ]
        result = subprocess.run(args, cwd=ROOT, capture_output=True, text=True, timeout=300)
        if result.returncode:
            self.fail(f"Docker Silver command failed:\n{(result.stdout + result.stderr)[-3000:]}")
        return result.stdout

    def test_product_price_flows_from_postgres_through_bronze_to_silver(self):
        settings = database_settings()
        if settings["POSTGRES_HOST"] not in ("127.0.0.1", "localhost"):
            self.skipTest("the test must never connect to a remote PostgreSQL host")
        db_name = "silver_e2e_" + uuid4().hex[:12]
        admin = {**connection_settings(settings), "dbname": "postgres"}
        with psycopg.connect(**admin, autocommit=True) as connection:
            connection.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
        try:
            test_settings = {**settings, "POSTGRES_DB": db_name}
            first = datetime(2026, 1, 1, tzinfo=timezone.utc)
            second = first + timedelta(hours=1)
            with psycopg.connect(**connection_settings(test_settings)) as connection:
                connection.execute("CREATE TABLE products (product_id varchar(32) PRIMARY KEY, current_price numeric(12,2), created_at timestamptz, updated_at timestamptz NOT NULL)")
                connection.execute("CREATE TABLE cdc_watermark (table_name varchar(63) PRIMARY KEY, last_updated_at timestamptz, last_run_at timestamptz)")
                connection.execute("INSERT INTO products VALUES (%s, %s, %s, %s)", ("p1", Decimal("10.00"), first, first))
            # shortcut: Docker Desktop can deny Windows cleanup of bind mounts; retain ignored test artifacts.
            with nullcontext(Path(tempfile.mkdtemp(prefix="silver_e2e_", dir=ROOT / "logs"))) as data_root:
                store = LocalObjectStore(data_root)
                extract_table(test_settings, store, "local", "products")
                self.docker(data_root, "-m", "spark_jobs.silver_transform", "--bronze-root", "/data",
                            "--silver-root", "/data/silver", "--dataset", "products")
                self.assertIn("price=10.00", self.docker(data_root, "-c", self.price_check_script("10.00")))

                with psycopg.connect(**connection_settings(test_settings)) as connection:
                    connection.execute("UPDATE products SET current_price = %s, updated_at = %s WHERE product_id = %s",
                                       (Decimal("20.00"), second, "p1"))
                extract_table(test_settings, store, "local", "products")
                self.docker(data_root, "-m", "spark_jobs.silver_transform", "--bronze-root", "/data",
                            "--silver-root", "/data/silver", "--dataset", "products")
                self.assertIn("price=20.00", self.docker(data_root, "-c", self.price_check_script("20.00")))
                manifests = sorted((data_root / "bronze" / "postgres_oltp" / "products").rglob("_manifest.json"))
                self.assertEqual([json.loads(path.read_text())["load_type"] for path in manifests], ["snapshot", "delta"])
                with psycopg.connect(**connection_settings(test_settings)) as connection:
                    self.assertEqual(connection.execute("SELECT last_updated_at FROM cdc_watermark WHERE table_name = 'products'").fetchone()[0], second)
        finally:
            with psycopg.connect(**admin, autocommit=True) as connection:
                connection.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(db_name)))

    @staticmethod
    def price_check_script(expected: str) -> str:
        return (
            "from spark_jobs.silver_transform import local_spark; "
            "s=local_spark(); "
            "rows=s.read.format('delta').load('/data/silver/slv_products').select('product_id','current_price').collect(); "
            f"assert len(rows)==1 and rows[0].product_id=='p1' and str(rows[0].current_price)=='{expected}', rows; "
            "print('price='+str(rows[0].current_price)); s.stop()"
        )


if __name__ == "__main__":
    unittest.main()
