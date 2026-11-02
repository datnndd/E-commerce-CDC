"""Publish local Delta Silver to S3, then point Databricks external tables at it."""

import argparse
import hashlib
import os
import re
from pathlib import Path
from urllib.parse import urlsplit

from databricks import sql
from botocore.exceptions import ClientError

from ingestion.s3 import pipeline_s3_prefix, s3_client_and_bucket
from spark_jobs.silver_transform import SILVER_NAMES


TABLES = tuple(SILVER_NAMES.values()) + ("silver_quarantine",)


def identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", value):
        raise ValueError(f"Unsafe SQL identifier: {value!r}")
    return f"`{value}`"


def upload_table(s3, bucket: str, table: str, path: Path) -> int:
    if not (path / "_delta_log").is_dir():
        raise FileNotFoundError(f"Missing Delta log: {path}")
    files = sorted(file for file in path.rglob("*") if file.is_file())
    # Delta commits become visible only after every referenced data file is present.
    files.sort(key=lambda file: file.relative_to(path).parts[0] == "_delta_log")
    uploaded = 0
    for file in files:
        relative = file.relative_to(path).as_posix()
        key = pipeline_s3_prefix() + f"silver/pipeline_live/{table}/{relative}"
        with file.open("rb") as stream:
            digest = hashlib.file_digest(stream, "sha256").hexdigest()
        try:
            obj = s3.head_object(Bucket=bucket, Key=key)
        except ClientError as exc:
            if exc.response["Error"]["Code"] not in ("404", "NoSuchKey", "NotFound"):
                raise
        else:
            if obj["ContentLength"] == file.stat().st_size and obj.get("Metadata", {}).get("sha256") == digest:
                continue
            raise ValueError(f"Published Delta file changed at {key}; stop rather than overwrite")
        s3.upload_file(str(file), bucket, key, ExtraArgs={"Metadata": {"sha256": digest}})
        obj = s3.head_object(Bucket=bucket, Key=key)
        if obj["ContentLength"] != file.stat().st_size or obj.get("Metadata", {}).get("sha256") != digest:
            raise ValueError(f"S3 upload verification failed: {key}")
        uploaded += 1
    return uploaded


def register_tables(bucket: str, catalog: str, schema: str, hostname: str, http_path: str, token: str) -> None:
    namespace = f"{identifier(catalog)}.{identifier(schema)}"
    with sql.connect(server_hostname=hostname, http_path=http_path, access_token=token) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {namespace}")
            for table in TABLES:
                name = f"{namespace}.{identifier(table)}"
                location = f"s3://{bucket}/{pipeline_s3_prefix()}silver/pipeline_live/{table}/"
                cursor.execute(f"CREATE TABLE IF NOT EXISTS {name} USING DELTA LOCATION '{location}'")
                cursor.execute(f"ALTER TABLE {name} SET LOCATION '{location}'")
                cursor.execute(f"SELECT count(*) FROM {name}")
                print(f"{table}: Databricks reads {cursor.fetchone()[0]} rows")


def publish(silver_root: Path) -> int:
    token = os.environ.get("DATABRICKS_TOKEN")
    host = os.environ.get("DBT_DATABRICKS_HOST")
    http_path = os.environ.get("DBT_DATABRICKS_HTTP_PATH")
    catalog = os.environ.get("DBT_CATALOG")
    schema = os.environ.get("DBT_SILVER_SCHEMA", "silver")
    if not all((token, host, http_path, catalog)):
        raise ValueError("Set DATABRICKS_TOKEN, DBT_DATABRICKS_HOST, DBT_DATABRICKS_HTTP_PATH, DBT_CATALOG")
    hostname = urlsplit(host).hostname or host
    identifier(catalog)
    identifier(schema)
    if pipeline_s3_prefix() and schema == "silver":
        raise ValueError("Isolated S3 prefix cannot publish into the main Silver schema")
    s3, bucket = s3_client_and_bucket()
    if not re.fullmatch(r"[a-z0-9.-]+", bucket):
        raise ValueError("Invalid S3 bucket name")
    uploaded = 0
    for table in TABLES:
        uploaded += upload_table(s3, bucket, table, silver_root / table)
    register_tables(bucket, catalog, schema, hostname, http_path, token)
    return uploaded


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--silver-root", type=Path, required=True)
    args = parser.parse_args()
    print(f"Published Silver; uploaded {publish(args.silver_root)} changed files")


if __name__ == "__main__":
    main()
