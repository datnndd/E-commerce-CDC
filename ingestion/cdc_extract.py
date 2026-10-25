"""Poll OLTP tables into idempotent Parquet objects in S3 Bronze."""

import argparse
import hashlib
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import psycopg
from botocore.exceptions import ClientError
from dotenv import dotenv_values
from psycopg import IsolationLevel, sql
from sqlalchemy import BigInteger, Boolean, DateTime, Float, Integer, Numeric, String

from sources.oltp.load_olist import FILES, ROOT, connection_settings, database_settings


TABLES = {model.__tablename__: model for model, _ in FILES}
BATCH_ROWS = 10_000


def arrow_type(column):
    column_type = column.type
    if isinstance(column_type, BigInteger):
        return pa.int64()
    if isinstance(column_type, Integer):
        return pa.int32()
    if isinstance(column_type, Boolean):
        return pa.bool_()
    if isinstance(column_type, Float):
        return pa.float64()
    if isinstance(column_type, Numeric):
        return pa.decimal128(column_type.precision, column_type.scale)
    if isinstance(column_type, DateTime):
        return pa.timestamp("us", tz="UTC" if column_type.timezone else None)
    if isinstance(column_type, String):
        return pa.string()
    raise TypeError(f"Unsupported source column type: {column.name} {column_type}")


def write_parquet(connection, model, table_name, last_updated_at, max_updated_at, kind, ingested_at):
    table = model.__table__
    if kind == "older":
        predicate = sql.SQL("updated_at < %s") if last_updated_at is None else sql.SQL(
            "updated_at >= %s AND updated_at < %s"
        )
        parameters = (max_updated_at,) if last_updated_at is None else (last_updated_at, max_updated_at)
    else:
        predicate = sql.SQL("updated_at = %s")
        parameters = (max_updated_at,)

    primary_key = sql.SQL(", ").join(sql.Identifier(column.name) for column in table.primary_key.columns)
    query = sql.SQL("SELECT * FROM {} WHERE {} ORDER BY updated_at, {}").format(
        sql.Identifier(table_name), predicate, primary_key
    )
    path = None
    with connection.cursor(name=f"cdc_{table_name}_{kind}") as cursor:
        cursor.execute(query, parameters)
        names = [column.name for column in cursor.description]
        schema = pa.schema(
            [pa.field(name, arrow_type(table.columns[name])) for name in names]
            + [pa.field("ingested_at", pa.timestamp("us", tz="UTC")), pa.field("source_table", pa.string())]
        )
        digest = hashlib.sha256(repr(names).encode("utf-8"))
        count = 0
        writer = None
        try:
            while rows := cursor.fetchmany(BATCH_ROWS):
                if writer is None:
                    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as temp:
                        path = Path(temp.name)
                    writer = pq.ParquetWriter(path, schema, compression="snappy")
                for row in rows:
                    digest.update(repr(row).encode("utf-8"))
                    digest.update(b"\n")
                batch = [dict(zip(names, row)) | {"ingested_at": ingested_at, "source_table": table_name} for row in rows]
                writer.write_table(pa.Table.from_pylist(batch, schema=schema))
                count += len(rows)
        except Exception:
            if writer is not None:
                writer.close()
                writer = None
            if path is not None:
                path.unlink(missing_ok=True)
            raise
        finally:
            if writer is not None:
                writer.close()
    return path, count, digest.hexdigest()


def object_exists(s3, bucket, key):
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise


def extract_table(settings, s3, bucket, table_name):
    ingested_at = datetime.now(timezone.utc)
    with psycopg.connect(**connection_settings(settings), options="-c TimeZone=UTC") as connection:
        connection.isolation_level = IsolationLevel.REPEATABLE_READ
        with connection.cursor() as cursor:
            cursor.execute(
                "INSERT INTO cdc_watermark (table_name) VALUES (%s) ON CONFLICT DO NOTHING",
                (table_name,),
            )
            cursor.execute(
                "SELECT last_updated_at FROM cdc_watermark WHERE table_name = %s FOR UPDATE",
                (table_name,),
            )
            last_updated_at = cursor.fetchone()[0]
            cursor.execute(sql.SQL("SELECT max(updated_at) FROM {}").format(sql.Identifier(table_name)))
            max_updated_at = cursor.fetchone()[0]
            if last_updated_at and max_updated_at and max_updated_at < last_updated_at:
                raise ValueError(f"{table_name}: source updated_at moved behind its watermark")

        files = 0
        rows = 0
        if max_updated_at is not None:
            for kind in ("older", "boundary"):
                path, count, digest = write_parquet(
                    connection, TABLES[table_name], table_name, last_updated_at, max_updated_at, kind, ingested_at
                )
                if path is None:
                    continue
                try:
                    partition = max_updated_at.astimezone(timezone.utc).strftime("%Y-%m-%d")
                    key = f"bronze/postgres/{table_name}/dt={partition}/{kind}-{digest}.parquet"
                    if not object_exists(s3, bucket, key):
                        s3.upload_file(str(path), bucket, key)
                        files += 1
                    rows += count
                finally:
                    path.unlink(missing_ok=True)

        with connection.cursor() as cursor:
            cursor.execute(
                "UPDATE cdc_watermark SET last_updated_at = COALESCE(%s, last_updated_at), last_run_at = %s "
                "WHERE table_name = %s",
                (max_updated_at, ingested_at, table_name),
            )
    print(f"{table_name}: scanned {rows} rows, uploaded {files} file(s), watermark {max_updated_at}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--table", action="append", choices=TABLES, help="Extract only this table; repeat as needed")
    args = parser.parse_args()
    local = dotenv_values(ROOT / ".env")
    bucket = os.environ.get("S3_BUCKET") or local.get("S3_BUCKET")
    if not bucket or bucket == "your-project-bucket":
        parser.error("Set S3_BUCKET in the environment or .env")
    region = os.environ.get("AWS_REGION") or local.get("AWS_REGION") or "ap-southeast-1"
    s3 = boto3.client("s3", region_name=region)
    settings = database_settings()
    for table_name in args.table or TABLES:
        extract_table(settings, s3, bucket, table_name)


if __name__ == "__main__":
    main()
