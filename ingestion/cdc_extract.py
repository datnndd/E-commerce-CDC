"""Poll OLTP tables into manifest-committed Parquet batches in S3 Bronze."""

import argparse
import hashlib
import json
import os
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import psycopg
from dotenv import dotenv_values
from psycopg import IsolationLevel, sql
from sqlalchemy import BigInteger, Boolean, DateTime, Float, Integer, Numeric, String

from sources.oltp.load_olist import FILES, ROOT, connection_settings, database_settings


TABLES = {model.__tablename__: model for model, _ in FILES}
BATCH_ROWS = 10_000
MAX_FILE_ROWS = 500_000
AWS_KEY_NAMES = ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN")


def s3_session(local):
    environment_keys = {name: os.environ.get(name) for name in AWS_KEY_NAMES}
    file_keys = {name: local.get(name) for name in AWS_KEY_NAMES}
    environment_has_keys = any(name in os.environ for name in AWS_KEY_NAMES)
    file_has_keys = any(name in local for name in AWS_KEY_NAMES)
    keys = environment_keys if environment_has_keys else file_keys
    if environment_has_keys or file_has_keys:
        if not keys["AWS_ACCESS_KEY_ID"] or not keys["AWS_SECRET_ACCESS_KEY"]:
            raise ValueError("Set both AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY for S3 access")
        return boto3.Session(
            aws_access_key_id=keys["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=keys["AWS_SECRET_ACCESS_KEY"],
            aws_session_token=keys["AWS_SESSION_TOKEN"],
        )
    profile = os.environ.get("AWS_PROFILE") or local.get("AWS_PROFILE")
    return boto3.Session(profile_name=profile)


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


def write_parquet(connection, model, table_name, lower, max_updated_at, ingested_at, max_file_rows=MAX_FILE_ROWS):
    table = model.__table__
    if lower is None:
        predicate, parameters = sql.SQL("updated_at <= %s"), (max_updated_at,)
    else:
        predicate = sql.SQL("updated_at > %s AND updated_at <= %s")
        parameters = (lower, max_updated_at)

    primary_key = sql.SQL(", ").join(sql.Identifier(column.name) for column in table.primary_key.columns)
    query = sql.SQL("SELECT * FROM {} WHERE {} ORDER BY updated_at, {}").format(
        sql.Identifier(table_name), predicate, primary_key
    )
    chunks = []
    path = None
    with connection.cursor(name=f"cdc_{table_name}") as cursor:
        cursor.execute(query, parameters)
        names = [column.name for column in cursor.description]
        source_fields = [pa.field(name, arrow_type(table.columns[name])) for name in names]
        schema = pa.schema(
            source_fields
            + [pa.field("ingested_at", pa.timestamp("us", tz="UTC")), pa.field("source_table", pa.string())]
        )
        ingested_scalar = pa.scalar(ingested_at, type=pa.timestamp("us", tz="UTC"))
        table_scalar = pa.scalar(table_name, type=pa.string())
        count = 0
        file_rows = 0
        writer = None
        try:
            while rows := cursor.fetchmany(BATCH_ROWS):
                offset = 0
                while offset < len(rows):
                    if writer is None:
                        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as temp:
                            path = Path(temp.name)
                        writer = pq.ParquetWriter(path, schema, compression="snappy")
                        file_rows = 0
                    piece = rows[offset : offset + max_file_rows - file_rows]
                    piece_count = len(piece)
                    columns = list(zip(*piece))
                    arrays = [
                        pa.array(column, type=field.type)
                        for column, field in zip(columns, source_fields)
                    ] + [
                        pa.repeat(ingested_scalar, piece_count),
                        pa.repeat(table_scalar, piece_count),
                    ]
                    writer.write_table(pa.Table.from_arrays(arrays, schema=schema))
                    file_rows += piece_count
                    count += piece_count
                    offset += piece_count
                    if file_rows == max_file_rows:
                        writer.close()
                        writer = None
                        chunks.append((path, file_rows))
                        path = None
            if writer is not None:
                writer.close()
                writer = None
                chunks.append((path, file_rows))
                path = None
        except Exception:
            if writer is not None:
                writer.close()
            if path is not None:
                path.unlink(missing_ok=True)
            for chunk_path, _ in chunks:
                chunk_path.unlink(missing_ok=True)
            raise
    return chunks, count


def utc_text(value):
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def extract_table(settings, s3, bucket, table_name, max_file_rows=MAX_FILE_ROWS):
    started_at = datetime.now(timezone.utc)
    run_id = started_at.strftime("%Y%m%dT%H%M%S%f")
    chunks = []
    files = published_rows = scanned_rows = 0
    try:
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

            lower = last_updated_at - timedelta(minutes=15) if last_updated_at else None
            if max_updated_at is not None:
                chunks, scanned_rows = write_parquet(
                    connection, TABLES[table_name], table_name, lower, max_updated_at, started_at, max_file_rows,
                )
                if not chunks:
                    raise ValueError(f"{table_name}: polling window has no source rows")

        if chunks:
            day = started_at.strftime("%Y-%m-%d")
            prefix = f"bronze/postgres_oltp/{table_name}/load_date={day}/run_id={run_id}/"
            manifest_files = []
            for index, (path, file_count) in enumerate(chunks):
                file_name = f"part-{index:04d}.parquet"
                with path.open("rb") as data_file:
                    sha256 = hashlib.file_digest(data_file, "sha256").hexdigest()
                s3.upload_file(str(path), bucket, prefix + file_name)
                manifest_files.append({
                    "name": file_name,
                    "row_count": file_count,
                    "size_bytes": path.stat().st_size,
                    "sha256": sha256,
                })
            manifest = {
                "run_id": run_id,
                "source_system": "postgres_oltp",
                "dataset": table_name,
                "load_type": "snapshot" if last_updated_at is None else "delta",
                "window": {
                    "lower_boundary_exclusive": utc_text(lower) if lower else None,
                    "upper_boundary_inclusive": utc_text(max_updated_at),
                },
                "status": "committed",
                "row_count": scanned_rows,
                "files": manifest_files,
                "started_at": utc_text(started_at),
                "finished_at": utc_text(datetime.now(timezone.utc)),
            }
            s3.put_object(
                Bucket=bucket, Key=prefix + "_manifest.json",
                Body=json.dumps(manifest).encode("utf-8"), ContentType="application/json",
            )
            files, published_rows = len(manifest_files), scanned_rows
    finally:
        for path, _ in chunks:
            path.unlink(missing_ok=True)

    with psycopg.connect(**connection_settings(settings), options="-c TimeZone=UTC") as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "UPDATE cdc_watermark SET last_updated_at = COALESCE(%s, last_updated_at), "
                "last_run_at = %s "
                "WHERE table_name = %s",
                (max_updated_at, started_at, table_name),
            )
    print(
        f"{table_name}: scanned {scanned_rows} rows, published {published_rows} rows "
        f"in {files} file(s), watermark {max_updated_at}"
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--table", action="append", choices=TABLES, help="Extract only this table; repeat as needed")
    args = parser.parse_args()
    local = dotenv_values(ROOT / ".env")
    bucket = os.environ.get("S3_BUCKET") or local.get("S3_BUCKET")
    if not bucket or bucket == "your-project-bucket":
        parser.error("Set S3_BUCKET in the environment or .env")
    region = os.environ.get("AWS_REGION") or local.get("AWS_REGION") or "ap-southeast-1"
    s3 = s3_session(local).client("s3", region_name=region)
    settings = database_settings()
    for table_name in args.table or TABLES:
        extract_table(settings, s3, bucket, table_name)


if __name__ == "__main__":
    main()
