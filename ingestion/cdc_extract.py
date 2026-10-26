"""Poll OLTP tables into manifest-committed Parquet batches in S3 Bronze."""

import argparse
import hashlib
import json
import os
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

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


def write_parquet(connection, model, table_name, last_updated_at, max_updated_at, ingested_at):
    table = model.__table__
    if last_updated_at is None:
        predicate, parameters = sql.SQL("updated_at <= %s"), (max_updated_at,)
    else:
        predicate, parameters = sql.SQL("updated_at >= %s AND updated_at <= %s"), (last_updated_at, max_updated_at)

    primary_key = sql.SQL(", ").join(sql.Identifier(column.name) for column in table.primary_key.columns)
    query = sql.SQL("SELECT * FROM {} WHERE {} ORDER BY updated_at, {}").format(
        sql.Identifier(table_name), predicate, primary_key
    )
    path = None
    with connection.cursor(name=f"cdc_{table_name}") as cursor:
        cursor.execute(query, parameters)
        names = [column.name for column in cursor.description]
        schema = pa.schema(
            [pa.field(name, arrow_type(table.columns[name])) for name in names]
            + [pa.field("ingested_at", pa.timestamp("us", tz="UTC")), pa.field("source_table", pa.string())]
        )
        boundary_hash = hashlib.sha256(repr(names).encode("utf-8"))
        updated_at_index = names.index("updated_at")
        count = 0
        writer = None
        try:
            while rows := cursor.fetchmany(BATCH_ROWS):
                if writer is None:
                    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as temp:
                        path = Path(temp.name)
                    writer = pq.ParquetWriter(path, schema, compression="snappy")
                for row in rows:
                    if row[updated_at_index] == max_updated_at:
                        boundary_hash.update(repr(row).encode("utf-8"))
                        boundary_hash.update(b"\n")
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
    return path, count, boundary_hash.hexdigest()


def utc_text(value):
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def extract_table(settings, s3, bucket, table_name, run_id=None):
    started_at = datetime.now(timezone.utc)
    run_id = run_id or f"pipeline_{started_at.strftime('%Y%m%dT%H%M%S%fZ')}"
    with psycopg.connect(**connection_settings(settings), options="-c TimeZone=UTC") as connection:
        connection.isolation_level = IsolationLevel.REPEATABLE_READ
        with connection.cursor() as cursor:
            cursor.execute(
                "INSERT INTO cdc_watermark (table_name) VALUES (%s) ON CONFLICT DO NOTHING",
                (table_name,),
            )
            cursor.execute(
                "SELECT last_updated_at, last_boundary_hash FROM cdc_watermark WHERE table_name = %s FOR UPDATE",
                (table_name,),
            )
            last_updated_at, last_boundary_hash = cursor.fetchone()
            cursor.execute(sql.SQL("SELECT max(updated_at) FROM {}").format(sql.Identifier(table_name)))
            max_updated_at = cursor.fetchone()[0]
            if last_updated_at and max_updated_at and max_updated_at < last_updated_at:
                raise ValueError(f"{table_name}: source updated_at moved behind its watermark")

        files = published_rows = scanned_rows = 0
        if max_updated_at is not None:
            path, count, boundary_hash = write_parquet(
                connection, TABLES[table_name], table_name, last_updated_at, max_updated_at, started_at
            )
            scanned_rows = count
            if path is None:
                raise ValueError(f"{table_name}: maximum updated_at has no source rows")
            try:
                if max_updated_at != last_updated_at or boundary_hash != last_boundary_hash:
                    batch_id = f"{table_name}_{started_at.strftime('%Y%m%dT%H%M%S%fZ')}_{uuid4().hex[:8]}"
                    day = max_updated_at.astimezone(timezone.utc).strftime("%Y-%m-%d")
                    prefix = f"bronze/postgres_oltp/{table_name}/dt={day}/batch_id={batch_id}/"
                    file_name = "part-0000.parquet"
                    with path.open("rb") as data_file:
                        sha256 = hashlib.file_digest(data_file, "sha256").hexdigest()
                    s3.upload_file(str(path), bucket, prefix + file_name)
                    # PostgreSQL timestamps have microsecond precision; this includes the old boundary group.
                    lower = last_updated_at - timedelta(microseconds=1) if last_updated_at else None
                    manifest = {
                        "run_id": run_id,
                        "batch_id": batch_id,
                        "source_system": "postgres_oltp",
                        "dataset": table_name,
                        "load_type": "snapshot" if last_updated_at is None else "delta",
                        "window": {
                            "lower_boundary_exclusive": utc_text(lower) if lower else None,
                            "upper_boundary_inclusive": utc_text(max_updated_at),
                        },
                        "status": "committed",
                        "row_count": count,
                        "files": [{
                            "name": file_name,
                            "row_count": count,
                            "size_bytes": path.stat().st_size,
                            "sha256": sha256,
                        }],
                        "started_at": utc_text(started_at),
                        "finished_at": utc_text(datetime.now(timezone.utc)),
                    }
                    s3.put_object(
                        Bucket=bucket, Key=prefix + "_manifest.json",
                        Body=json.dumps(manifest).encode("utf-8"), ContentType="application/json",
                    )
                    files, published_rows = 1, count
            finally:
                path.unlink(missing_ok=True)

        with connection.cursor() as cursor:
            cursor.execute(
                "UPDATE cdc_watermark SET last_updated_at = COALESCE(%s, last_updated_at), "
                "last_boundary_hash = COALESCE(%s, last_boundary_hash), last_run_at = %s "
                "WHERE table_name = %s",
                (max_updated_at, boundary_hash if max_updated_at is not None else None, started_at, table_name),
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
    profile = os.environ.get("AWS_PROFILE") or local.get("AWS_PROFILE")
    s3 = boto3.Session(profile_name=profile).client("s3", region_name=region)
    settings = database_settings()
    run_id = f"pipeline_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')}"
    for table_name in args.table or TABLES:
        extract_table(settings, s3, bucket, table_name, run_id)


if __name__ == "__main__":
    main()
