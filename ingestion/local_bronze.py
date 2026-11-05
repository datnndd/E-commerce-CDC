"""Materialize a complete Bronze input locally without changing PostgreSQL."""

import argparse
import hashlib
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

import psycopg
from psycopg import IsolationLevel, sql

from ingestion.cdc_extract import TABLES, utc_text, write_parquet
from ingestion.ingest_api import exchange_rate_url, ingest_api
from ingestion.ingest_logs import ingest_logs
from sources.oltp.load_olist import connection_settings, database_settings


CAMPAIGN_FIXTURE = Path(__file__).resolve().parents[1] / "sources" / "api" / "mock_campaigns.json"


class LocalObjectStore:
    """The two S3 write calls used by the Bronze ingesters, backed by local files."""

    def __init__(self, root: Path):
        self.root = root.resolve()

    def target(self, key: str) -> Path:
        path = (self.root / key).resolve()
        if not path.is_relative_to(self.root) or not key.startswith("bronze/"):
            raise ValueError(f"Unsafe Bronze key: {key}")
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def upload_file(self, source: str, bucket: str, key: str) -> None:
        with self.target(key).open("xb") as target, Path(source).open("rb") as original:
            shutil.copyfileobj(original, target)

    def put_object(self, *, Bucket: str, Key: str, Body: bytes, ContentType: str) -> None:
        with self.target(Key).open("xb") as target:
            target.write(Body)


def materialize_oltp(store: LocalObjectStore) -> dict[str, int]:
    started = datetime.now(timezone.utc)
    run_id = started.strftime("%Y%m%dT%H%M%S%f")
    counts = {}
    settings = database_settings()
    with psycopg.connect(**connection_settings(settings), options="-c TimeZone=UTC") as connection:
        connection.isolation_level = IsolationLevel.REPEATABLE_READ
        with connection.cursor() as cursor:
            cursor.execute("SET TRANSACTION READ ONLY")
        for table_name, model in TABLES.items():
            with connection.cursor() as cursor:
                cursor.execute(sql.SQL("SELECT max(updated_at) FROM {}").format(sql.Identifier(table_name)))
                upper = cursor.fetchone()[0]
            if upper is None:
                raise ValueError(f"{table_name}: source is empty")
            chunks, count = write_parquet(connection, model, table_name, None, upper, started)
            prefix = f"bronze/postgres_oltp/{table_name}/load_date={started:%Y-%m-%d}/run_id={run_id}/"
            files = []
            try:
                for index, (source, rows) in enumerate(chunks):
                    name = f"part-{index:04d}.parquet"
                    with source.open("rb") as data:
                        digest = hashlib.file_digest(data, "sha256").hexdigest()
                    size = source.stat().st_size
                    store.upload_file(str(source), "local", prefix + name)
                    files.append({"name": name, "row_count": rows, "size_bytes": size, "sha256": digest})
                store.put_object(Bucket="local", Key=prefix + "_manifest.json", ContentType="application/json",
                                 Body=json.dumps({
                                     "run_id": run_id, "source_system": "postgres_oltp", "dataset": table_name,
                                     "load_type": "snapshot", "status": "committed", "row_count": count,
                                     "window": {"lower_boundary_exclusive": None,
                                                "upper_boundary_inclusive": utc_text(upper)},
                                     "files": files, "started_at": utc_text(started),
                                     "finished_at": utc_text(datetime.now(timezone.utc)),
                                 }).encode("utf-8"))
                counts[table_name] = count
            finally:
                for source, _ in chunks:
                    source.unlink(missing_ok=True)
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("logs/local_bronze"))
    parser.add_argument("--clickstream", type=Path, required=True, help="Existing local clickstream JSONL")
    args = parser.parse_args()
    store = LocalObjectStore(args.root)
    counts = materialize_oltp(store)
    clickstream = ingest_logs(args.clickstream, store, "local")
    ingest_api("exchange_rate", exchange_rate_url(start_date="2016-09-01", end_date="2018-10-17"), store, "local")
    ingest_api("campaign", CAMPAIGN_FIXTURE.as_uri(), store, "local")
    print(f"Local Bronze: {counts}; clickstream events={clickstream['row_count']}; API responses=2")


if __name__ == "__main__":
    main()
