"""Provision nine ordered CDC topics and one PostgreSQL Debezium connector."""

import argparse
import json
import os
from pathlib import Path
from urllib.request import Request, urlopen

import psycopg
from confluent_kafka.admin import AdminClient, NewTopic
from psycopg import sql

from sources.oltp.load_olist import FILES, database_settings, connection_settings


TABLES = tuple(model.__tablename__ for model, _ in FILES)
SECRET_DIR = Path(os.environ.get("CDC_SECRET_DIR", Path(__file__).resolve().parents[1] / "logs" / "cdc-secrets"))


def write_secret() -> Path:
    settings = database_settings()
    SECRET_DIR.mkdir(parents=True, exist_ok=True)
    path = SECRET_DIR / "postgres.properties"
    def escaped(value: str) -> str:
        return (value.replace("\\", "\\\\").replace("\n", "\\n").replace("\r", "\\r")
                .replace("=", "\\=").replace(":", "\\:").replace("#", "\\#").replace("!", "\\!"))
    path.write_text(f"user={escaped(settings['POSTGRES_USER'])}\n"
                    f"password={escaped(settings['POSTGRES_PASSWORD'])}\n", encoding="utf-8")
    path.chmod(0o600)
    return path


def connector_config() -> dict[str, str]:
    settings = database_settings()
    prefix = os.environ.get("CDC_TOPIC_PREFIX", "ecommerce_cdc")
    publication = os.environ.get("CDC_PUBLICATION", "ecommerce_cdc_pub")
    slot = os.environ.get("CDC_SLOT_NAME", "ecommerce_cdc_slot")
    if not all(name.replace("_", "").isalnum() for name in (prefix, publication, slot)):
        raise ValueError("CDC topic prefix, publication and slot must use letters, digits or underscores")
    return {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "tasks.max": "1",
        "plugin.name": "pgoutput",
        "database.hostname": os.environ.get("DEBEZIUM_DATABASE_HOST", settings["POSTGRES_HOST"]),
        "database.port": str(os.environ.get("DEBEZIUM_DATABASE_PORT", settings["POSTGRES_PORT"])),
        "database.user": "${file:/run/secrets/postgres.properties:user}",
        "database.password": "${file:/run/secrets/postgres.properties:password}",
        "database.dbname": settings["POSTGRES_DB"],
        "database.sslmode": os.environ.get("DEBEZIUM_DATABASE_SSLMODE", settings["POSTGRES_SSLMODE"]),
        "topic.prefix": prefix,
        "table.include.list": ",".join(f"public.{table}" for table in TABLES),
        "slot.name": slot,
        "publication.name": publication,
        "publication.autocreate.mode": "disabled",
        "snapshot.mode": "initial",
        "decimal.handling.mode": "string",
        "tombstones.on.delete": "true",
        "topic.creation.default.partitions": "1",
        "topic.creation.default.replication.factor": "1",
    }


def connector_config_with_cert() -> dict[str, str]:
    config = connector_config()
    certificate = os.environ.get("DEBEZIUM_DATABASE_SSLROOTCERT")
    if certificate:
        config["database.sslrootcert"] = certificate
    return config


def ensure_publication() -> None:
    publication = os.environ.get("CDC_PUBLICATION", "ecommerce_cdc_pub")
    if not publication.replace("_", "").isalnum():
        raise ValueError("Unsafe publication name")
    with psycopg.connect(**connection_settings(database_settings()), connect_timeout=10) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1 FROM pg_publication WHERE pubname = %s", (publication,))
            if cursor.fetchone():
                cursor.execute(
                    "SELECT tablename FROM pg_publication_tables WHERE pubname = %s AND schemaname = 'public'",
                    (publication,),
                )
                actual = {row[0] for row in cursor.fetchall()}
                if actual != set(TABLES):
                    raise ValueError(f"Publication {publication} tables differ: {sorted(actual)}")
            else:
                tables = sql.SQL(", ").join(sql.Identifier("public", table) for table in TABLES)
                cursor.execute(sql.SQL("CREATE PUBLICATION {} FOR TABLE {}").format(sql.Identifier(publication), tables))


def ensure_topics() -> None:
    prefix = os.environ.get("CDC_TOPIC_PREFIX", "ecommerce_cdc")
    admin = AdminClient({"bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")})
    names = [f"{prefix}.public.{table}" for table in TABLES]
    existing = admin.list_topics(timeout=15).topics
    futures = admin.create_topics([NewTopic(name, num_partitions=1, replication_factor=1)
                                   for name in names if name not in existing])
    for future in futures.values():
        future.result(timeout=30)
    actual = admin.list_topics(timeout=15).topics
    for name in names:
        if len(actual[name].partitions) != 1:
            raise ValueError(f"{name} must have exactly one partition")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write-secret", action="store_true", help="Write ignored Connect secret file only")
    parser.add_argument("--publication-only", action="store_true", help="Create/check publication only")
    args = parser.parse_args()
    if args.write_secret:
        print(f"Wrote Connect secret file at {write_secret()}")
        return
    ensure_publication()
    if args.publication_only:
        print(f"Verified publication for {len(TABLES)} tables")
        return
    ensure_topics()
    name = os.environ.get("CDC_CONNECTOR_NAME", "ecommerce-postgres-cdc")
    if not name.replace("-", "").replace("_", "").isalnum():
        raise ValueError("Unsafe connector name")
    base = os.environ.get("KAFKA_CONNECT_URL", "http://localhost:8083").rstrip("/")
    request = Request(
        f"{base}/connectors/{name}/config",
        data=json.dumps(connector_config_with_cert()).encode("utf-8"),
        headers={"Content-Type": "application/json"}, method="PUT",
    )
    with urlopen(request, timeout=30) as response:
        if response.status not in (200, 201):
            raise RuntimeError(f"Connector configuration failed: HTTP {response.status}")
    print(f"Configured {name} for {len(TABLES)} single-partition topics")


if __name__ == "__main__":
    main()
