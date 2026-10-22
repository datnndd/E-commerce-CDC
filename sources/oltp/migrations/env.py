"""Run OLTP migrations using the local PostgreSQL settings in .env."""

import os
from pathlib import Path

from alembic import context
from dotenv import dotenv_values
from sqlalchemy import create_engine, pool
from sqlalchemy.engine import URL

from sources.oltp.models import Base


REPO_ROOT = Path(__file__).resolve().parents[3]
LOCAL_ENV = dotenv_values(REPO_ROOT / ".env")


def setting(name: str) -> str:
    value = os.environ.get(name) or LOCAL_ENV.get(name)
    if not value or value == "replace-with-a-local-password":
        raise RuntimeError(f"Set {name} in the environment or in .env")
    return value


database_url = URL.create(
    "postgresql+psycopg",
    username=setting("POSTGRES_USER"),
    password=setting("POSTGRES_PASSWORD"),
    host="127.0.0.1",
    port=int(setting("POSTGRES_PORT")),
    database=setting("POSTGRES_DB"),
)
target_metadata = Base.metadata


if context.is_offline_mode():
    context.configure(
        url=database_url.render_as_string(hide_password=False),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()
else:
    engine = create_engine(database_url, poolclass=pool.NullPool)
    with engine.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()
