"""Run OLTP migrations using the PostgreSQL settings in .env or the environment."""

from alembic import context
from sqlalchemy import create_engine, pool
from sqlalchemy.engine import URL

from sources.oltp.models import Base
from sources.oltp.load_olist import database_settings


settings = database_settings()
tls = {"sslmode": settings["POSTGRES_SSLMODE"]}
if "POSTGRES_SSLROOTCERT" in settings:
    tls["sslrootcert"] = settings["POSTGRES_SSLROOTCERT"]
database_url = URL.create(
    "postgresql+psycopg",
    username=settings["POSTGRES_USER"],
    password=settings["POSTGRES_PASSWORD"],
    host=settings["POSTGRES_HOST"],
    port=int(settings["POSTGRES_PORT"]),
    database=settings["POSTGRES_DB"],
    query=tls,
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
