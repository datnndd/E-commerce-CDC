# E-commerce CDC Lakehouse

Portfolio project in progress. The target pipeline is PostgreSQL OLTP → S3 Bronze → Databricks Delta Silver/Gold with dbt-databricks → dashboard, orchestrated by Airflow. The local environment currently runs PostgreSQL 18.6 and Airflow 3.3.2; data loading and pipeline DAGs are planned for later stages in [PLAN.md](PLAN.md).

## Stage 0: local setup

Prerequisites: Docker Desktop with Compose, and Python 3.13 with [uv](https://docs.astral.sh/uv/) for the Python tools. Allow enough Docker memory for Airflow.

On Windows PowerShell:

```powershell
Copy-Item .env.example .env
# Replace POSTGRES_PASSWORD in .env with a private local password.
docker compose up -d
uv sync --frozen
```

On macOS or Linux, use `cp .env.example .env`, set a private password, then run `make up` (or `docker compose up -d`) and `uv sync --frozen`.

PostgreSQL listens only on `localhost:5432` by default. The database, username and host port come from `.env`. Airflow is at [http://localhost:8080](http://localhost:8080), also bound to localhost. Its initial `admin` password is generated inside the container; read it locally with:

```powershell
docker compose exec airflow cat /opt/airflow/simple_auth_manager_passwords.json.generated
```

Local checks:

```powershell
docker compose ps
docker compose exec postgres sh -c 'pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
docker compose exec postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atqc "SHOW wal_level"'
uv run python -c "import faker, pyspark; print('Python tooling OK')"
uv run dbt --version
```

`SHOW wal_level` should return `logical`. On macOS or Linux, `make test` runs the PostgreSQL checks. Stop the local services with `docker compose down`; named volumes retain the local data.

The PostgreSQL 18 image uses a new named volume, `postgres18_data`. The earlier PostgreSQL 15 volume is retained for recovery; it is not mounted into PostgreSQL 18.

The Compose stack is for local development. Keep `.env`, cloud credentials, and generated Airflow passwords out of Git.

## Project code layout

`scripts/` is reserved for repository bootstrap and validation maintained by the Harness. Project data-source code belongs in `sources/`: `sources/oltp/` holds SQLAlchemy models and Alembic migrations, with Olist loading and OLTP simulation to follow; `sources/clickstream/` will hold the event generator. `ingestion/` receives source changes for Bronze, while `spark_jobs/`, `dbt_project/` and `airflow/dags/` own later transformation and orchestration stages.

## Stage 1: first OLTP table

The OLTP table definitions are in [`models.py`](sources/oltp/models.py). The initial Alembic revision has been generated and applied locally. From the repository root, with `.env` configured and PostgreSQL running, verify the schema with:

```powershell
uv sync --frozen
uv run alembic -c sources/oltp/alembic.ini current
uv run alembic -c sources/oltp/alembic.ini check
```

For later changes, edit a mapped class in `models.py`, then generate a candidate migration with `uv run alembic -c sources/oltp/alembic.ini revision --autogenerate -m "describe change"`. Inspect and correct every generated migration before `upgrade head`; Alembic cannot infer every rename, data migration or business rule. Keep each reviewed revision in Git. `uv run alembic -c sources/oltp/alembic.ini check` reports whether the models and applied schema differ.

The `customers` model treats `customer_id` as the primary key. `customer_unique_id` identifies a person across orders and is indexed, but is not unique in this table. The ZIP prefix is text so leading zeros survive. The deletion policy remains unresolved.

The nine Olist CSVs in `olist_dataset/` can be loaded into an **empty** migrated database with:

```powershell
uv run python -m sources.oltp.load_olist --dataset olist_dataset
```

The loader uses PostgreSQL `COPY` in one transaction, checks source headers and foreign keys, and saves row counts plus any rejected rows under `logs/olist_load/`. A nonempty source table causes it to stop; it does not replace existing data. Olist timestamps have no timezone, so the loader interprets them in `America/Sao_Paulo` and writes UTC instants to PostgreSQL. For orders, `created_at` is purchase time and `updated_at` is the latest recorded order event; for reviews, they are creation and answer times. Other tables use one UTC snapshot timestamp for both fields. The estimated delivery date is a forecast and is not used as an update time.

The Stage 1B [baseline summary](docs/baselines/olist_20261023T032906Z_99dc95d7.md) and [full results](docs/baselines/olist_20261023T032906Z_99dc95d7.json) record CSV SHA-256 hashes, the load report, SQL definitions and results under `snapshot_id = olist_20261023T032906Z_99dc95d7`. Its required checks are table counts, raw order-status counts, all-status item-price totals by local purchase month, and six foreign-key orphan counts. Exploratory category, cancellation, review, product, seller and seasonality queries are kept separately in the same file. Every query ran in one read-only repeatable-read transaction. To capture a different load, pass its exact report path:

```powershell
uv run python -m sources.oltp.capture_baseline --load-report logs/olist_load/<run-id>/report.json --dataset olist_dataset
```

Baseline files are immutable by default: the command refuses to overwrite an existing `snapshot_id`. Keep the main source snapshot unchanged until its Bronze/Silver/Gold outputs have been reconciled against the same definitions.

## Stage 0 cloud status

AWS Console and CloudShell access in `ap-southeast-1` have been checked. The project S3 bucket has Block Public Access, ACLs disabled, and default SSE-S3 encryption. A small object was uploaded, inspected, and deleted using the AWS CLI in CloudShell. The bucket name and cloud credentials are intentionally kept out of this repository.

Databricks Free Edition CLI login is configured outside the repository. `SELECT 1` succeeded on its serverless SQL warehouse, which was then stopped. The workspace metastore is in `us-east-2`; the project S3 bucket stays in `ap-southeast-1` by choice. A project-scoped IAM role, Unity Catalog storage credential and external location were created. Databricks validation passed `READ`, `LIST`, `WRITE` and `DELETE` against that bucket. This cross-Region path can add transfer cost and latency; measure both before scaling. No pipeline dataset has been loaded yet.
