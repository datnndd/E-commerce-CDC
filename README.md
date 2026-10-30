# E-commerce CDC Lakehouse

Portfolio project in progress. The target pipeline is PostgreSQL OLTP → S3 Bronze → Databricks Delta Silver/Gold with dbt-databricks → dashboard, orchestrated by Airflow. Docker Compose configures PostgreSQL 18.6 and Airflow 3.3.2 for local development. Olist loading, the isolated OLTP simulator, and PostgreSQL Bronze extraction are implemented; the EC2/RDS → S3 run and downstream pipeline remain to be verified or built. See [PLAN.md](PLAN.md).

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

`scripts/` is reserved for repository bootstrap and validation maintained by the Harness. Project data-source code belongs in `sources/`: `sources/oltp/` holds SQLAlchemy models, Alembic migrations, Olist loading, and the isolated OLTP simulator; `sources/clickstream/` holds the event generator. `ingestion/` contains the PostgreSQL Bronze extractor and will hold the remaining source ingesters. `spark_jobs/`, `dbt_project/` and `airflow/dags/` own later transformation and orchestration stages.

## Stage 1: first OLTP table

The OLTP table definitions are in [`models.py`](sources/oltp/models.py). The initial Alembic revision has been generated and applied locally. From the repository root, with `.env` configured and PostgreSQL running, verify the schema with:

```powershell
uv sync --frozen
uv run alembic -c sources/oltp/alembic.ini current
uv run alembic -c sources/oltp/alembic.ini check
```

For later changes, edit a mapped class in `models.py`, then generate a candidate migration with `uv run alembic -c sources/oltp/alembic.ini revision --autogenerate -m "describe change"`. Inspect and correct every generated migration before `upgrade head`; Alembic cannot infer every rename, data migration or business rule. Keep each reviewed revision in Git. `uv run alembic -c sources/oltp/alembic.ini check` reports whether the models and applied schema differ.

The `customers` model treats `customer_id` as the primary key. `customer_unique_id` identifies a person across orders and is indexed, but is not unique in this table. The ZIP prefix is text so leading zeros survive. Stage 1C models customer deletion as `is_deleted/deleted_at` on the isolated simulator database so polling can observe it; the main snapshot remains unchanged.

The nine Olist CSVs in `olist_dataset/` can be loaded into an **empty** migrated database with:

```powershell
uv run python -m sources.oltp.load_olist --dataset olist_dataset
```

On an EC2 client in the RDS VPC, clone the repository and copy `.env.example` to `.env`. Set `POSTGRES_HOST` to the private RDS endpoint, `POSTGRES_PORT` to the RDS port, and `POSTGRES_USER`, `POSTGRES_PASSWORD`, and `POSTGRES_DB` to the RDS database credentials/name. Set `POSTGRES_SSLMODE=verify-full` and `POSTGRES_SSLROOTCERT` to the absolute path of the RDS CA bundle on EC2. Keep `.env` out of Git. Verify the target database is empty before running Alembic or the loader; the main source must remain an untouched initial snapshot until downstream reconciliation.

For the main RDS initial snapshot, apply only the initial revision before loading. The Stage 1C revision stays on the isolated test database until downstream reconciliation:

```bash
uv sync --frozen
uv run alembic -c sources/oltp/alembic.ini upgrade b5e79439fda1
uv run python -m sources.oltp.load_olist --dataset olist_dataset
```

The loader uses PostgreSQL `COPY` in one transaction, checks source headers and foreign keys, and saves row counts plus any rejected rows under `logs/olist_load/`. A nonempty source table causes it to stop; it does not replace existing data. Olist timestamps have no timezone, so the loader interprets them in `America/Sao_Paulo` and writes UTC instants to PostgreSQL. For orders, `created_at` is purchase time and `updated_at` is the latest recorded order event; for reviews, they are creation and answer times. Other tables use one UTC snapshot timestamp for both fields. The estimated delivery date is a forecast and is not used as an update time.

The Stage 1B [baseline summary](docs/baselines/olist_20261023T032906Z_99dc95d7.md) and [full results](docs/baselines/olist_20261023T032906Z_99dc95d7.json) record CSV SHA-256 hashes, the load report, SQL definitions and results under `snapshot_id = olist_20261023T032906Z_99dc95d7`. Its required checks are table counts, raw order-status counts, all-status item-price totals by local purchase month, and six foreign-key orphan counts. Exploratory category, cancellation, review, product, seller and seasonality queries are kept separately in the same file. Every query ran in one read-only repeatable-read transaction. To capture a different load, pass its exact report path:

```powershell
uv run python -m sources.oltp.capture_baseline --load-report logs/olist_load/<run-id>/report.json --dataset olist_dataset
```

Baseline files are immutable by default: the command refuses to overwrite an existing `snapshot_id`. Keep the main source snapshot unchanged until its Bronze/Silver/Gold outputs have been reconciled against the same definitions.

### Stage 1C: isolated source changes

Stage 1C adds `products.current_price` (initialized from the most recent historical sale) and `customers.is_deleted/deleted_at`. The old `order_items.price` values remain unchanged. Apply the new Alembic revision **only to an isolated copy** of the snapshot for now; the main `ecommerce` database and its 1B baseline remain frozen. The simulator refuses database names other than `*_1c_test`.

For a copy named `ecommerce_1c_test`, set `POSTGRES_DB=ecommerce_1c_test` in the current shell, then run:

```powershell
uv run alembic -c sources/oltp/alembic.ini upgrade head
uv run python -m sources.oltp.simulate_new_day --orders 100 --seed 42
uv run python -m sources.clickstream.generate logs/simulator/<run-id>.json
```

The simulator writes a JSON run report under `logs/simulator/`; the clickstream command writes the corresponding JSONL file. It creates new customers and orders, updates product prices and existing customer addresses, and marks some customers as deleted without removing their historical orders. Each report records the affected counts. Main-source execution remains disabled until initial Bronze/Silver/Gold reconciliation, when its safety gate can be reviewed.

### Stage 2: CDC control table

The main RDS source needs the `cdc_watermark` table from revision `d79b3fd3ea12`. If the database is already there, check it with:

```bash
uv run alembic -c sources/oltp/alembic.ini current
```

Expect at least revision `d79b3fd3ea12`, with `table_name`, `last_updated_at`, and `last_run_at`. The later `6c4d9b7e2a11` boundary-hash migration remains in history but this 15-minute-overlap extractor does not require it. The separate Stage 1C migration remains unapplied on the main source; do not use `upgrade head` there. An isolated database that already includes 1C can use merge revision `94f012a6c7b3`.

### Stage 2: PostgreSQL to S3 Bronze

On EC2, set `S3_BUCKET` in `.env` to the project bucket name. The instance role needs `s3:PutObject` on `bronze/postgres_oltp/*`. The bucket is in `ap-southeast-1`. With `.env` pointing to the RDS source at `d79b3fd3ea12` (or the boundary-hash-only `6c4d9b7e2a11`, without Stage 1C), run from the repository root:

```bash
uv sync --frozen
uv run --frozen python -m ingestion.cdc_extract
```

Use `--table orders` to check one table first. Each poll with source rows in its window writes `bronze/postgres_oltp/<table>/load_date=YYYY-MM-DD/run_id=YYYYMMDDTHHMMSSffffff/part-0000.parquet` (rolling to `part-0001.parquet`, etc. every 500,000 rows), then `_manifest.json` as the final object. `load_date` is the UTC ingestion date; `run_id` is a UTC start timestamp with microseconds so nearby runs have separate paths. The manifest retains the exact source-time window. Silver reads only runs whose manifest has `status: committed`, and only the files listed in that manifest. The manifest records the run ID, source, load type, timestamp window, physical row count, per-file size and SHA-256, and start/finish times. The first run has `load_type: snapshot` and a null lower boundary; later runs are `delta`. Each Parquet row includes `ingested_at` and `source_table`.

For each later poll, the exclusive lower bound is `last_updated_at - 15 minutes` and the inclusive upper bound is the source's current maximum `updated_at`. The first poll takes a full snapshot. This temporary overlap re-reads recent rows even when the source is unchanged; every such poll can produce a new run, so Silver must deduplicate by source key and row version/hash. The source watermark advances only after the committed manifest uploads. A failed attempt can leave an unreferenced Parquet object without a manifest; Silver ignores that run. If the manifest succeeds but the database commit fails, a retry can leave two committed runs with overlapping rows. Polling still misses physical deletes and writes backdated more than 15 minutes below the saved watermark. Orphan cleanup needs a separate retention policy and job; this extractor does not delete S3 objects.

#### Next EC2/RDS check

1. Bring the EC2 checkout to the same revision as the local checkout before running the extractor; a Git bundle procedure is below. Local commits are not available through `git pull` until they are transferred or pushed; keep the existing EC2 `.env` private and intact.
2. Confirm `.env` points to the intended RDS database and project S3 bucket, with `AWS_REGION=ap-southeast-1` and no `AWS_PROFILE` or AWS key values on EC2. Check `uv run --frozen alembic -c sources/oltp/alembic.ini current`; the main RDS source was last reported at `d79b3fd3ea12` and must not include the Stage 1C migration.
3. Run `uv run --frozen python -m ingestion.cdc_extract --table orders` once. In S3, inspect `bronze/postgres_oltp/orders/load_date=<UTC date>/run_id=<run ID>/`: it should contain `part-0000.parquet` and `_manifest.json`. Confirm the manifest has `status: committed`, names that Parquet file, and records its row count and byte size. Download the Parquet file and compare its SHA-256 with the manifest. On the frozen first snapshot, `row_count` should equal `SELECT count(*) FROM orders`; `window.upper_boundary_inclusive` should equal `SELECT max(updated_at) FROM orders`. Check `SELECT table_name, last_updated_at, last_run_at FROM cdc_watermark WHERE table_name = 'orders'` after the upload.
4. Once the single-table result is correct, run `uv run --frozen python -m ingestion.cdc_extract` for all nine OLTP tables. Check the other eight tables' first-snapshot manifest counts against their source counts. The second `orders` poll is a delta run and intentionally replays its 15-minute overlap, so compare its manifest to that window rather than the whole table. Keep the main OLTP snapshot frozen until Bronze/Silver/Gold reconciliation; use the isolated `*_1c_test` database for simulator changes.

The EC2/RDS → S3 run and manifest checks above have not yet been observed. PostgreSQL extraction has passed focused tests against disposable PostgreSQL and fake S3; a previous real-S3 smoke test used an earlier Bronze layout.

To transfer the local commits to the existing EC2 clone without pushing to GitHub, create a bundle from the local repository and upload it with Terminus SFTP. The bundle does not include the ignored `.env`:

```powershell
git bundle create ../ecommerce-stage2.bundle origin/main..HEAD
```

On EC2, after uploading the bundle to `~/ecommerce-stage2.bundle`, run from the repository root:

```bash
git status --short
git bundle verify ~/ecommerce-stage2.bundle
git fetch ~/ecommerce-stage2.bundle HEAD
git merge --ff-only FETCH_HEAD
```

Proceed only if the EC2 checkout has no unexpected tracked changes and the bundle verifies. A failed fast-forward means the checkouts have diverged and need reconciliation before extraction. Do not replace the EC2 `.env`.

For local development, put `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` in the ignored `.env` to let this extractor upload without `aws login`. Add `AWS_SESSION_TOKEN` only for temporary credentials; those expire. A complete key pair takes priority over `AWS_PROFILE`. Alternatively, set `AWS_PROFILE` in `.env` to a temporary-credential profile that boto3 can use; AWS CLI v2 can obtain one with [`aws login`](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-sign-in.html). The AWS CLI does not read this project's `.env`; these keys are for the Python extractor. Do not commit or share `.env`. On EC2, leave all three key variables and `AWS_PROFILE` unset so boto3 uses the instance role. A private RDS endpoint still requires network access from the local machine; S3 credentials alone do not provide it.

## Stage 0 cloud status

AWS Console and CloudShell access in `ap-southeast-1` have been checked. The project S3 bucket has Block Public Access, ACLs disabled, and default SSE-S3 encryption. A small object was uploaded, inspected, and deleted using the AWS CLI in CloudShell. The bucket name and cloud credentials are intentionally kept out of this repository.

Databricks Free Edition CLI login is configured outside the repository. `SELECT 1` succeeded on its serverless SQL warehouse, which was then stopped. The workspace metastore is in `us-east-2`; the project S3 bucket stays in `ap-southeast-1` by choice. A project-scoped IAM role, Unity Catalog storage credential and external location were created. Databricks validation passed `READ`, `LIST`, `WRITE` and `DELETE` against that bucket. This cross-Region path can add transfer cost and latency; measure both before scaling. No pipeline dataset has been loaded yet.
