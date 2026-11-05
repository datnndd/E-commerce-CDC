"""Read committed Bronze runs and merge them into Delta Silver (local or Databricks)."""

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
import re
import sys
from pathlib import Path

import great_expectations as gx
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window, functions as F
from pyspark.sql.types import StringType

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from spark_jobs.silver_schema import KEYS, OLTP_NAMES, SILVER_NAMES


def get_spark() -> SparkSession:
    """Return active Databricks SparkSession, or configure a local standalone one."""
    active = SparkSession.getActiveSession()
    if active is not None:
        return active
    if "DATABRICKS_RUNTIME_VERSION" in os.environ:
        return SparkSession.builder.getOrCreate()
    builder = (SparkSession.builder.master("local[2]").appName("ecommerce-silver")
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.ui.showConsoleProgress", "false")
               .config("spark.sql.shuffle.partitions", "2"))
    return configure_spark_with_delta_pip(builder).getOrCreate()


def local_spark() -> SparkSession:
    return get_spark()


def is_s3_uri(path: str) -> bool:
    return str(path).startswith("s3://") or str(path).startswith("s3a://")


def bronze_dataset_subpath(dataset: str) -> str:
    if dataset in OLTP_NAMES:
        return f"bronze/postgres_oltp/{dataset}"
    if dataset == "clickstream":
        return "bronze/clickstream"
    return f"bronze/api/{dataset}"


def load_manifests_from_s3(spark: SparkSession, s3_root: str, dataset: str) -> list[tuple[dict, str]]:
    path = f"{s3_root.rstrip('/')}/{bronze_dataset_subpath(dataset)}"
    rows = (spark.read.format("binaryFile")
            .option("recursiveFileLookup", "true")
            .option("pathGlobFilter", "_manifest.json")
            .load(path).select("path", "content").collect())
    manifests = []
    for row in rows:
        manifest = json.loads(row.content.decode("utf-8"))
        manifests.append((manifest, row.path.rsplit("/", 1)[0]))
    return sorted(manifests, key=lambda item: item[0].get("started_at", ""))


def verify_manifest_file(path: str, file: dict, spark: SparkSession | None = None) -> None:
    if is_s3_uri(path):
        row = (spark.read.format("binaryFile").load(path)
               .select(F.length("content").alias("size"), F.sha2("content", 256).alias("sha"))
               .first())
        size, digest = row.size, row.sha
    else:
        with Path(path).open("rb") as stream:
            size = Path(path).stat().st_size
            digest = hashlib.file_digest(stream, "sha256").hexdigest()
    if size != file["size_bytes"] or digest != file["sha256"]:
        raise ValueError(f"{path}: size or SHA-256 differs from manifest")


def committed_frames(spark: SparkSession, root: str, dataset: str) -> DataFrame:
    frames = []
    if is_s3_uri(root):
        manifest_entries = load_manifests_from_s3(spark, root, dataset)
    else:
        root_path = Path(root)
        local_dir = root_path / bronze_dataset_subpath(dataset)
        manifest_entries = []
        for mp in sorted(local_dir.rglob("_manifest.json")):
            manifest = json.loads(mp.read_text(encoding="utf-8"))
            manifest_entries.append((manifest, str(mp.parent)))

    for manifest, manifest_dir in manifest_entries:
        if manifest.get("status") != "committed":
            continue
        expected_source = "postgres_oltp" if dataset in OLTP_NAMES else ("clickstream" if dataset == "clickstream" else "api")
        expected_dataset = "events" if dataset == "clickstream" else dataset
        if manifest.get("source_system") != expected_source or manifest.get("dataset") != expected_dataset:
            raise ValueError(f"{manifest_dir}: source or dataset differs from its Bronze path")
        if not manifest.get("files"):
            raise ValueError(f"{manifest_dir}: committed run has no files")

        run_frames = []
        for file in manifest["files"]:
            name = file["name"]
            if Path(name).name != name or name in (".", ".."):
                raise ValueError(f"Unsafe file name in manifest: {name}")
            file_path = f"{manifest_dir}/{name}"
            verify_manifest_file(file_path, file, spark)
            if dataset in OLTP_NAMES:
                frame = spark.read.parquet(file_path)
            else:
                frame = spark.read.option("multiLine", dataset != "clickstream").json(file_path)
                if dataset == "campaign":
                    frame = frame.select("synthetic", F.explode("campaigns").alias("campaign")).select("synthetic", "campaign.*")
            if "row_count" in file and frame.count() != file["row_count"]:
                raise ValueError(f"{file_path}: row count differs from manifest")
            run_frames.append(frame)

        run = run_frames[0]
        for frame in run_frames[1:]:
            run = run.unionByName(frame, allowMissingColumns=True)
        if "row_count" in manifest and run.count() != manifest["row_count"]:
            raise ValueError(f"{manifest_dir}: run row count differs from manifest")
        run = run.withColumn("bronze_run_id", F.lit(manifest["run_id"]))
        if dataset not in OLTP_NAMES:
            run = run.withColumn("ingested_at", F.try_to_timestamp(F.lit(manifest["started_at"])))
        frames.append(run)

    if not frames:
        raise ValueError(f"{dataset}: no committed Bronze manifests under {root}")
    result = frames[0]
    for frame in frames[1:]:
        result = result.unionByName(frame, allowMissingColumns=True)
    return result


def normalize(dataset: str, frame: DataFrame) -> tuple[DataFrame, DataFrame]:
    if dataset == "clickstream":
        frame = frame.withColumn("event_timestamp", F.try_to_timestamp("timestamp")).drop("timestamp")
        frame = frame.withColumn("updated_at", F.col("event_timestamp"))
    elif dataset == "exchange_rate":
        frame = (frame.withColumn("date", F.expr("try_cast(date AS date)"))
                 .withColumn("rate", F.expr("try_cast(rate AS decimal(18,8))"))
                 .withColumn("updated_at", F.col("ingested_at")))
    elif dataset == "campaign":
        frame = (frame.withColumn("discount_pct", F.expr("try_cast(discount_pct AS int)"))
                 .withColumn("updated_at", F.col("ingested_at")))
    else:
        frame = frame.withColumn("updated_at", F.try_to_timestamp("updated_at"))
    frame = frame.withColumn("ingested_at", F.try_to_timestamp("ingested_at"))
    for field in frame.schema.fields:
        if isinstance(field.dataType, StringType):
            frame = frame.withColumn(field.name, F.trim(F.col(field.name)))
    if dataset == "orders":
        frame = frame.withColumn("order_status", F.lower("order_status"))
    if dataset == "exchange_rate":
        frame = frame.withColumn("base", F.upper("base")).withColumn("quote", F.upper("quote"))
    if dataset in ("clickstream", "campaign") and "utm_source" in frame.columns:
        frame = frame.withColumn("utm_source", F.lower("utm_source"))

    required = KEYS[dataset] + ["updated_at", "ingested_at"]
    missing = set(required) - set(frame.columns)
    if missing:
        raise ValueError(f"{dataset}: missing Silver columns {sorted(missing)}")
    reason = F.when(F.lit(False), F.lit(""))
    for key in KEYS[dataset]:
        invalid_key = F.col(key).isNull()
        if isinstance(frame.schema[key].dataType, StringType):
            invalid_key = invalid_key | (F.col(key) == "")
        reason = reason.when(invalid_key, F.lit(f"missing {key}"))
    reason = reason.when(F.col("updated_at").isNull(), F.lit("missing or invalid updated_at"))
    reason = reason.when(F.col("ingested_at").isNull(), F.lit("missing or invalid ingested_at"))
    if "customer_id" in frame.columns:
        reason = reason.when(F.col("customer_id").isNull() | (F.col("customer_id") == ""), F.lit("missing customer_id"))
    if dataset == "order_items":
        reason = reason.when(F.col("price") < 0, F.lit("negative price"))
    if dataset == "clickstream":
        reason = reason.when(F.col("product_id").isNull() | (F.col("product_id") == ""), F.lit("missing product_id"))
        reason = reason.when((F.col("event_type") == "purchase") & F.col("order_id").isNull(), F.lit("purchase missing order_id"))
    if dataset == "exchange_rate":
        reason = reason.when(F.col("rate").isNull(), F.lit("missing or invalid rate"))
    if dataset == "campaign":
        reason = reason.when(F.col("discount_pct").isNull(), F.lit("missing discount_pct"))
    if "quantity" in frame.columns:
        reason = reason.when(F.col("quantity") <= 0, F.lit("nonpositive quantity"))
    if "unit_price" in frame.columns:
        reason = reason.when(F.col("unit_price") < 0, F.lit("negative unit_price"))

    frame = frame.withColumn("quarantine_reason", reason.otherwise(F.lit(None)))
    content_columns = sorted(set(frame.columns) - {"bronze_run_id", "ingested_at", "quarantine_reason", "row_hash"})
    frame = frame.withColumn("row_hash", F.sha2(F.to_json(F.struct(*[F.col(name) for name in content_columns])), 256))
    bad = frame.filter(F.col("quarantine_reason").isNotNull())
    bad = (bad.withColumn("dataset", F.lit(dataset))
           .withColumn("record_json", F.to_json(F.struct(*[F.col(name) for name in content_columns])))
           .withColumn("record_id", F.sha2(F.concat_ws("|", "dataset", "bronze_run_id", "record_json"), 256))
           .select("record_id", "dataset", "quarantine_reason", "record_json", "bronze_run_id", "ingested_at")
           .dropDuplicates(["record_id"]))
    good = frame.filter(F.col("quarantine_reason").isNull()).drop("quarantine_reason")
    window = Window.partitionBy(*KEYS[dataset]).orderBy(F.col("updated_at").desc(), F.col("ingested_at").desc(), F.col("row_hash").desc())
    good = good.withColumn("_rank", F.row_number().over(window)).filter("_rank = 1").drop("_rank")
    return good, bad


def merge_delta(spark: SparkSession, frame: DataFrame, path: str, keys: list[str], quarantine: bool = False) -> None:
    path_str = str(path).rstrip("/")
    if not is_s3_uri(path_str):
        Path(path_str).parent.mkdir(parents=True, exist_ok=True)
    if not DeltaTable.isDeltaTable(spark, path_str):
        frame.write.format("delta").mode("errorifexists").save(path_str)
        return
    if not frame.take(1):
        return
    target = DeltaTable.forPath(spark, path_str)
    condition = " AND ".join(f"target.{key} = source.{key}" for key in keys)
    merge = target.alias("target").merge(frame.alias("source"), condition)
    if quarantine:
        merge.whenNotMatchedInsertAll().execute()
    else:
        newer = ("source.row_hash <> target.row_hash AND (source.updated_at > target.updated_at OR "
                 "(source.updated_at = target.updated_at AND "
                 "(source.ingested_at > target.ingested_at OR "
                 "(source.ingested_at = target.ingested_at AND source.row_hash > target.row_hash))))")
        merge.whenMatchedUpdateAll(condition=newer).whenNotMatchedInsertAll().execute()


def run_dataset(spark: SparkSession, bronze_root: str, silver_root: str, dataset: str) -> tuple[int, int]:
    root_str = str(silver_root).rstrip("/")
    good, bad = normalize(dataset, committed_frames(spark, str(bronze_root), dataset))
    if dataset == "order_payments" and good.filter(F.col("payment_value") < 0).limit(1).count():
        raise ValueError("Quality gate failed: slv_payments has negative payment values")
    if dataset == "clickstream" and good.filter(F.col("event_timestamp") > F.current_timestamp()).limit(1).count():
        raise ValueError("Quality gate failed: slv_clickstream_events has future event timestamps")
    valid_count, bad_count = good.count(), bad.count()
    merge_delta(spark, good, f"{root_str}/{SILVER_NAMES[dataset]}", KEYS[dataset])
    merge_delta(spark, bad, f"{root_str}/silver_quarantine", ["record_id"], quarantine=True)
    return valid_count, bad_count


def check_frame(context, name: str, frame: DataFrame, expectations: list) -> bool:
    source = context.data_sources.add_spark(name=f"source_{name}")
    asset = source.add_dataframe_asset(name=f"asset_{name}")
    batch = asset.add_batch_definition_whole_dataframe("current")
    suite = context.suites.add(gx.ExpectationSuite(name=f"suite_{name}"))
    for expectation in expectations:
        suite.add_expectation(expectation)
    definition = context.validation_definitions.add(
        gx.ValidationDefinition(name=f"validation_{name}", data=batch, suite=suite)
    )
    checkpoint = context.checkpoints.add(
        gx.Checkpoint(name=f"checkpoint_{name}", validation_definitions=[definition])
    )
    return checkpoint.run(batch_parameters={"dataframe": frame}).success


def validate_frames(frames: dict[str, DataFrame]) -> dict[str, bool]:
    context = gx.get_context(mode="ephemeral")
    checks = {
        "slv_orders": [gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_id")],
        "slv_payments": [gx.expectations.ExpectColumnValuesToBeBetween(column="payment_value", min_value=0)],
        "slv_clickstream_events": [gx.expectations.ExpectColumnValuesToBeBetween(
            column="event_timestamp", max_value=datetime.now(timezone.utc))],
    }
    return {name: check_frame(context, name, frames[name], expectations)
            for name, expectations in checks.items()}


def verify_silver_quality(spark: SparkSession, silver_root: str) -> None:
    """Verify primary keys, joins, and the 3 critical Quality Gate rules."""
    root_str = str(silver_root).rstrip("/")
    tables = {}
    for dataset, name in SILVER_NAMES.items():
        table_path = f"{root_str}/{name}"
        if not DeltaTable.isDeltaTable(spark, table_path):
            raise ValueError(f"Missing Delta table: {table_path}")
        frame = spark.read.format("delta").load(table_path)
        tables[dataset] = frame

        duplicate_keys = frame.groupBy(*KEYS[dataset]).count().filter(F.col("count") > 1).count()
        missing_keys = frame.filter(" OR ".join(f"{key} IS NULL" for key in KEYS[dataset])).count()
        if duplicate_keys or missing_keys:
            raise ValueError(f"{name}: {duplicate_keys} duplicate keys, {missing_keys} missing keys")

    purchases = tables["clickstream"].filter(F.col("event_type") == "purchase")
    missing_orders = purchases.join(
        tables["orders"].select("order_id", "customer_id"), ["order_id", "customer_id"], "left_anti"
    ).count()
    missing_products = purchases.join(
        tables["order_items"].select("order_id", "product_id").distinct(),
        ["order_id", "product_id"], "left_anti"
    ).count()
    if missing_orders or missing_products:
        raise ValueError(f"Clickstream purchase joins: {missing_orders} missing orders, {missing_products} missing products")

    results = validate_frames({name: tables[dataset] for dataset, name in SILVER_NAMES.items()})
    if not all(results.values()):
        raise ValueError(f"Silver Great Expectations quality gate failed: {results}")

    quarantine_path = f"{root_str}/silver_quarantine"
    quarantine_count = (spark.read.format("delta").load(quarantine_path).count()
                        if DeltaTable.isDeltaTable(spark, quarantine_path) else 0)
    print(f"All Silver tables verified: unique keys, valid joins, Quality Gate passed. Quarantine rows: {quarantine_count}")


def register_unity_catalog(spark: SparkSession, silver_root: str, catalog: str, schema: str) -> None:
    for value in (catalog, schema):
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", value):
            raise ValueError(f"Unsafe Unity Catalog identifier: {value!r}")
    root_str = str(silver_root).rstrip("/")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
    for name in list(SILVER_NAMES.values()) + ["silver_quarantine"]:
        location = f"{root_str}/{name}/"
        spark.sql(f"CREATE TABLE IF NOT EXISTS `{catalog}`.`{schema}`.`{name}` USING DELTA LOCATION '{location}'")
        spark.sql(f"ALTER TABLE `{catalog}`.`{schema}`.`{name}` SET LOCATION '{location}'")
        print(f"Registered Unity Catalog table `{catalog}`.`{schema}`.`{name}` at {location}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bronze-root", required=True, help="Bronze root (local directory path or s3:// URI)")
    parser.add_argument("--silver-root", required=True, help="Silver Delta root (local directory path or s3:// URI)")
    parser.add_argument("--dataset", action="append", choices=SILVER_NAMES, help="Process one dataset; repeat or omit for all")
    parser.add_argument("--catalog", help="Databricks Unity Catalog to register tables into")
    parser.add_argument("--schema", default="silver", help="Databricks schema for Silver tables")
    args = parser.parse_args()
    if args.dataset and args.catalog:
        parser.error("--dataset cannot be combined with --catalog; register only after the full quality gate")

    spark = get_spark()
    is_standalone_local = not is_s3_uri(args.silver_root) and "DATABRICKS_RUNTIME_VERSION" not in os.environ
    try:
        datasets = args.dataset or SILVER_NAMES
        for dataset in datasets:
            valid, quarantined = run_dataset(spark, args.bronze_root, args.silver_root, dataset)
            print(f"{dataset}: {valid} latest valid rows, {quarantined} quarantined")

        if not args.dataset:
            verify_silver_quality(spark, args.silver_root)

        if args.catalog:
            register_unity_catalog(spark, args.silver_root, args.catalog, args.schema)
    finally:
        if is_standalone_local:
            spark.stop()


if __name__ == "__main__":
    main()
