"""Read committed local Bronze runs and merge them into local Delta Silver."""

import argparse
import hashlib
import json
from pathlib import Path

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window, functions as F
from pyspark.sql.types import StringType

from ingestion.cdc_extract import TABLES


OLTP_NAMES = {
    "customers": "slv_customers",
    "geolocation": "slv_geolocation",
    "product_category_name_translation": "slv_category_translation",
    "products": "slv_products",
    "sellers": "slv_sellers",
    "orders": "slv_orders",
    "order_items": "slv_order_items",
    "order_payments": "slv_payments",
    "order_reviews": "slv_reviews",
}
SILVER_NAMES = {**OLTP_NAMES, "clickstream": "slv_clickstream_events",
                "exchange_rate": "slv_exchange_rates", "campaign": "slv_campaigns"}
KEYS = {name: [column.name for column in TABLES[name].__table__.primary_key.columns]
        for name in OLTP_NAMES}
KEYS.update({"clickstream": ["event_id"], "exchange_rate": ["date", "base", "quote"],
             "campaign": ["campaign_id"]})


def local_spark() -> SparkSession:
    builder = (SparkSession.builder.master("local[2]").appName("ecommerce-silver")
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.ui.showConsoleProgress", "false")
               .config("spark.sql.shuffle.partitions", "2"))
    return configure_spark_with_delta_pip(builder).getOrCreate()


def bronze_directory(root: Path, dataset: str) -> Path:
    if dataset in OLTP_NAMES:
        return root / "bronze" / "postgres_oltp" / dataset
    if dataset == "clickstream":
        return root / "bronze" / "clickstream"
    return root / "bronze" / "api" / dataset


def committed_frames(spark: SparkSession, root: Path, dataset: str) -> DataFrame:
    frames = []
    for manifest_path in sorted(bronze_directory(root, dataset).rglob("_manifest.json")):
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest.get("status") != "committed":
            continue
        expected_source = "postgres_oltp" if dataset in OLTP_NAMES else ("clickstream" if dataset == "clickstream" else "api")
        expected_dataset = "events" if dataset == "clickstream" else dataset
        if manifest.get("source_system") != expected_source or manifest.get("dataset") != expected_dataset:
            raise ValueError(f"{manifest_path}: source or dataset differs from its Bronze path")
        if not manifest.get("files"):
            raise ValueError(f"{manifest_path}: committed run has no files")
        run_frames = []
        for file in manifest["files"]:
            name = file["name"]
            if Path(name).name != name or name in (".", ".."):
                raise ValueError(f"{manifest_path}: unsafe file name {name}")
            path = manifest_path.parent / name
            with path.open("rb") as stream:
                digest = hashlib.file_digest(stream, "sha256").hexdigest()
            if path.stat().st_size != file["size_bytes"] or digest != file["sha256"]:
                raise ValueError(f"{path}: size or SHA-256 differs from manifest")
            if dataset in OLTP_NAMES:
                frame = spark.read.parquet(str(path))
            else:
                frame = spark.read.option("multiLine", dataset != "clickstream").json(str(path))
                if dataset == "campaign":
                    frame = frame.select("synthetic", F.explode("campaigns").alias("campaign")).select("synthetic", "campaign.*")
            actual_count = frame.count()
            if "row_count" in file and actual_count != file["row_count"]:
                raise ValueError(f"{path}: row count differs from manifest")
            run_frames.append(frame)
        run = run_frames[0]
        for frame in run_frames[1:]:
            run = run.unionByName(frame, allowMissingColumns=True)
        if "row_count" in manifest and run.count() != manifest["row_count"]:
            raise ValueError(f"{manifest_path}: run row count differs from manifest")
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


def merge_delta(spark: SparkSession, frame: DataFrame, path: Path, keys: list[str], quarantine: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not DeltaTable.isDeltaTable(spark, str(path)):
        frame.write.format("delta").mode("errorifexists").save(str(path))
        return
    if not frame.take(1):
        return
    target = DeltaTable.forPath(spark, str(path))
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


def run_dataset(spark: SparkSession, bronze_root: Path, silver_root: Path, dataset: str) -> tuple[int, int]:
    good, bad = normalize(dataset, committed_frames(spark, bronze_root, dataset))
    valid_count, bad_count = good.count(), bad.count()
    merge_delta(spark, good, silver_root / SILVER_NAMES[dataset], KEYS[dataset])
    merge_delta(spark, bad, silver_root / "silver_quarantine", ["record_id"], quarantine=True)
    return valid_count, bad_count


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bronze-root", type=Path, required=True, help="Local directory containing bronze/")
    parser.add_argument("--silver-root", type=Path, required=True, help="Local Delta output directory")
    parser.add_argument("--dataset", action="append", choices=SILVER_NAMES, help="Process one dataset; repeat or omit for all")
    args = parser.parse_args()
    spark = local_spark()
    try:
        for dataset in args.dataset or SILVER_NAMES:
            valid, quarantined = run_dataset(spark, args.bronze_root, args.silver_root, dataset)
            print(f"{dataset}: {valid} latest valid rows, {quarantined} quarantined")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
