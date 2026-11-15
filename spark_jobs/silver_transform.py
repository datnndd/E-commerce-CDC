"""Read committed Bronze runs and merge them into Delta Silver (local or AWS Glue)."""

import argparse
from datetime import datetime, timezone
import hashlib
import json
import logging
import os
from pathlib import Path
import re
import sys
from urllib.parse import urlsplit

import boto3
from botocore.exceptions import ClientError
import great_expectations as gx
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window, functions as F
from pyspark.sql.types import StringType, StructType

from spark_jobs.silver_schema import KEYS, OLTP_NAMES, SILVER_NAMES

logger = logging.getLogger(__name__)


def get_spark() -> SparkSession:
    """Return active SparkSession, AWS Glue SparkSession, or configure a local standalone one."""
    active = SparkSession.getActiveSession()
    if active is not None:
        return active

    try:
        from awsglue.context import GlueContext
        from pyspark.context import SparkContext

        sc = SparkContext.getOrCreate()
        glue_context = GlueContext(sc)
        return glue_context.spark_session
    except ImportError:
        pass

    builder = (
        SparkSession.builder.master("local[2]")
        .appName("ecommerce-silver")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.shuffle.partitions", "2")
    )
    from delta import configure_spark_with_delta_pip

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


def load_manifests_from_s3(spark: SparkSession, s3_root: str, dataset: str,
                           source_system: str = "postgres_oltp") -> list[tuple[dict, str]]:
    parsed = urlsplit(s3_root)
    prefix = parsed.path.strip("/")
    subpath = (f"bronze/postgres_debezium/{dataset}" if source_system == "postgres_debezium"
               else bronze_dataset_subpath(dataset))
    dataset_prefix = f"{prefix + '/' if prefix else ''}{subpath}/"
    # Glue 6.0's Spark 4.1 skips underscore-prefixed manifest files during listing.
    s3 = boto3.client("s3", endpoint_url=os.environ.get("S3_ENDPOINT_URL"))
    manifests = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=parsed.netloc, Prefix=dataset_prefix):
        for item in page.get("Contents", []):
            key = item["Key"]
            if not key.endswith("/_manifest.json"):
                continue
            with s3.get_object(Bucket=parsed.netloc, Key=key)["Body"] as body:
                manifest = json.loads(body.read())
            manifests.append((manifest, f"{parsed.scheme}://{parsed.netloc}/{key.rsplit('/', 1)[0]}"))
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


def cdc_checkpoint(silver_root: str, dataset: str) -> int:
    path = f"{silver_root.rstrip('/')}/_cdc_offsets/{dataset}.json"
    if is_s3_uri(path):
        parsed = urlsplit(path)
        try:
            payload = boto3.client("s3", endpoint_url=os.environ.get("S3_ENDPOINT_URL")).get_object(
                Bucket=parsed.netloc, Key=parsed.path.lstrip("/"))["Body"].read()
        except ClientError as exc:
            if exc.response["Error"]["Code"] in {"NoSuchKey", "404"}:
                return -1
            raise
    else:
        local = Path(path)
        if not local.exists():
            return -1
        payload = local.read_bytes()
    state = json.loads(payload)
    if state.get("dataset") != dataset or not isinstance(state.get("offset"), int):
        raise ValueError(f"{path}: invalid CDC checkpoint")
    return state["offset"]


def save_cdc_checkpoint(silver_root: str, dataset: str, offset: int) -> None:
    path = f"{silver_root.rstrip('/')}/_cdc_offsets/{dataset}.json"
    current = cdc_checkpoint(silver_root, dataset)
    if offset < current:
        raise ValueError(f"{dataset}: CDC checkpoint moved backwards")
    payload = json.dumps({"dataset": dataset, "offset": offset}).encode("utf-8")
    if is_s3_uri(path):
        parsed = urlsplit(path)
        boto3.client("s3", endpoint_url=os.environ.get("S3_ENDPOINT_URL")).put_object(
            Bucket=parsed.netloc, Key=parsed.path.lstrip("/"), Body=payload,
            ContentType="application/json")
    else:
        local = Path(path)
        local.parent.mkdir(parents=True, exist_ok=True)
        temporary = local.with_suffix(".tmp")
        temporary.write_bytes(payload)
        temporary.replace(local)


def committed_cdc_frames(spark: SparkSession, root: str, dataset: str,
                         last_offset: int, topic_prefix: str = "ecommerce_cdc") -> tuple[DataFrame | None, int]:
    if is_s3_uri(root):
        entries = load_manifests_from_s3(spark, root, dataset, "postgres_debezium")
    else:
        local_dir = Path(root) / "bronze" / "postgres_debezium" / dataset
        entries = [(json.loads(path.read_text(encoding="utf-8")), str(path.parent))
                   for path in local_dir.rglob("_manifest.json")]
    entries.sort(key=lambda item: item[0].get("offset_start", -1))
    paths = []
    expected = last_offset + 1
    for manifest, directory in entries:
        if manifest.get("status") != "committed":
            continue
        if manifest.get("source_system") != "postgres_debezium" or manifest.get("dataset") != dataset:
            raise ValueError(f"{directory}: invalid CDC source or dataset")
        expected_topic = f"{topic_prefix}.public.{dataset}"
        if manifest.get("topic") != expected_topic or manifest.get("partition") != 0:
            raise ValueError(f"{directory}: CDC topic or partition differs from {dataset}")
        start, end = manifest.get("offset_start"), manifest.get("offset_end")
        if not isinstance(start, int) or not isinstance(end, int) or end < start:
            raise ValueError(f"{directory}: invalid CDC offset range")
        if end <= last_offset:
            continue
        if start != expected:
            raise ValueError(f"{dataset}: missing or overlapping CDC offsets at {expected}")
        expected = end + 1
        files = manifest.get("files", [])
        if len(files) != 1 or files[0].get("name") != "events.jsonl":
            raise ValueError(f"{directory}: invalid CDC file list")
        path = f"{directory}/events.jsonl"
        verify_manifest_file(path, files[0], spark)
        frame = spark.read.json(path)
        count = frame.count()
        if count != manifest.get("row_count") or count != end - start + 1:
            raise ValueError(f"{directory}: CDC row count differs from offsets")
        if frame.filter((F.col("offset") < start) | (F.col("offset") > end) |
                        (F.col("partition") != 0) | (F.col("topic") != manifest["topic"])).count():
            raise ValueError(f"{directory}: CDC records differ from manifest")
        if manifest.get("run_id") != directory.rstrip("/").rsplit("/", 1)[-1]:
            raise ValueError(f"{directory}: CDC run ID differs from path")
        paths.append(path)
    if not paths:
        return None, last_offset
    # Infer one schema across snapshot and streaming files. An all-null `before`
    # in a snapshot file otherwise becomes a string and cannot union with a
    # subsequent update/delete `before` struct.
    result = spark.read.json(paths)
    result = result.withColumn(
        "bronze_run_id",
        F.regexp_extract(F.input_file_name(), r"/(offset=\d{20}-\d{20})/events\.jsonl$", 1),
    )
    if result.filter(F.col("bronze_run_id") == "").limit(1).count():
        raise ValueError(f"{dataset}: CDC data path has no offset run ID")
    for column in ("key", "value"):
        field_type = result.schema[column].dataType if column in result.columns else None
        if isinstance(field_type, StructType) and "payload" in field_type.names:
            payload_schema = field_type["payload"].dataType
            payload_json = F.get_json_object(F.to_json(F.col(column)), "$.payload")
            direct_json = F.to_json(F.col(column))
            result = result.withColumn(column, F.from_json(F.coalesce(payload_json, direct_json), payload_schema))
    return result, expected - 1


def quarantine_rows(dataset: str, frame: DataFrame) -> DataFrame:
    content_columns = sorted(set(frame.columns) - {"bronze_run_id", "ingested_at", "quarantine_reason", "row_hash"})
    return (frame.withColumn("dataset", F.lit(dataset))
            .withColumn("record_json", F.to_json(F.struct(*[F.col(name) for name in content_columns])))
            .withColumn("record_id", F.sha2(F.concat_ws("|", "dataset", "bronze_run_id", "record_json"), 256))
            .select("record_id", "dataset", "quarantine_reason", "record_json", "bronze_run_id", "ingested_at")
            .dropDuplicates(["record_id"]))


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
    bad = quarantine_rows(dataset, frame.filter(F.col("quarantine_reason").isNotNull()))
    good = frame.filter(F.col("quarantine_reason").isNull()).drop("quarantine_reason")
    window = Window.partitionBy(*KEYS[dataset]).orderBy(F.col("updated_at").desc(), F.col("ingested_at").desc(), F.col("row_hash").desc())
    good = good.withColumn("_rank", F.row_number().over(window)).filter("_rank = 1").drop("_rank")
    return good, bad


def quarantine_orphan_clickstream(spark: SparkSession, silver_root: str, frame: DataFrame) -> tuple[DataFrame, DataFrame]:
    root = silver_root.rstrip("/")
    customers = (spark.read.format("delta").load(f"{root}/slv_customers")
                 .select("customer_id").distinct().withColumn("_customer_exists", F.lit(True)))
    orders = (spark.read.format("delta").load(f"{root}/slv_orders")
              .select("order_id", "customer_id").distinct().withColumn("_order_exists", F.lit(True)))
    items = (spark.read.format("delta").load(f"{root}/slv_order_items")
             .select("order_id", "product_id").distinct().withColumn("_item_exists", F.lit(True)))
    checked = (frame.join(customers, ["customer_id"], "left")
               .join(orders, ["order_id", "customer_id"], "left")
               .join(items, ["order_id", "product_id"], "left"))
    reason = (F.when(F.col("_customer_exists").isNull(), "customer not found in Silver")
              .when((F.col("event_type") == "purchase") & F.col("_order_exists").isNull(),
                    "purchase order/customer not found in Silver")
              .when((F.col("event_type") == "purchase") & F.col("_item_exists").isNull(),
                    "purchase order/item product not found in Silver"))
    checked = (checked.withColumn("quarantine_reason", reason)
               .drop("_customer_exists", "_order_exists", "_item_exists"))
    good = checked.filter(F.col("quarantine_reason").isNull()).drop("quarantine_reason")
    bad = quarantine_rows("clickstream", checked.filter(F.col("quarantine_reason").isNotNull()))
    return good, bad


def merge_delta(spark: SparkSession, frame: DataFrame, path: str, keys: list[str], quarantine: bool = False) -> None:
    path_str = str(path).rstrip("/")
    if not is_s3_uri(path_str):
        Path(path_str).parent.mkdir(parents=True, exist_ok=True)
    if not DeltaTable.isDeltaTable(spark, path_str):
        frame.write.format("delta").mode("append").save(path_str)
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
    if dataset == "clickstream":
        good, orphaned = quarantine_orphan_clickstream(spark, root_str, good)
        bad = bad.unionByName(orphaned).dropDuplicates(["record_id"])
    if dataset == "order_payments" and good.filter(F.col("payment_value") < 0).limit(1).count():
        raise ValueError("Quality gate failed: slv_payments has negative payment values")
    if dataset == "clickstream" and good.filter(F.col("event_timestamp") > F.current_timestamp()).limit(1).count():
        raise ValueError("Quality gate failed: slv_clickstream_events has future event timestamps")
    valid_count, bad_count = good.count(), bad.count()
    merge_delta(spark, good, f"{root_str}/{SILVER_NAMES[dataset]}", KEYS[dataset])
    merge_delta(spark, bad, f"{root_str}/silver_quarantine", ["record_id"], quarantine=True)
    return valid_count, bad_count


def merge_cdc_delta(spark: SparkSession, upserts: DataFrame, deletes: DataFrame,
                    path: str, keys: list[str]) -> None:
    path = path.rstrip("/")
    if not DeltaTable.isDeltaTable(spark, path):
        if not upserts.take(1):
            raise ValueError(f"{path}: CDC snapshot must establish the table before deletes")
        upserts.write.format("delta").mode("append").save(path)
    target = DeltaTable.forPath(spark, path)
    condition = " AND ".join(f"target.{key} = source.{key}" for key in keys)
    if upserts.take(1):
        (target.alias("target").merge(upserts.alias("source"), condition)
         .whenMatchedUpdateAll(condition="source.cdc_offset > target.cdc_offset")
         .whenNotMatchedInsertAll().execute())
    if deletes.take(1):
        (target.alias("target").merge(deletes.alias("source"), condition)
         .whenMatchedDelete(condition="source.cdc_offset > target.cdc_offset").execute())


def run_cdc_dataset(spark: SparkSession, bronze_root: str, silver_root: str,
                    dataset: str, topic_prefix: str = "ecommerce_cdc") -> tuple[int, int, int]:
    last = cdc_checkpoint(silver_root, dataset)
    frame, end = committed_cdc_frames(spark, bronze_root, dataset, last, topic_prefix)
    if frame is None:
        return 0, 0, last
    if "value" not in frame.columns or not isinstance(frame.schema["value"].dataType, StructType):
        if "value" not in frame.columns or frame.filter(F.col("value").isNotNull()).count():
            raise ValueError(f"{dataset}: CDC batch has no Debezium envelope")
        return 0, 0, end  # A Debezium tombstone follows its delete envelope.
    invalid_op = F.col("value.op").isNull() | ~F.col("value.op").isin("r", "c", "u", "d")
    if frame.filter(F.col("value").isNotNull() & invalid_op).count():
        raise ValueError(f"{dataset}: invalid Debezium operation")
    if "key" not in frame.columns or not isinstance(frame.schema["key"].dataType, StructType):
        raise ValueError(f"{dataset}: CDC batch has no key")
    keys = KEYS[dataset]
    for key in keys:
        if key not in frame.schema["key"].dataType.names:
            raise ValueError(f"{dataset}: CDC key lacks {key}")
    events = frame.filter(F.col("value.op").isin("r", "c", "u", "d"))
    latest = (events.withColumn("_rank", F.row_number().over(
        Window.partitionBy(*[F.col(f"key.{key}") for key in keys])
        .orderBy(F.col("offset").desc())))
        .filter(F.col("_rank") == 1).drop("_rank"))
    deletes = latest.filter(F.col("value.op") == "d").select(
        *[F.col(f"key.{key}").alias(key) for key in keys], F.col("offset").alias("cdc_offset"))
    upsert_events = latest.filter(F.col("value.op").isin("r", "c", "u"))
    if not upsert_events.take(1):
        good = None
        bad = None
    else:
        if "after" not in frame.schema["value"].dataType.names or not isinstance(
            frame.schema["value"].dataType["after"].dataType, StructType
        ):
            raise ValueError(f"{dataset}: CDC upserts lack after image")
        upserts = upsert_events.select(
            "value.after.*", F.col("offset").alias("cdc_offset"),
            F.col("value.op").alias("cdc_op"), F.col("value.source.lsn").alias("source_lsn"),
            F.col("ingested_at"), F.lit(dataset).alias("source_table"), "bronze_run_id")
        for name in list(upserts.columns):
            if name.endswith(("_at", "_date", "_timestamp")):
                value = F.col(name).cast("string")
                upserts = upserts.withColumn(name, F.when(
                    value.rlike("^[0-9]+$"), F.expr(f"timestamp_micros(cast(`{name}` as bigint))")
                ).otherwise(F.try_to_timestamp(value)))
            elif name in {"current_price", "price", "freight_value", "payment_value"}:
                upserts = upserts.withColumn(name, F.col(name).cast("decimal(12,2)"))
        good, bad = normalize(dataset, upserts)
        if bad.take(1):
            raise ValueError(f"{dataset}: CDC upsert failed Silver validation")
    path = f"{silver_root.rstrip('/')}/{SILVER_NAMES[dataset]}"
    if good is not None:
        merge_cdc_delta(spark, good, deletes, path, keys)
    elif deletes.take(1):
        if not DeltaTable.isDeltaTable(spark, path):
            raise ValueError(f"{dataset}: CDC delete precedes snapshot")
        target = DeltaTable.forPath(spark, path)
        condition = " AND ".join(f"target.{key} = source.{key}" for key in keys)
        (target.alias("target").merge(deletes.alias("source"), condition)
         .whenMatchedDelete(condition="source.cdc_offset > target.cdc_offset").execute())
    if good is not None and bad is not None:
        merge_delta(spark, bad, f"{silver_root.rstrip('/')}/silver_quarantine", ["record_id"], quarantine=True)
    return (good.count() if good is not None else 0), 0, end


def prune_deleted_clickstream_references(spark: SparkSession, silver_root: str) -> int:
    root = silver_root.rstrip("/")
    path = f"{root}/slv_clickstream_events"
    if not DeltaTable.isDeltaTable(spark, path):
        return 0
    events = spark.read.format("delta").load(path)
    customers = spark.read.format("delta").load(f"{root}/slv_customers").select("customer_id")
    orders = spark.read.format("delta").load(f"{root}/slv_orders").select("order_id", "customer_id")
    items = spark.read.format("delta").load(f"{root}/slv_order_items").select("order_id", "product_id")
    missing_customer = events.join(customers, ["customer_id"], "left_anti").select("event_id")
    purchases = events.filter(F.col("event_type") == "purchase")
    missing_order = purchases.join(orders, ["order_id", "customer_id"], "left_anti").select("event_id")
    missing_item = purchases.join(items, ["order_id", "product_id"], "left_anti").select("event_id")
    orphan_ids = missing_customer.union(missing_order).union(missing_item).distinct()
    count = orphan_ids.count()
    if count:
        (DeltaTable.forPath(spark, path).alias("target")
         .merge(orphan_ids.alias("source"), "target.event_id = source.event_id")
         .whenMatchedDelete().execute())
    return count


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


def register_glue_catalog(spark: SparkSession, silver_root: str, database: str) -> None:
    """Expose only quality-gated Delta tables, without repointing existing tables."""
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", database):
        raise ValueError(f"Unsafe Glue database identifier: {database!r}")
    root = silver_root.rstrip("/")
    if not re.fullmatch(r"s3a?://[A-Za-z0-9.-]+(?:/[A-Za-z0-9._/-]+)?", root):
        raise ValueError(f"Unsafe Silver S3 location: {silver_root!r}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS `{database}`")
    for name in [*SILVER_NAMES.values(), "silver_quarantine"]:
        path = f"{root}/{name}"
        relation = f"`{database}`.`{name}`"
        if spark.catalog.tableExists(f"{database}.{name}"):
            actual = spark.sql(f"DESCRIBE DETAIL {relation}").select("location").first().location
            if actual.rstrip("/").replace("s3a://", "s3://") != path.replace("s3a://", "s3://"):
                raise ValueError(f"{relation} already points to {actual}, expected {path}")
        else:
            spark.sql(f"CREATE TABLE {relation} USING DELTA LOCATION '{path}'")


def write_silver_metrics(spark: SparkSession, silver_root: str, bronze_root: str,
                         run_id: str, cdc_bronze_rows: int | None = None) -> None:
    """Write measured post-merge counts for the matching Airflow run."""
    import boto3

    from spark_jobs.ops_key import silver_metrics_key

    parsed = urlsplit(bronze_root)
    if parsed.scheme not in {"s3", "s3a"} or not parsed.netloc:
        raise ValueError("Bronze root must be an S3 URI for Glue metrics")
    counts = {
        name: spark.read.format("delta").load(f"{silver_root.rstrip('/')}/{name}").count()
        for name in SILVER_NAMES.values()
    }
    quarantine = spark.read.format("delta").load(f"{silver_root.rstrip('/')}/silver_quarantine").count()
    key = silver_metrics_key(parsed.path.strip("/"), run_id)
    boto3.client("s3").put_object(
        Bucket=parsed.netloc,
        Key=key,
        Body=json.dumps({"run_id": run_id, "silver_counts": counts,
                         "quarantine_rows": quarantine, "cdc_bronze_rows": cdc_bronze_rows}).encode("utf-8"),
        ContentType="application/json",
    )


def validate_execution_date(date_str: str | None) -> str | None:
    """Validate ISO-8601 date (YYYY-MM-DD) or ISO timestamp."""
    if not date_str:
        return None
    cleaned = str(date_str).strip()
    try:
        iso_clean = cleaned.replace("Z", "+00:00")
        datetime.fromisoformat(iso_clean)
        return cleaned
    except Exception:
        raise ValueError(
            f"Invalid execution-date: {date_str!r}. Must be ISO-8601 format (e.g. YYYY-MM-DD or YYYY-MM-DDTHH:MM:SSZ)."
        )


GLUE_OPTION_SPEC: list[tuple[str, bool, str | None]] = [
    ("JOB_NAME", True, None),
    ("bronze_root", True, None),
    ("silver_root", True, None),
    ("dataset", False, None),
    ("glue_database", True, None),
    ("pipeline_run_id", True, None),
    ("cdc_mode", False, "polling"),
    ("cdc_topic_prefix", False, "ecommerce_cdc"),
]


def is_glue_runtime(argv: list[str]) -> bool:
    """Return True if executing in an AWS Glue runtime environment."""
    has_glue = "awsglue" in sys.modules or "awsglue.utils" in sys.modules
    if not has_glue:
        try:
            import awsglue.utils  # noqa: F401
            has_glue = True
        except ImportError:
            has_glue = False
    if not has_glue:
        return False
    all_args = list(argv) + list(sys.argv)
    return any(a == "--JOB_NAME" or a.startswith("--JOB_NAME=") for a in all_args)


def parse_glue_args(argv: list[str], resolve_fn=None) -> argparse.Namespace:
    """Safely parse AWS Glue arguments without crashing on missing optional options."""
    fn = resolve_fn
    if fn is None:
        if "awsglue.utils" in sys.modules and hasattr(sys.modules["awsglue.utils"], "getResolvedOptions"):
            fn = sys.modules["awsglue.utils"].getResolvedOptions
        else:
            try:
                from awsglue.utils import getResolvedOptions as gro
                fn = gro
            except ImportError:
                pass
    if fn is None:
        raise RuntimeError("AWS Glue getResolvedOptions is not available in this environment")

    raw_argv = list(argv)
    if raw_argv and raw_argv[0].startswith("--"):
        glue_argv = [sys.argv[0] if sys.argv and sys.argv[0] else "job.py"] + raw_argv
    elif not raw_argv:
        glue_argv = [sys.argv[0] if sys.argv and sys.argv[0] else "job.py"]
    else:
        glue_argv = raw_argv

    passed_flags = set()
    for arg in glue_argv:
        if arg.startswith("--"):
            flag = arg[2:].split("=", 1)[0]
            passed_flags.add(flag)

    options_to_resolve: list[str] = []
    defaults: dict[str, str | None] = {}

    for name, required, default_val in GLUE_OPTION_SPEC:
        norm_key = name.lower().replace("-", "_")
        defaults[norm_key] = default_val

        if name in passed_flags or required:
            options_to_resolve.append(name)

    unsupported = passed_flags & {"bronze-root", "silver-root", "glue-database", "pipeline-run-id"}
    if unsupported:
        raise ValueError(f"Glue arguments require underscores: {sorted(unsupported)}")

    raw_resolved = fn(glue_argv, options_to_resolve)

    resolved: dict[str, str | None] = dict(defaults)
    for k, v in raw_resolved.items():
        norm_key = k.lower().replace("-", "_")
        resolved[norm_key] = v

    # Normalize dataset
    dataset_raw = resolved.get("dataset")
    if dataset_raw:
        if isinstance(dataset_raw, str):
            datasets = [d.strip() for d in dataset_raw.split(",") if d.strip()]
        else:
            datasets = list(dataset_raw)
        for d in datasets:
            if d not in SILVER_NAMES:
                raise ValueError(f"Invalid dataset: {d!r}. Must be one of: {', '.join(SILVER_NAMES)}")
        resolved["dataset"] = datasets
    else:
        resolved["dataset"] = None

    if resolved["dataset"]:
        raise ValueError("Glue jobs must process all datasets before catalog registration")

    # Validate database identifier if provided
    glue_db = resolved.get("glue_database")
    if glue_db:
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,254}", glue_db):
            raise ValueError(f"Invalid glue-database identifier: {glue_db!r}")

    if not resolved.get("pipeline_run_id"):
        raise ValueError("Glue jobs require --pipeline_run_id")
    if resolved.get("cdc_mode") not in {"polling", "debezium"}:
        raise ValueError("cdc_mode must be polling or debezium")
    if not re.fullmatch(r"[A-Za-z0-9_]+", resolved.get("cdc_topic_prefix") or ""):
        raise ValueError("cdc_topic_prefix must use letters, digits or underscores")

    # Validate S3 path contracts for non-empty paths in Glue mode
    for path_key in ("bronze_root", "silver_root"):
        val = resolved.get(path_key)
        if val and str(val).strip():
            stripped = str(val).strip()
            parsed = urlsplit(stripped)
            if parsed.scheme not in {"s3", "s3a"} or not parsed.netloc or parsed.query or parsed.fragment:
                raise ValueError(
                    f"Invalid S3 path for {path_key.replace('_', '-')}: {val!r}. "
                    "AWS Glue runtime requires valid S3 URIs (s3://bucket/... or s3a://bucket/...)"
                )

    return argparse.Namespace(**resolved)


def parse_cli_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command line arguments locally or in unit tests using argparse."""
    parser = argparse.ArgumentParser(description="AWS Glue / Local Silver Delta Transformation")
    parser.add_argument("--JOB_NAME", "--job-name", dest="job_name", default=None,
                        help="AWS Glue job name (injected by Glue or optional locally)")
    parser.add_argument("--bronze_root", "--bronze-root", required=True, dest="bronze_root",
                        help="Bronze root (local directory path or s3:// URI)")
    parser.add_argument("--silver_root", "--silver-root", required=True, dest="silver_root",
                        help="Silver Delta root (local directory path or s3:// URI)")
    parser.add_argument("--dataset", action="append", default=None,
                        help="Process one or more datasets (repeat flag or comma-separated); omit for all")
    parser.add_argument("--glue_database", "--glue-database", dest="glue_database", default=None,
                        help="AWS Glue Data Catalog database name for Silver tables")
    parser.add_argument("--pipeline_run_id", "--pipeline-run-id", dest="pipeline_run_id", default=None)
    parser.add_argument("--cdc_mode", "--cdc-mode", choices=("polling", "debezium"), default="polling")
    parser.add_argument("--cdc_topic_prefix", "--cdc-topic-prefix", default="ecommerce_cdc")
    parser.add_argument("--execution-date", "--execution_date", dest="execution_date", default=None,
                        help="Pipeline execution date partition or watermark (YYYY-MM-DD or ISO timestamp)")

    args = parser.parse_args(argv)

    if args.dataset:
        normalized: list[str] = []
        for item in args.dataset:
            for sub in item.split(","):
                sub = sub.strip()
                if sub:
                    if sub not in SILVER_NAMES:
                        parser.error(f"Invalid dataset: {sub!r}. Must be one of: {', '.join(SILVER_NAMES)}")
                    normalized.append(sub)
        args.dataset = normalized

    if args.dataset and args.glue_database:
        parser.error("--dataset cannot be combined with --glue_database; register only after the full quality gate")

    if args.glue_database:
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,254}", args.glue_database):
            parser.error(f"Invalid glue-database identifier: {args.glue_database!r}")
    if not re.fullmatch(r"[A-Za-z0-9_]+", args.cdc_topic_prefix):
        parser.error("cdc-topic-prefix must use letters, digits or underscores")

    if args.execution_date:
        try:
            validate_execution_date(args.execution_date)
        except ValueError as exc:
            parser.error(str(exc))

    return args


def parse_args(argv: list[str] | None = None, force_mode: str | None = None, resolve_fn=None) -> argparse.Namespace:
    """Unified entrypoint parameter resolver supporting both AWS Glue and argparse."""
    effective_argv = sys.argv[1:] if argv is None else list(argv)
    if force_mode == "glue" or (force_mode != "local" and is_glue_runtime(effective_argv)):
        return parse_glue_args(effective_argv, resolve_fn=resolve_fn)
    return parse_cli_args(effective_argv)


parse_arguments = parse_args


def main() -> None:
    args = parse_args()

    spark = get_spark()
    glue_job = None

    if "awsglue" in sys.modules and getattr(args, "job_name", None):
        from awsglue.context import GlueContext
        from awsglue.job import Job
        from pyspark.context import SparkContext

        glue_context = GlueContext(SparkContext.getOrCreate())
        glue_job = Job(glue_context)
        glue_job.init(args.job_name, vars(args))

    is_standalone_local = not is_s3_uri(args.silver_root) and "awsglue" not in sys.modules
    try:
        datasets = args.dataset or list(SILVER_NAMES.keys())
        cdc_progress = {}
        cdc_bronze_rows = 0
        for dataset in datasets:
            if args.cdc_mode == "debezium" and dataset in OLTP_NAMES:
                previous_offset = cdc_checkpoint(args.silver_root, dataset)
                valid, quarantined, end = run_cdc_dataset(
                    spark, args.bronze_root, args.silver_root, dataset, args.cdc_topic_prefix)
                cdc_progress[dataset] = end
                cdc_bronze_rows += end - previous_offset
            else:
                valid, quarantined = run_dataset(spark, args.bronze_root, args.silver_root, dataset)
            print(f"{dataset}: {valid} latest valid rows, {quarantined} quarantined")

        if args.cdc_mode == "debezium" and not args.dataset:
            removed = prune_deleted_clickstream_references(spark, args.silver_root)
            print(f"Removed {removed} clickstream events referencing deleted source rows")

        if not args.dataset:
            verify_silver_quality(spark, args.silver_root)

        if args.glue_database:
            if args.dataset:
                raise ValueError("Cannot register a partial Silver run")
            register_glue_catalog(spark, args.silver_root, args.glue_database)
        if args.pipeline_run_id:
            write_silver_metrics(spark, args.silver_root, args.bronze_root, args.pipeline_run_id,
                                 cdc_bronze_rows if args.cdc_mode == "debezium" else None)

        if args.cdc_mode == "debezium" and not args.dataset:
            for dataset, end in cdc_progress.items():
                if end >= 0:
                    save_cdc_checkpoint(args.silver_root, dataset, end)

        if glue_job is not None:
            glue_job.commit()
    finally:
        if is_standalone_local:
            spark.stop()


if __name__ == "__main__":
    main()
