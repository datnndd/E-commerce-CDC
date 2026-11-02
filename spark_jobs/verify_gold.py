"""Execute dbt Gold SELECTs against local Silver Delta and check their grains."""

import argparse
import json
from decimal import Decimal
from pathlib import Path

from jinja2 import Environment, StrictUndefined
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

from spark_jobs.silver_transform import SILVER_NAMES, local_spark


MODELS = (
    "stg_customers", "stg_geolocation", "stg_category_translation",
    "stg_products", "stg_sellers", "stg_orders", "stg_order_items",
    "stg_payments", "stg_reviews", "stg_clickstream_events",
    "stg_exchange_rates", "stg_campaigns",
    "int_orders_enriched", "int_customer_orders",
    "dim_customer", "dim_product", "dim_seller", "dim_category",
    "dim_geography", "dim_channel", "dim_campaign", "dim_date",
    "fct_orders", "fct_order_items", "fct_payments", "fct_reviews",
    "fct_clickstream_events", "fct_sessions", "fct_exchange_rates",
)
TESTS = (
    "assert_core_grains", "assert_core_reconciliation",
    "assert_item_revenue", "assert_nonnegative_items", "assert_scd2_windows",
)
BUSINESS_MODELS = (
    "mart_daily_sales", "mart_product_performance", "mart_customer_rfm",
    "mart_funnel_daily", "mart_payment_mix_daily", "mart_order_experience_daily",
)
BUSINESS_TESTS = (
    "assert_daily_sales_grain", "assert_product_performance_grain",
    "assert_funnel_daily_grain", "assert_payment_mix_grain",
    "assert_order_experience_grain", "assert_sales_totals",
    "assert_product_totals", "assert_rfm_totals", "assert_payment_totals",
    "assert_experience_totals", "assert_funnel_totals",
    "assert_funnel_bounds", "assert_rfm_scores", "assert_rfm_ties",
)


def render(path: Path, *, incremental: bool = False, target: str = "target") -> str:
    template = Environment(undefined=StrictUndefined).from_string(path.read_text(encoding="utf-8"))
    return template.render(
        ref=lambda name: name,
        source=lambda _source, table: table,
        config=lambda **_settings: "",
        is_incremental=lambda: incremental,
        this=target,
    )


def model_path(project: Path, name: str) -> Path:
    matches = list((project / "models").rglob(f"{name}.sql"))
    if len(matches) != 1:
        raise ValueError(f"Expected one dbt model named {name}; found {len(matches)}")
    return matches[0]


def check_count(spark, source: str, fact: str) -> int:
    expected = spark.table(source).count()
    actual = spark.table(fact).count()
    if expected != actual:
        raise AssertionError(f"{fact}: {actual} rows, expected {expected} from {source}")
    print(f"{fact}: {actual} rows = {source}")
    return actual


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--silver-root", type=Path, required=True)
    parser.add_argument("--project-dir", type=Path, default=Path("dbt_project"))
    parser.add_argument("--baseline", type=Path, help="Frozen Stage 1B JSON for monthly item reconciliation")
    parser.add_argument("--business", action="store_true", help="Build and verify the Stage 5 business marts too")
    args = parser.parse_args()
    spark = local_spark()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        for table in SILVER_NAMES.values():
            spark.read.format("delta").load(str(args.silver_root / table)).createOrReplaceTempView(table)
        for name in MODELS[:12]:
            spark.sql(render(model_path(args.project_dir, name))).createOrReplaceTempView(name)
        for entity in ("customers", "products"):
            key = "customer_id" if entity == "customers" else "product_id"
            snapshot = spark.sql(
                f"select *, sha2(concat({key}, '|', cast(updated_at as string)), 256) as dbt_scd_id, "
                f"updated_at as dbt_valid_from, cast(null as timestamp) as dbt_valid_to "
                f"from stg_{entity}"
            )
            snapshot.createOrReplaceTempView(f"{entity}_snapshot")
        for name in MODELS[12:]:
            frame = spark.sql(render(model_path(args.project_dir, name)))
            frame.persist(StorageLevel.MEMORY_AND_DISK)
            frame.count()
            frame.createOrReplaceTempView(name)
        for dimension, key in (
            ("dim_date", "date_key"), ("dim_customer", "customer_key"),
            ("dim_product", "product_key"), ("dim_seller", "seller_key"),
            ("dim_category", "category_key"), ("dim_geography", "geography_key"),
            ("dim_channel", "channel_key"), ("dim_campaign", "campaign_key"),
        ):
            frame = spark.table(dimension)
            if (frame.filter(F.col(key).isNull()).limit(1).count()
                    or frame.groupBy(key).count().filter(F.col("count") > 1).limit(1).count()):
                raise AssertionError(f"{dimension}: {key} must be non-null and unique")
            print(f"{dimension}.{key}: unique and non-null")

        for source, fact in (
            ("stg_orders", "fct_orders"),
            ("stg_order_items", "fct_order_items"),
            ("stg_payments", "fct_payments"),
            ("stg_reviews", "fct_reviews"),
            ("stg_clickstream_events", "fct_clickstream_events"),
            ("stg_exchange_rates", "fct_exchange_rates"),
        ):
            check_count(spark, source, fact)
        sessions = spark.table("fct_sessions").count()
        distinct_sessions = spark.table("stg_clickstream_events").select("session_id").distinct().count()
        if sessions != distinct_sessions:
            raise AssertionError(f"fct_sessions: {sessions} rows, expected {distinct_sessions}")
        revenue = spark.table("fct_order_items").agg(F.sum("item_price_brl")).first()[0]
        print(f"fct_sessions: {sessions}; historical item revenue: BRL {revenue}")
        if args.baseline:
            baseline = json.loads(args.baseline.read_text(encoding="utf-8"))
            expected = {
                row["purchase_month"][:7].replace("-", ""):
                (row["item_count"], Decimal(row["item_price_total"]))
                for row in baseline["metrics"]["item_price_by_purchase_month"]["rows"]
            }
            monthly = spark.table("fct_order_items").groupBy(
                F.substring(F.col("purchase_date_key").cast("string"), 1, 6).alias("month")
            ).agg(F.count("*").alias("items"), F.sum("item_price_brl").alias("revenue"))
            actual = {row.month: (row.items, row.revenue) for row in monthly.collect()}
            if actual != expected:
                raise AssertionError("Gold item counts/revenue by Sao Paulo purchase month differ from Stage 1B")
            print(f"Stage 1B monthly baseline: {len(actual)} months matched")

        for test in TESTS:
            failures = spark.sql(render(args.project_dir / "tests" / f"{test}.sql")).limit(1).count()
            if failures:
                raise AssertionError(f"{test}: returned violating rows")
            print(f"{test}: passed")

        if args.business:
            for name in BUSINESS_MODELS:
                # Materialized dbt tables do not share one unresolved temp-view lineage.
                frame = spark.sql(render(model_path(args.project_dir, name))).localCheckpoint(eager=True)
                count = frame.count()
                frame.createOrReplaceTempView(name)
                print(f"{name}: {count} rows")
            for test in BUSINESS_TESTS:
                print(f"Checking {test}", flush=True)
                failures = spark.sql(render(args.project_dir / "tests" / f"{test}.sql")).limit(1).count()
                if failures:
                    raise AssertionError(f"{test}: returned violating rows")
                print(f"{test}: passed")
            if args.baseline:
                baseline = json.loads(args.baseline.read_text(encoding="utf-8"))
                expected = {
                    row["calendar_month"]: Decimal(row["item_price_total"])
                    for row in baseline["exploration"]["delivered_item_price_by_calendar_month"]["rows"]
                }
                monthly = spark.table("mart_daily_sales").groupBy(
                    F.substring(F.col("purchase_date_key").cast("string"), 5, 2).cast("int").alias("month")
                ).agg(F.sum("delivered_item_revenue_brl").alias("revenue"))
                actual = {row.month: row.revenue for row in monthly.collect()}
                if actual != expected:
                    raise AssertionError("Delivered sales mart differs from Stage 1B source calendar-month baseline")
                print(f"Stage 1B delivered sales baseline: {len(actual)} calendar months matched")
            sales = spark.table("mart_daily_sales")
            sales.unionByName(sales.limit(1)).createOrReplaceTempView("mart_daily_sales")
            if spark.sql(render(args.project_dir / "tests" / "assert_daily_sales_grain.sql")).limit(1).count() == 0:
                raise AssertionError("assert_daily_sales_grain failed to detect a duplicate sales row")
            print("assert_daily_sales_grain: duplicate fixture rejected")

        # Exercise the grain guard against one duplicate without changing Delta files.
        original = spark.table("fct_order_items")
        original.unionByName(original.limit(1)).createOrReplaceTempView("fct_order_items")
        if spark.sql(render(args.project_dir / "tests" / "assert_core_grains.sql")).limit(1).count() == 0:
            raise AssertionError("assert_core_grains failed to detect a duplicate order item")
        print("assert_core_grains: duplicate fixture rejected")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
