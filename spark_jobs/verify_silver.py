"""Check local Delta Silver counts, primary keys, and clickstream joins."""

import argparse
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from spark_jobs.silver_transform import KEYS, SILVER_NAMES, local_spark


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--silver-root", type=Path, required=True)
    args = parser.parse_args()
    spark = local_spark()
    try:
        tables = {}
        for dataset, name in SILVER_NAMES.items():
            path = args.silver_root / name
            if not DeltaTable.isDeltaTable(spark, str(path)):
                raise ValueError(f"Missing Delta table: {path}")
            frame = spark.read.format("delta").load(str(path))
            count = frame.count()
            duplicate_keys = frame.groupBy(*KEYS[dataset]).count().filter(F.col("count") > 1).count()
            missing_keys = frame.filter(" OR ".join(f"{key} IS NULL" for key in KEYS[dataset])).count()
            if duplicate_keys or missing_keys:
                raise ValueError(f"{name}: {duplicate_keys} duplicate keys, {missing_keys} missing keys")
            tables[dataset] = frame
            print(f"{name}: {count} rows, unique non-null keys")
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
        quarantine_path = args.silver_root / "silver_quarantine"
        quarantine_count = (spark.read.format("delta").load(str(quarantine_path)).count()
                            if DeltaTable.isDeltaTable(spark, str(quarantine_path)) else 0)
        print(f"Purchase joins: {purchases.count()} matched; quarantine: {quarantine_count}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
