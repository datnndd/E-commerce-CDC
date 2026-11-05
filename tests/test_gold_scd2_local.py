"""Check SCD2 fact joins with two observed versions and a pre-snapshot order."""

import os
import unittest
from decimal import Decimal
from pathlib import Path

from spark_jobs.silver_transform import local_spark
from spark_jobs.verify_gold import model_path, render


PROJECT = Path(__file__).resolve().parents[1] / "dbt_project"


@unittest.skipUnless(os.environ.get("SILVER_TEST_DOCKER") == "1", "run with SILVER_TEST_DOCKER=1 in the local Silver Docker image")
class GoldScd2LocalTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = local_spark()
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def test_old_order_keeps_first_version_and_sale_price(self) -> None:
        self.spark.sql("""
            with cases as (
                select 'old' as order_id, timestamp('2018-01-01 12:00:00') as purchased
                union all
                select 'new', timestamp('2026-10-15 12:00:00')
            )
            select order_id, 'c1' as customer_id, 'delivered' as order_status,
                   purchased as order_purchase_timestamp,
                   cast(null as timestamp) as order_approved_at,
                   cast(null as timestamp) as order_delivered_carrier_date,
                   cast(null as timestamp) as order_delivered_customer_date,
                   purchased as order_estimated_delivery_date,
                   1 as item_count, 0 as payment_count,
                   cast(12 as decimal(18,2)) as item_subtotal_brl,
                   cast(0 as decimal(18,2)) as freight_total_brl,
                   cast(0 as decimal(18,2)) as payment_total_brl,
                   timestamp('2026-10-16 00:00:00') as source_ingested_at
            from cases
        """).createOrReplaceTempView("int_orders_enriched")
        self.spark.sql("""
            select 'c1' as customer_id, 'u1' as customer_unique_id,
                   'c-v1' as customer_key, '12345|SP' as geography_key,
                   timestamp('2026-01-01') as dbt_valid_from,
                   timestamp('2026-10-01') as dbt_valid_to, 1 as version_number
            union all
            select 'c1', 'u1', 'c-v2', '54321|SP',
                   timestamp('2026-10-01'), cast(null as timestamp), 2
        """).createOrReplaceTempView("dim_customer")
        self.spark.sql(render(model_path(PROJECT, "fct_orders"))).createOrReplaceTempView("fct_orders")
        customers = {r.order_id: r.customer_key for r in
                     self.spark.table("fct_orders").select("order_id", "customer_key").collect()}
        self.assertEqual(customers, {"old": "c-v1", "new": "c-v2"})
        self.spark.sql("""
            select timestamp('2026-10-15') as source_ingested_at
        """).createOrReplaceTempView("target_orders")
        incremental = self.spark.sql(render(
            model_path(PROJECT, "fct_orders"), incremental=True, target="target_orders"
        ))
        self.assertEqual({r.order_id for r in incremental.select("order_id").collect()}, {"old", "new"})

        self.spark.sql("""
            select 'old' as order_id, 1 as order_item_id, 'p1' as product_id,
                   's1' as seller_id, timestamp('2018-01-02') as shipping_limit_date,
                   cast(12 as decimal(12,2)) as price, cast(0 as decimal(12,2)) as freight_value,
                   timestamp('2026-10-16') as ingested_at
            union all
            select 'new', 1, 'p1', 's1', timestamp('2026-10-16'),
                   cast(18 as decimal(12,2)), cast(0 as decimal(12,2)),
                   timestamp('2026-10-16')
        """).createOrReplaceTempView("stg_order_items")
        self.spark.sql("""
            select 'p1' as product_id, 'p-v1' as product_key, 'cat' as category_key,
                   cast(99 as decimal(12,2)) as current_price,
                   timestamp('2026-01-01') as dbt_valid_from,
                   timestamp('2026-10-01') as dbt_valid_to, 1 as version_number
            union all
            select 'p1', 'p-v2', 'cat', cast(199 as decimal(12,2)),
                   timestamp('2026-10-01'), cast(null as timestamp), 2
        """).createOrReplaceTempView("dim_product")
        self.spark.sql("""
            select 's1' as seller_id, 's1' as seller_key, '12345|SP' as geography_key
        """).createOrReplaceTempView("dim_seller")
        rows = self.spark.sql(render(model_path(PROJECT, "fct_order_items"))).select(
            "order_id", "product_key", "item_price_brl"
        ).collect()
        self.assertEqual(
            {r.order_id: (r.product_key, r.item_price_brl) for r in rows},
            {"old": ("p-v1", Decimal("12.00")), "new": ("p-v2", Decimal("18.00"))},
        )

        self.spark.sql("""
            select '12345' as customer_zip_code_prefix, 'SP' as customer_state
            union all select '54321', 'SP'
        """).createOrReplaceTempView("customers_snapshot")
        self.spark.sql("""
            select '12345' as seller_zip_code_prefix, 'SP' as seller_state
        """).createOrReplaceTempView("stg_sellers")
        self.spark.sql("""
            select '12345' as geolocation_zip_code_prefix, 'SP' as geolocation_state,
                   cast(0 as double) as geolocation_lat, cast(0 as double) as geolocation_lng,
                   'city' as geolocation_city
        """).createOrReplaceTempView("stg_geolocation")
        geography = self.spark.sql(render(model_path(PROJECT, "dim_geography")))
        self.assertEqual(
            {r.geography_key for r in geography.select("geography_key").collect()},
            {"12345|SP", "54321|SP"},
        )


if __name__ == "__main__":
    unittest.main()
