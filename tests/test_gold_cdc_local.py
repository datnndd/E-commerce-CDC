"""Exercise the dbt Gold delete hook against local Delta SQL."""

import os
from pathlib import Path
import tempfile
import unittest

from jinja2 import Environment

from spark_jobs.silver_transform import local_spark


@unittest.skipUnless(os.environ.get("GOLD_CDC_LOCAL") == "1", "requires local Spark/Delta")
class GoldCdcLocalTest(unittest.TestCase):
    def test_review_delete_hook_removes_stale_fact_and_mart_value(self):
        spark = local_spark()
        spark.sparkContext.setLogLevel("ERROR")
        try:
            with tempfile.TemporaryDirectory() as directory:
                target = f"{directory}/fct_reviews"
                spark.createDataFrame(
                    [(1, 5), (2, 3)], ["review_row_id", "review_score"]
                ).write.format("delta").save(target)
                spark.createDataFrame([(1,)], ["review_row_id"]).createOrReplaceTempView(
                    "stg_reviews_cdc_fixture"
                )
                template = (Path(__file__).parents[1] / "dbt_project" / "macros" /
                            "sync_deleted_keys.sql").read_text(encoding="utf-8")
                macro = Environment().from_string(template).module.sync_deleted_keys
                sql = macro(f"delta.`{target}`", "stg_reviews_cdc_fixture", ["review_row_id"])
                for _ in range(2):
                    spark.sql(sql).collect()
                    rows = spark.read.format("delta").load(target).collect()
                    self.assertEqual([(row.review_row_id, row.review_score) for row in rows], [(1, 5)])
                    self.assertEqual(spark.read.format("delta").load(target)
                                     .selectExpr("avg(review_score) as score").first().score, 5.0)
        finally:
            spark.stop()


if __name__ == "__main__":
    unittest.main()
