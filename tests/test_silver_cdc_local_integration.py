"""Run the nine observed local Bronze streams through real Delta MERGE."""

import os
from pathlib import Path
import unittest

from pyspark.sql import functions as F

from spark_jobs.silver_schema import OLTP_NAMES, SILVER_NAMES
from spark_jobs.silver_transform import (
    local_spark, run_cdc_dataset, save_cdc_checkpoint,
)


@unittest.skipUnless(os.environ.get("CDC_SILVER_E2E") == "1", "requires copied local CDC Bronze")
class SilverCdcIntegrationTest(unittest.TestCase):
    def test_snapshot_delete_and_replay_across_nine_tables(self):
        bronze_root = os.environ["CDC_LOCAL_BRONZE_ROOT"]
        silver_root = os.environ["CDC_LOCAL_SILVER_ROOT"]
        topic_prefix = os.environ.get("CDC_TOPIC_PREFIX", "ecommerce_cdc_small")
        self.assertFalse(Path(silver_root).exists(), "use a fresh Silver root for this proof")
        spark = local_spark()
        spark.sparkContext.setLogLevel("ERROR")
        try:
            progress = {}
            for table in OLTP_NAMES:
                valid, bad, end = run_cdc_dataset(spark, bronze_root, silver_root, table, topic_prefix)
                self.assertEqual((valid, bad), (1, 0), table)
                self.assertEqual(end, 4 if table == "order_reviews" else 0, table)
                progress[table] = end
                rows = spark.read.format("delta").load(f"{silver_root}/{SILVER_NAMES[table]}")
                self.assertEqual(rows.count(), 1, table)
                self.assertIn("cdc_offset", rows.columns)
                self.assertEqual(rows.select("cdc_offset").first()[0],
                                 1 if table == "order_reviews" else 0, table)

            reviews = spark.read.format("delta").load(f"{silver_root}/slv_reviews")
            review = reviews.select("review_row_id", "review_score", "cdc_offset").first()
            self.assertEqual((review.review_row_id, review.review_score, review.cdc_offset), (1, 5, 1))
            self.assertEqual(reviews.filter(F.col("review_row_id") == 2).count(), 0)

            # A failed run before checkpoint may replay all manifests. Delta stays key-unique.
            for table in OLTP_NAMES:
                run_cdc_dataset(spark, bronze_root, silver_root, table, topic_prefix)
                rows = spark.read.format("delta").load(f"{silver_root}/{SILVER_NAMES[table]}")
                self.assertEqual(rows.count(), 1, table)

            for table, end in progress.items():
                save_cdc_checkpoint(silver_root, table, end)
                self.assertEqual(run_cdc_dataset(spark, bronze_root, silver_root, table, topic_prefix),
                                 (0, 0, end))
            self.assertEqual(spark.read.format("delta").load(f"{silver_root}/slv_reviews")
                             .filter(F.col("review_row_id") == 2).count(), 0)
        finally:
            spark.stop()
