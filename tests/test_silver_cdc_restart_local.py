"""Apply CDC events emitted after a Kafka/Connect restart to checkpointed Silver."""

import os
import unittest

from pyspark.sql import functions as F

from spark_jobs.silver_transform import cdc_checkpoint, local_spark, run_cdc_dataset, save_cdc_checkpoint


@unittest.skipUnless(os.environ.get("CDC_SILVER_RESTART_E2E") == "1", "requires restarted local CDC stack")
class SilverCdcRestartTest(unittest.TestCase):
    def test_review_update_and_delete_after_restart(self):
        bronze_root = os.environ["CDC_LOCAL_BRONZE_ROOT"]
        silver_root = os.environ["CDC_LOCAL_SILVER_ROOT"]
        prefix = os.environ.get("CDC_TOPIC_PREFIX", "ecommerce_cdc_small")
        self.assertEqual(cdc_checkpoint(silver_root, "order_reviews"), 4)
        spark = local_spark()
        spark.sparkContext.setLogLevel("ERROR")
        try:
            self.assertEqual(run_cdc_dataset(spark, bronze_root, silver_root, "order_reviews", prefix),
                             (1, 0, 8))
            reviews = spark.read.format("delta").load(f"{silver_root}/slv_reviews")
            self.assertEqual(reviews.count(), 1)
            row = reviews.select("review_row_id", "review_score", "cdc_offset").first()
            self.assertEqual((row.review_row_id, row.review_score, row.cdc_offset), (1, 4, 5))
            self.assertEqual(reviews.filter(F.col("review_row_id").isin(2, 3)).count(), 0)
            save_cdc_checkpoint(silver_root, "order_reviews", 8)
            self.assertEqual(run_cdc_dataset(spark, bronze_root, silver_root, "order_reviews", prefix),
                             (0, 0, 8))
        finally:
            spark.stop()


if __name__ == "__main__":
    unittest.main()
