"""Spark/GX proof that bad Silver payments stop the Gold gate."""

import os
import tempfile
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from orchestration.silver_quality import validate
from spark_jobs.silver_transform import local_spark


@unittest.skipUnless(os.environ.get("SILVER_TEST_DOCKER"), "run in the Linux Spark Docker image")
class SilverQualityTest(unittest.TestCase):
    def test_checkpoint_accepts_valid_silver_and_rejects_negative_payment(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            spark = local_spark()
            try:
                spark.createDataFrame([("customer-1",)], ["customer_id"]).write.format("delta").save(
                    str(root / "slv_orders")
                )
                spark.createDataFrame([(Decimal("10.00"),)], ["payment_value"]).write.format("delta").save(
                    str(root / "slv_payments")
                )
                spark.createDataFrame([(datetime(2020, 1, 1, tzinfo=timezone.utc),)],
                                      ["event_timestamp"]).write.format("delta").save(
                    str(root / "slv_clickstream_events")
                )
            finally:
                spark.stop()
            self.assertEqual(validate(root), {
                "slv_orders": True, "slv_payments": True, "slv_clickstream_events": True,
            })
            spark = local_spark()
            try:
                spark.createDataFrame([(Decimal("-100.00"),)], ["payment_value"]).write.format("delta").mode(
                    "overwrite").save(str(root / "slv_payments"))
            finally:
                spark.stop()
            self.assertFalse(validate(root)["slv_payments"])


if __name__ == "__main__":
    unittest.main()
