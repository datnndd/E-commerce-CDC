"""The mutation fixture must never target the main source database."""

import unittest
from unittest.mock import patch

from sources.oltp.simulate_cdc_review import main


class ReviewFixtureGuardTests(unittest.TestCase):
    def test_rejects_main_database_before_connection(self):
        with patch("sources.oltp.simulate_cdc_review.database_settings",
                   return_value={"POSTGRES_DB": "ecommerce"}), \
             patch("sources.oltp.simulate_cdc_review.psycopg.connect") as connect:
            with self.assertRaisesRegex(ValueError, "isolated"):
                main()
            connect.assert_not_called()


if __name__ == "__main__":
    unittest.main()
