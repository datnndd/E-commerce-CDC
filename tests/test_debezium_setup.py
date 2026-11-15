"""Connector configuration keeps source credentials out of Kafka Connect."""

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from ingestion import debezium_setup


class DebeziumSetupTests(unittest.TestCase):
    def test_connector_has_nine_tables_and_externalized_secret(self):
        settings = {"POSTGRES_HOST": "127.0.0.1", "POSTGRES_PORT": "5434",
                    "POSTGRES_USER": "cdc", "POSTGRES_PASSWORD": "hidden:secret",
                    "POSTGRES_DB": "demo", "POSTGRES_SSLMODE": "prefer"}
        with tempfile.TemporaryDirectory() as directory, \
             patch.object(debezium_setup, "database_settings", return_value=settings), \
             patch.object(debezium_setup, "SECRET_DIR", Path(directory)):
            config = debezium_setup.connector_config()
            self.assertEqual(len(config["table.include.list"].split(",")), 9)
            self.assertEqual(config["database.password"], "${file:/run/secrets/postgres.properties:password}")
            self.assertNotIn(settings["POSTGRES_PASSWORD"], str(config))
            path = debezium_setup.write_secret()
            self.assertEqual(path.read_text(), "user=cdc\npassword=hidden\\:secret\n")


if __name__ == "__main__":
    unittest.main()
