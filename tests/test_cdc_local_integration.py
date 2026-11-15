"""Observed local PostgreSQL, Kafka and manifest-committed S3 CDC proof."""

import hashlib
import json
import os
import unittest

import boto3
import psycopg
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient

from sources.oltp.load_olist import connection_settings, database_settings
from spark_jobs.silver_schema import OLTP_NAMES


@unittest.skipUnless(os.environ.get("CDC_LOCAL_E2E") == "1", "requires the isolated local CDC stack")
class LocalCdcIntegrationTest(unittest.TestCase):
    def test_nine_snapshots_and_review_mutations_are_committed_in_order(self):
        settings = database_settings()
        self.assertEqual(settings["POSTGRES_DB"], "ecommerce_cdc_small_1c_test")
        bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
        topic_prefix = os.environ.get("CDC_TOPIC_PREFIX", "ecommerce_cdc_small")
        s3 = boto3.client("s3", endpoint_url=os.environ["S3_ENDPOINT_URL"],
                          region_name=os.environ.get("AWS_REGION", "us-east-1"))
        bucket = os.environ["S3_BUCKET"]
        root = os.environ.get("PIPELINE_S3_PREFIX", "stage9_small").strip("/")
        metadata = AdminClient({"bootstrap.servers": bootstrap}).list_topics(timeout=15).topics
        reader = Consumer({"bootstrap.servers": bootstrap, "group.id": "cdc-local-proof",
                           "enable.auto.commit": False})
        try:
            all_events = {}
            for table in OLTP_NAMES:
                topic = f"{topic_prefix}.public.{table}"
                self.assertIn(topic, metadata)
                self.assertEqual(len(metadata[topic].partitions), 1, topic)
                prefix = f"{root}/bronze/postgres_debezium/{table}/topic={topic}/partition=0/"
                keys = [item["Key"] for page in s3.get_paginator("list_objects_v2").paginate(
                    Bucket=bucket, Prefix=prefix) for item in page.get("Contents", [])
                    if item["Key"].endswith("/_manifest.json")]
                self.assertTrue(keys, f"{table}: no committed manifest")
                expected = 0
                events = []
                for key in sorted(keys):
                    manifest = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
                    self.assertEqual(manifest["status"], "committed")
                    self.assertEqual((manifest["topic"], manifest["partition"]), (topic, 0))
                    self.assertEqual(manifest["offset_start"], expected, table)
                    file = manifest["files"][0]
                    data = s3.get_object(Bucket=bucket, Key=key[:-len("_manifest.json")] + file["name"])["Body"].read()
                    self.assertEqual(hashlib.sha256(data).hexdigest(), file["sha256"])
                    self.assertEqual(len(data), file["size_bytes"])
                    batch = [json.loads(line) for line in data.splitlines()]
                    self.assertEqual(len(batch), manifest["row_count"])
                    self.assertEqual([event["offset"] for event in batch],
                                     list(range(expected, manifest["offset_end"] + 1)))
                    self.assertTrue(all(event["topic"] == topic and event["partition"] == 0 for event in batch))
                    events.extend(batch)
                    expected = manifest["offset_end"] + 1
                _, kafka_high = reader.get_watermark_offsets(TopicPartition(topic, 0), timeout=15)
                self.assertEqual(expected, kafka_high, f"{table}: Bronze lagged Kafka")
                first = events[0]["value"]
                self.assertEqual(first.get("payload", first)["op"], "r", table)
                all_events[table] = events
        finally:
            reader.close()

        reviews = all_events["order_reviews"]
        cycles = int(os.environ.get("CDC_EXPECT_REVIEW_CYCLES", "1"))
        self.assertGreater(cycles, 0)
        self.assertEqual([event["offset"] for event in reviews], list(range(1 + 4 * cycles)))
        values = [event["value"].get("payload", event["value"]) if event["value"] else None
                  for event in reviews]
        self.assertEqual(values[0]["op"], "r")
        keys = [event["key"].get("payload", event["key"])["review_row_id"] for event in reviews]
        self.assertEqual(keys[0], 1)
        deleted_ids = []
        for cycle in range(cycles):
            start = 1 + 4 * cycle
            self.assertEqual([value["op"] if value else None for value in values[start:start + 4]],
                             ["u", "c", "d", None])
            inserted_id = keys[start + 1]
            deleted_ids.append(inserted_id)
            self.assertEqual(keys[start:start + 4], [1, inserted_id, inserted_id, inserted_id])
            self.assertEqual(values[start]["after"]["review_score"], 5 if cycle % 2 == 0 else 4)
            self.assertEqual(values[start + 2]["before"]["review_row_id"], inserted_id)
        self.assertTrue(all(value["source"]["lsn"] is not None for value in values if value))

        with psycopg.connect(**connection_settings(settings)) as connection:
            with connection.cursor() as cursor:
                for table in OLTP_NAMES:
                    cursor.execute(f"SELECT count(*) FROM {table}")
                    self.assertEqual(cursor.fetchone()[0], 1, table)
                cursor.execute("SELECT review_score FROM order_reviews WHERE review_row_id = 1")
                self.assertEqual(cursor.fetchone()[0], 5 if cycles % 2 else 4)
                for inserted_id in deleted_ids:
                    cursor.execute("SELECT 1 FROM order_reviews WHERE review_row_id = %s", (inserted_id,))
                    self.assertIsNone(cursor.fetchone())
