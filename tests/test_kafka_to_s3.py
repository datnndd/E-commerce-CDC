"""Manifest publication and replay safety for Kafka CDC Bronze."""

import io
import json
import unittest
from unittest.mock import patch

from botocore.exceptions import ClientError

from ingestion.kafka_to_s3 import encode_message, publish_batch, published_high_water


class FakeS3:
    def __init__(self):
        self.objects = {}

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey", "Message": "missing"}}, "GetObject")
        return {"Body": io.BytesIO(self.objects[Key])}

    def put_object(self, Bucket, Key, Body, **_):
        self.objects[Key] = Body

    def get_paginator(self, _):
        return self

    def paginate(self, Bucket, Prefix):
        yield {"Contents": [{"Key": key} for key in self.objects if key.startswith(Prefix)]}


class FailManifestOnceS3(FakeS3):
    def __init__(self):
        super().__init__()
        self.fail_manifest = True

    def put_object(self, Bucket, Key, Body, **kwargs):
        if Key.endswith("/_manifest.json") and self.fail_manifest:
            self.fail_manifest = False
            raise RuntimeError("simulated stop before manifest")
        super().put_object(Bucket=Bucket, Key=Key, Body=Body, **kwargs)


class Message:
    def __init__(self, offset, value):
        self._offset, self._value = offset, value

    def topic(self):
        return "ecommerce_cdc.public.order_reviews"

    def partition(self):
        return 0

    def offset(self):
        return self._offset

    def key(self):
        return b'{"review_row_id":1}'

    def value(self):
        return self._value

    def timestamp(self):
        return (1, 1760000000000)


class KafkaBronzeTests(unittest.TestCase):
    def test_committed_batch_is_idempotent_and_gap_is_rejected(self):
        s3 = FakeS3()
        records = [encode_message(Message(0, b'{"op":"r","source":{"lsn":10},"after":{"review_row_id":1}}')),
                   encode_message(Message(1, b'{"op":"d","source":{"lsn":11},"before":{"review_row_id":1}}')),
                   encode_message(Message(2, None))]
        with patch("ingestion.kafka_to_s3.pipeline_s3_prefix", return_value="stage9_local/"):
            self.assertEqual(publish_batch(s3, "bucket", records, "ecommerce_cdc"), 2)
            self.assertEqual(publish_batch(s3, "bucket", records, "ecommerce_cdc"), 2)
            self.assertEqual(published_high_water(s3, "bucket", records[0]["topic"], 0, "ecommerce_cdc"), 2)
            self.assertEqual(len(s3.objects), 2)
            changed = [dict(record) for record in records]
            changed[1]["value"] = {"op": "u"}
            with self.assertRaisesRegex(ValueError, "differs"):
                publish_batch(s3, "bucket", changed, "ecommerce_cdc")
            with self.assertRaisesRegex(ValueError, "contiguous"):
                publish_batch(s3, "bucket", [records[0], records[2]], "ecommerce_cdc")
            s3.objects.pop(next(key for key in s3.objects if key.endswith("events.jsonl")))
            with self.assertRaisesRegex(ValueError, "missing or corrupt"):
                published_high_water(s3, "bucket", records[0]["topic"], 0, "ecommerce_cdc")

    def test_rejects_unexpected_event_before_publication(self):
        with self.assertRaisesRegex(ValueError, "invalid Debezium envelope"):
            encode_message(Message(0, json.dumps({"op": "bad", "source": {}}).encode()))

    def test_restart_after_data_put_publishes_the_same_batch(self):
        s3 = FailManifestOnceS3()
        record = encode_message(Message(0, b'{"op":"r","source":{"lsn":10},"after":{"review_row_id":1}}'))
        with patch("ingestion.kafka_to_s3.pipeline_s3_prefix", return_value="stage9_local/"):
            with self.assertRaisesRegex(RuntimeError, "before manifest"):
                publish_batch(s3, "bucket", [record], "ecommerce_cdc")
            self.assertEqual(len(s3.objects), 1)
            self.assertTrue(next(iter(s3.objects)).endswith("/events.jsonl"))
            self.assertEqual(publish_batch(s3, "bucket", [record], "ecommerce_cdc"), 0)
            self.assertEqual(published_high_water(s3, "bucket", record["topic"], 0, "ecommerce_cdc"), 0)


if __name__ == "__main__":
    unittest.main()
