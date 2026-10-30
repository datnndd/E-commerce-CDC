"""Focused proof for API Bronze publication and retry behavior."""

import hashlib
import json
import unittest
from datetime import datetime, timezone
from io import BytesIO
from unittest.mock import Mock, patch
from urllib.error import HTTPError, URLError

from ingestion.ingest_api import fetch_json, ingest_api


class IngestApiTest(unittest.TestCase):
    def test_raw_response_precedes_committed_manifest(self):
        body = b'{"rates":{"USD":0.18},"base":"BRL"}'
        s3 = Mock()
        started_at = datetime(2026, 10, 30, 9, 15, tzinfo=timezone.utc)
        with patch("ingestion.ingest_api.fetch_json", return_value=body):
            manifest = ingest_api("exchange_rate", "http://localhost/rates", s3, "test-bucket", started_at)

        prefix = "bronze/api/exchange_rate/dt=2026-10-30/run_id=20261030T091500000000/"
        first, second = s3.put_object.call_args_list
        self.assertEqual(first.kwargs["Key"], prefix + "response.json")
        self.assertEqual(first.kwargs["Body"], body)
        self.assertEqual(second.kwargs["Key"], prefix + "_manifest.json")
        self.assertEqual(json.loads(second.kwargs["Body"]), manifest)
        self.assertEqual(manifest["status"], "committed")
        self.assertEqual(manifest["files"][0]["size_bytes"], len(body))
        self.assertEqual(manifest["files"][0]["sha256"], hashlib.sha256(body).hexdigest())

    def test_retry_three_times_then_succeed(self):
        response = BytesIO(b'{"ok":true}')
        with patch("ingestion.ingest_api.urlopen", side_effect=[URLError("temporary")] * 3 + [response]) as open_url:
            with patch("ingestion.ingest_api.time.sleep") as sleep:
                self.assertEqual(fetch_json("http://localhost/data"), b'{"ok":true}')
        self.assertEqual(open_url.call_count, 4)
        self.assertEqual([call.args[0] for call in sleep.call_args_list], [1, 2, 4])

    def test_invalid_response_or_permanent_http_error_never_uploads(self):
        s3 = Mock()
        with patch("ingestion.ingest_api.urlopen", side_effect=lambda *args, **kwargs: BytesIO(b"not-json")):
            with patch("ingestion.ingest_api.time.sleep"):
                with self.assertRaisesRegex(RuntimeError, "after 4 attempts"):
                    ingest_api("campaign", "http://localhost/campaign", s3, "test-bucket")
        s3.put_object.assert_not_called()

        error = HTTPError("http://localhost/private", 401, "Unauthorized", {}, None)
        with patch("ingestion.ingest_api.urlopen", side_effect=error) as open_url:
            with self.assertRaisesRegex(RuntimeError, "HTTP 401"):
                fetch_json("http://localhost/private")
        self.assertEqual(open_url.call_count, 1)
