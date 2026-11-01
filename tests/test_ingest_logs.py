"""Focused proof for clickstream Bronze publication."""

import gzip
import hashlib
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock

from ingestion.ingest_logs import ingest_logs


class IngestLogsTest(unittest.TestCase):
    def test_gzip_precedes_committed_manifest(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            source = Path(temp_dir) / "simulator.jsonl"
            raw = b'{"event_id":"e1"}\n\n{"event_id":"e2"}'
            source.write_bytes(raw)
            s3 = Mock()
            uploaded = []
            s3.upload_file.side_effect = lambda path, bucket, key: uploaded.append((key, Path(path).read_bytes()))
            started_at = datetime(2026, 10, 30, 9, 15, tzinfo=timezone.utc)
            manifest = ingest_logs(source, s3, "test-bucket", started_at)

        prefix = "bronze/clickstream/dt=2026-10-30/hour=09/run_id=20261030T091500000000/"
        self.assertEqual(uploaded[0][0], prefix + "events.json.gz")
        self.assertEqual(gzip.decompress(uploaded[0][1]), b'{"event_id":"e1"}\n{"event_id":"e2"}\n')
        self.assertEqual(manifest["row_count"], 2)
        self.assertEqual(manifest["status"], "committed")
        self.assertEqual(manifest["files"][0]["sha256"], hashlib.sha256(uploaded[0][1]).hexdigest())
        self.assertEqual(manifest["files"][0]["size_bytes"], len(uploaded[0][1]))
        self.assertEqual(s3.method_calls[0][0], "upload_file")
        self.assertEqual(s3.method_calls[1][0], "put_object")
        self.assertEqual(s3.put_object.call_args.kwargs["Key"], prefix + "_manifest.json")
        self.assertEqual(json.loads(s3.put_object.call_args.kwargs["Body"]), manifest)

    def test_invalid_source_or_upload_failure_has_no_manifest(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            source = Path(temp_dir) / "invalid.jsonl"
            source.write_text("not json\n", encoding="utf-8")
            s3 = Mock()
            with self.assertRaisesRegex(ValueError, "invalid JSON at line 1"):
                ingest_logs(source, s3, "test-bucket")
            s3.upload_file.assert_not_called()
            s3.put_object.assert_not_called()

            source.write_text('{"event_id":"e1"}\n', encoding="utf-8")
            s3.upload_file.side_effect = RuntimeError("upload failed")
            with self.assertRaisesRegex(RuntimeError, "upload failed"):
                ingest_logs(source, s3, "test-bucket")
            s3.put_object.assert_not_called()
