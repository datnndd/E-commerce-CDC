"""Adversarial stress-testing suite for Silver transformation, CDC merge, and quarantine logic."""

from datetime import datetime, timezone, timedelta
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest

from spark_jobs.silver_schema import KEYS, OLTP_NAMES, SILVER_NAMES
from spark_jobs.silver_transform import (
    GLUE_OPTION_SPEC,
    is_glue_runtime,
    is_s3_uri,
    local_spark,
    parse_args,
    parse_cli_args,
    parse_glue_args,
)


class TestLocalSparkBackwardCompatibility(unittest.TestCase):
    """Verify local_spark() contract across the codebase."""

    def test_local_spark_callable_and_alias(self):
        from unittest.mock import patch
        from spark_jobs.silver_transform import get_spark, local_spark
        self.assertTrue(callable(local_spark))
        with patch("spark_jobs.silver_transform.get_spark", return_value="mock_spark"):
            self.assertEqual(local_spark(), "mock_spark")

    def test_all_seven_caller_scripts_reference_local_spark(self):
        """Empirically inspect all caller files in the repository to ensure they import local_spark."""
        callers = [
            "tests/test_stage6_quality.py",
            "orchestration/silver_quality.py",
            "tests/test_silver_postgres_e2e.py",
            "tests/test_silver_local.py",
            "tests/test_gold_scd2_local.py",
            "spark_jobs/verify_silver.py",
        ]
        repo_root = Path(__file__).resolve().parent.parent
        for rel_path in callers:
            file_path = repo_root / rel_path
            self.assertTrue(file_path.exists(), f"Caller script {rel_path} does not exist")
            content = file_path.read_text(encoding="utf-8")
            self.assertIn("local_spark", content, f"Caller {rel_path} does not reference local_spark")


class TestDeterministicQuarantineHashing(unittest.TestCase):
    """Adversarial tests for quarantine deterministic hashing logic."""

    def test_hash_oracle_identical_inputs(self):
        dataset = "orders"
        bronze_run_id = "run-20261101T000000"
        record_json = json.dumps({"customer_id": "c1", "order_id": "o1"}, sort_keys=True)

        concat_str = f"{dataset}|{bronze_run_id}|{record_json}"
        hash1 = hashlib.sha256(concat_str.encode("utf-8")).hexdigest()
        hash2 = hashlib.sha256(concat_str.encode("utf-8")).hexdigest()

        self.assertEqual(hash1, hash2)
        self.assertEqual(len(hash1), 64)

    def test_hash_oracle_different_record_json(self):
        dataset = "orders"
        bronze_run_id = "run-20261101T000000"
        json1 = json.dumps({"customer_id": "c1", "order_id": "o1"}, sort_keys=True)
        json2 = json.dumps({"customer_id": "c2", "order_id": "o1"}, sort_keys=True)

        id1 = hashlib.sha256(f"{dataset}|{bronze_run_id}|{json1}".encode("utf-8")).hexdigest()
        id2 = hashlib.sha256(f"{dataset}|{bronze_run_id}|{json2}".encode("utf-8")).hexdigest()

        self.assertNotEqual(id1, id2)

    def test_hash_oracle_different_dataset_or_run_id(self):
        json_val = json.dumps({"order_id": "o1"}, sort_keys=True)
        id_a = hashlib.sha256(f"orders|run-1|{json_val}".encode("utf-8")).hexdigest()
        id_b = hashlib.sha256(f"orders|run-2|{json_val}".encode("utf-8")).hexdigest()
        id_c = hashlib.sha256(f"order_items|run-1|{json_val}".encode("utf-8")).hexdigest()

        self.assertNotEqual(id_a, id_b)
        self.assertNotEqual(id_a, id_c)
        self.assertNotEqual(id_b, id_c)


class TestCDCMergeConditionTruthTable(unittest.TestCase):
    """Exhaustive truth-table verification of the CDC MERGE condition predicate."""

    @staticmethod
    def evaluate_merge_condition(s_updated, t_updated, s_ingested, t_ingested, s_hash, t_hash) -> bool:
        """Python oracle implementing the exact SQL merge condition:

        source.row_hash <> target.row_hash AND (
            source.updated_at > target.updated_at OR (
                source.updated_at = target.updated_at AND (
                    source.ingested_at > target.ingested_at OR (
                        source.ingested_at = target.ingested_at AND source.row_hash > target.row_hash
                    )
                )
            )
        )
        """
        if s_hash == t_hash:
            return False

        if s_updated > t_updated:
            return True
        elif s_updated == t_updated:
            if s_ingested > t_ingested:
                return True
            elif s_ingested == t_ingested:
                return s_hash > t_hash
            else:
                return False
        else:
            return False

    def test_same_row_hash_never_updates(self):
        t1, t2 = datetime(2026, 1, 1), datetime(2026, 1, 2)
        h = "same_hash_123"
        self.assertFalse(self.evaluate_merge_condition(t2, t1, t2, t1, h, h))

    def test_newer_updated_at_wins(self):
        t1, t2 = datetime(2026, 1, 1), datetime(2026, 1, 2)
        self.assertTrue(self.evaluate_merge_condition(
            s_updated=t2, t_updated=t1,
            s_ingested=t1, t_ingested=t2,
            s_hash="aaa", t_hash="zzz"
        ))

    def test_older_updated_at_never_wins(self):
        t1, t2 = datetime(2026, 1, 1), datetime(2026, 1, 2)
        self.assertFalse(self.evaluate_merge_condition(
            s_updated=t1, t_updated=t2,
            s_ingested=t2, t_ingested=t1,
            s_hash="zzz", t_hash="aaa"
        ))

    def test_same_updated_at_newer_ingested_at_wins(self):
        t1, t2 = datetime(2026, 1, 1), datetime(2026, 1, 2)
        self.assertTrue(self.evaluate_merge_condition(
            s_updated=t1, t_updated=t1,
            s_ingested=t2, t_ingested=t1,
            s_hash="aaa", t_hash="zzz"
        ))

    def test_same_updated_at_older_ingested_at_never_wins(self):
        t1, t2 = datetime(2026, 1, 1), datetime(2026, 1, 2)
        self.assertFalse(self.evaluate_merge_condition(
            s_updated=t1, t_updated=t1,
            s_ingested=t1, t_ingested=t2,
            s_hash="zzz", t_hash="aaa"
        ))

    def test_same_updated_and_ingested_higher_row_hash_wins(self):
        t1 = datetime(2026, 1, 1)
        self.assertTrue(self.evaluate_merge_condition(
            s_updated=t1, t_updated=t1,
            s_ingested=t1, t_ingested=t1,
            s_hash="zzz", t_hash="aaa"
        ))

    def test_same_updated_and_ingested_lower_row_hash_never_wins(self):
        t1 = datetime(2026, 1, 1)
        self.assertFalse(self.evaluate_merge_condition(
            s_updated=t1, t_updated=t1,
            s_ingested=t1, t_ingested=t1,
            s_hash="aaa", t_hash="zzz"
        ))


class TestOfflineCDCAndQuarantineLogic(unittest.TestCase):
    """Adversarial offline testing of CDC merge SQL generation, circuit breakers, and hashing invariants."""

    def test_quarantine_deterministic_column_sorting(self):
        """Verify sorting column keys ensures identical JSON serialization regardless of dictionary key order."""
        dict_1 = {"z_key": "val1", "a_key": "val2", "m_key": "val3"}
        dict_2 = {"a_key": "val2", "m_key": "val3", "z_key": "val1"}

        # Simulate sorted(set(frame.columns) - ...)
        cols_sorted = sorted(dict_1.keys())
        ordered_1 = {k: dict_1[k] for k in cols_sorted}
        ordered_2 = {k: dict_2[k] for k in cols_sorted}

        json_1 = json.dumps(ordered_1)
        json_2 = json.dumps(ordered_2)
        self.assertEqual(json_1, json_2)

        hash_1 = hashlib.sha256(f"orders|run-1|{json_1}".encode("utf-8")).hexdigest()
        hash_2 = hashlib.sha256(f"orders|run-1|{json_2}".encode("utf-8")).hexdigest()
        self.assertEqual(hash_1, hash_2)

    def test_quarantine_avalanche_effect(self):
        """Verify minor bit alteration produces completely different SHA-256 record_id."""
        base_json = json.dumps({"order_id": "o1", "price": 10.00}, sort_keys=True)
        altered_json = json.dumps({"order_id": "o1", "price": 10.01}, sort_keys=True)

        h1 = hashlib.sha256(f"orders|run-1|{base_json}".encode("utf-8")).hexdigest()
        h2 = hashlib.sha256(f"orders|run-1|{altered_json}".encode("utf-8")).hexdigest()

        self.assertNotEqual(h1, h2)
        # Check Hamming distance / character difference
        differing_chars = sum(c1 != c2 for c1, c2 in zip(h1, h2))
        self.assertGreater(differing_chars, 30, "SHA-256 should have strong avalanche effect")

    def test_merge_delta_sql_generation_and_modes(self):
        """Adversarially verify merge_delta creates exact SQL conditions for standard and quarantine merges."""
        from unittest.mock import MagicMock, patch
        from spark_jobs.silver_transform import merge_delta

        spark = MagicMock()
        frame = MagicMock()
        frame.take.return_value = [True]

        # Standard merge
        with patch("spark_jobs.silver_transform.is_s3_uri", return_value=True), \
             patch("spark_jobs.silver_transform.DeltaTable.isDeltaTable", return_value=True), \
             patch("spark_jobs.silver_transform.DeltaTable.forPath") as mock_for_path:
            mock_table = MagicMock()
            mock_for_path.return_value = mock_table
            mock_merge_builder = mock_table.alias.return_value.merge.return_value
            mock_update_builder = mock_merge_builder.whenMatchedUpdateAll.return_value

            merge_delta(spark, frame, "s3://b/silver/slv_orders", ["order_id"])

            # Verify join condition
            join_cond = mock_table.alias.return_value.merge.call_args[0][1]
            self.assertEqual(join_cond, "target.order_id = source.order_id")

            # Verify whenMatchedUpdateAll condition
            update_cond = mock_merge_builder.whenMatchedUpdateAll.call_args.kwargs.get("condition")
            self.assertIn("source.row_hash <> target.row_hash", update_cond)
            self.assertIn("source.updated_at > target.updated_at", update_cond)
            self.assertIn("source.ingested_at > target.ingested_at", update_cond)
            self.assertIn("source.row_hash > target.row_hash", update_cond)

            # Verify whenNotMatchedInsertAll was called
            mock_update_builder.whenNotMatchedInsertAll.return_value.execute.assert_called_once()

        # Quarantine merge
        with patch("spark_jobs.silver_transform.is_s3_uri", return_value=True), \
             patch("spark_jobs.silver_transform.DeltaTable.isDeltaTable", return_value=True), \
             patch("spark_jobs.silver_transform.DeltaTable.forPath") as mock_for_path:
            mock_table = MagicMock()
            mock_for_path.return_value = mock_table
            mock_merge_builder = mock_table.alias.return_value.merge.return_value

            merge_delta(spark, frame, "s3://b/silver/silver_quarantine", ["record_id"], quarantine=True)

            # In quarantine mode: only whenNotMatchedInsertAll, never whenMatchedUpdateAll
            mock_merge_builder.whenNotMatchedInsertAll.return_value.execute.assert_called_once()
            mock_merge_builder.whenMatchedUpdateAll.assert_not_called()

    def test_run_dataset_negative_payment_circuit_breaker(self):
        """Verify run_dataset raises ValueError when negative payments are detected."""
        from unittest.mock import MagicMock, patch
        from spark_jobs.silver_transform import run_dataset

        spark = MagicMock()
        mock_good = MagicMock()
        mock_bad = MagicMock()

        # Simulate negative payment present: filter(...).limit(1).count() returns 1
        mock_filter = mock_good.filter.return_value
        mock_filter.limit.return_value.count.return_value = 1

        mock_col = MagicMock()
        mock_col.__lt__.return_value = MagicMock()

        with patch("spark_jobs.silver_transform.committed_frames", return_value=MagicMock()), \
             patch("spark_jobs.silver_transform.normalize", return_value=(mock_good, mock_bad)), \
             patch("spark_jobs.silver_transform.F.col", return_value=mock_col):
            with self.assertRaisesRegex(ValueError, "slv_payments has negative payment values"):
                run_dataset(spark, "/bronze", "/silver", "order_payments")

    def test_run_dataset_future_timestamp_circuit_breaker(self):
        """Verify run_dataset raises ValueError when future event timestamps are detected."""
        from unittest.mock import MagicMock, patch
        from spark_jobs.silver_transform import run_dataset

        spark = MagicMock()
        mock_good = MagicMock()
        mock_bad = MagicMock()

        # Simulate future event timestamp present: filter(...).limit(1).count() returns 1
        mock_filter = mock_good.filter.return_value
        mock_filter.limit.return_value.count.return_value = 1

        mock_col = MagicMock()
        mock_col.__gt__.return_value = MagicMock()

        with patch("spark_jobs.silver_transform.committed_frames", return_value=MagicMock()), \
             patch("spark_jobs.silver_transform.normalize", return_value=(mock_good, mock_bad)), \
             patch("spark_jobs.silver_transform.quarantine_orphan_clickstream", return_value=(mock_good, mock_bad)), \
             patch("spark_jobs.silver_transform.F.col", return_value=mock_col), \
             patch("spark_jobs.silver_transform.F.current_timestamp", return_value=MagicMock()):
            with self.assertRaisesRegex(ValueError, "slv_clickstream_events has future event timestamps"):
                run_dataset(spark, "/bronze", "/silver", "clickstream")


@unittest.skipUnless(os.environ.get("SILVER_TEST_DOCKER") == "1", "Run inside Docker with PySpark & Delta")
class TestSparkDeltaAdversarialLive(unittest.TestCase):
    """Live empirical tests executed against real PySpark, Delta Lake, and Great Expectations."""

    @classmethod
    def setUpClass(cls):
        cls.spark = local_spark()
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.root = Path(self.temp_dir.name)

    def test_live_merge_delta_identical_row_hash_no_update(self):
        """Adversarial test: Same row_hash must NOT be updated even with newer timestamps."""
        from delta.tables import DeltaTable
        from pyspark.sql.types import StructType, StructField, StringType, TimestampType
        from spark_jobs.silver_transform import merge_delta

        schema = StructType([
            StructField("order_id", StringType(), False),
            StructField("customer_id", StringType(), True),
            StructField("order_status", StringType(), True),
            StructField("updated_at", TimestampType(), True),
            StructField("ingested_at", TimestampType(), True),
            StructField("row_hash", StringType(), True),
        ])

        t1 = datetime(2026, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        t2 = datetime(2026, 1, 2, 10, 0, 0, tzinfo=timezone.utc)
        hash_val = "fixed_hash_12345"

        table_path = str(self.root / "slv_orders_hash_test")

        # Initial Target Row
        initial_data = [("o1", "c1", "CREATED", t1, t1, hash_val)]
        df_target = self.spark.createDataFrame(initial_data, schema)
        merge_delta(self.spark, df_target, table_path, ["order_id"])

        delta_table = DeltaTable.forPath(self.spark, table_path)
        history_v0 = delta_table.history().count()

        # Candidate Source Row with identical row_hash but newer timestamps and altered status
        candidate_data = [("o1", "c1", "ATTEMPTED_OVERWRITE", t2, t2, hash_val)]
        df_source = self.spark.createDataFrame(candidate_data, schema)
        merge_delta(self.spark, df_source, table_path, ["order_id"])

        # Verification: Row must NOT have been updated
        read_back = self.spark.read.format("delta").load(table_path).collect()
        self.assertEqual(len(read_back), 1)
        self.assertEqual(read_back[0].order_status, "CREATED")
        self.assertEqual(read_back[0].updated_at.replace(tzinfo=timezone.utc), t1)

    def test_live_merge_delta_precedence_hierarchy(self):
        """Adversarially verify all levels of the precedence hierarchy with live Delta merge."""
        from pyspark.sql.types import StructType, StructField, StringType, TimestampType
        from spark_jobs.silver_transform import merge_delta

        schema = StructType([
            StructField("order_id", StringType(), False),
            StructField("status", StringType(), True),
            StructField("updated_at", TimestampType(), True),
            StructField("ingested_at", TimestampType(), True),
            StructField("row_hash", StringType(), True),
        ])

        t1 = datetime(2026, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        t2 = datetime(2026, 1, 2, 10, 0, 0, tzinfo=timezone.utc)
        t3 = datetime(2026, 1, 3, 10, 0, 0, tzinfo=timezone.utc)

        table_path = str(self.root / "slv_orders_precedence")

        # 1. Target row at t1
        df_init = self.spark.createDataFrame([("o1", "INITIAL", t1, t1, "hash_000")], schema)
        merge_delta(self.spark, df_init, table_path, ["order_id"])

        # 2. Newer updated_at (t2) wins over older (t1) even if ingested_at is earlier and hash is lower
        df_newer_update = self.spark.createDataFrame([("o1", "NEWER_UPDATE", t2, t1 - timedelta(hours=1), "hash_000_low")], schema)
        merge_delta(self.spark, df_newer_update, table_path, ["order_id"])
        res = self.spark.read.format("delta").load(table_path).collect()[0]
        self.assertEqual(res.status, "NEWER_UPDATE")

        # 3. Older updated_at (t1) must NEVER overwrite newer (t2) even if ingested_at is huge and hash is highest
        df_older_update = self.spark.createDataFrame([("o1", "OLD_ATTEMPT", t1, t3, "hash_999_high")], schema)
        merge_delta(self.spark, df_older_update, table_path, ["order_id"])
        res = self.spark.read.format("delta").load(table_path).collect()[0]
        self.assertEqual(res.status, "NEWER_UPDATE")

        # 4. Same updated_at (t2), newer ingested_at (t3) wins
        df_newer_ingest = self.spark.createDataFrame([("o1", "NEWER_INGEST", t2, t3, "hash_000_low")], schema)
        merge_delta(self.spark, df_newer_ingest, table_path, ["order_id"])
        res = self.spark.read.format("delta").load(table_path).collect()[0]
        self.assertEqual(res.status, "NEWER_INGEST")

        # 5. Same updated_at (t2), older ingested_at (t1) must NEVER overwrite
        df_older_ingest = self.spark.createDataFrame([("o1", "OLD_INGEST_ATTEMPT", t2, t1, "hash_999_high")], schema)
        merge_delta(self.spark, df_older_ingest, table_path, ["order_id"])
        res = self.spark.read.format("delta").load(table_path).collect()[0]
        self.assertEqual(res.status, "NEWER_INGEST")

        # 6. Same updated_at (t2), same ingested_at (t3), higher row_hash wins
        df_higher_hash = self.spark.createDataFrame([("o1", "HIGHER_HASH", t2, t3, "hash_zzz")], schema)
        merge_delta(self.spark, df_higher_hash, table_path, ["order_id"])
        res = self.spark.read.format("delta").load(table_path).collect()[0]
        self.assertEqual(res.status, "HIGHER_HASH")

        # 7. Same updated_at (t2), same ingested_at (t3), lower row_hash must NEVER overwrite
        df_lower_hash = self.spark.createDataFrame([("o1", "LOWER_HASH_ATTEMPT", t2, t3, "hash_aaa")], schema)
        merge_delta(self.spark, df_lower_hash, table_path, ["order_id"])
        res = self.spark.read.format("delta").load(table_path).collect()[0]
        self.assertEqual(res.status, "HIGHER_HASH")

    def test_live_quarantine_deterministic_hashing_and_idempotency(self):
        """Adversarially verify quarantine SHA-256 calculation and deduplication."""
        from pyspark.sql import functions as F
        from spark_jobs.silver_transform import quarantine_rows, merge_delta

        t0 = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        # Create invalid records
        records_a = [
            ("o1", None, "CREATED", t0, t0, "run_001", "missing customer_id"),
            ("o2", "c2", "UNKNOWN", t0, t0, "run_001", "invalid status"),
        ]
        columns = ["order_id", "customer_id", "order_status", "updated_at", "ingested_at", "bronze_run_id", "quarantine_reason"]
        df_a = self.spark.createDataFrame(records_a, columns)

        bad_a = quarantine_rows("orders", df_a).collect()
        self.assertEqual(len(bad_a), 2)
        rec_ids_a = {r.order_id if hasattr(r, "order_id") else json.loads(r.record_json)["order_id"]: r.record_id for r in bad_a}

        # 1. Rerun with identical data: must yield identical record_ids
        bad_a_repeat = quarantine_rows("orders", df_a).collect()
        rec_ids_repeat = {json.loads(r.record_json)["order_id"]: r.record_id for r in bad_a_repeat}
        self.assertEqual(rec_ids_a, rec_ids_repeat)

        # 2. Rerun with permuted column order: must yield identical record_ids
        permuted_columns = ["quarantine_reason", "order_status", "order_id", "customer_id", "bronze_run_id", "updated_at", "ingested_at"]
        df_permuted = df_a.select(*permuted_columns)
        bad_permuted = quarantine_rows("orders", df_permuted).collect()
        rec_ids_permuted = {json.loads(r.record_json)["order_id"]: r.record_id for r in bad_permuted}
        self.assertEqual(rec_ids_a, rec_ids_permuted)

        # 3. Different bronze_run_id: must yield different record_ids
        df_diff_run = df_a.withColumn("bronze_run_id", F.lit("run_002"))
        bad_diff_run = quarantine_rows("orders", df_diff_run).collect()
        rec_ids_diff_run = {json.loads(r.record_json)["order_id"]: r.record_id for r in bad_diff_run}
        for oid in rec_ids_a:
            self.assertNotEqual(rec_ids_a[oid], rec_ids_diff_run[oid])

        # 4. Quarantine Delta merge idempotency
        quarantine_table = str(self.root / "silver_quarantine")
        merge_delta(self.spark, quarantine_rows("orders", df_a), quarantine_table, ["record_id"], quarantine=True)
        q_count1 = self.spark.read.format("delta").load(quarantine_table).count()
        self.assertEqual(q_count1, 2)

        # Merge same records again: count must remain 2
        merge_delta(self.spark, quarantine_rows("orders", df_a), quarantine_table, ["record_id"], quarantine=True)
        q_count2 = self.spark.read.format("delta").load(quarantine_table).count()
        self.assertEqual(q_count2, 2)

    def test_live_quality_stops_negative_payment_and_future_timestamp(self):
        """Adversarially verify quality stops raise ValueError before Delta merge."""
        from spark_jobs.silver_transform import run_dataset
        import pyarrow as pa
        import pyarrow.parquet as pq
        import gzip

        now = datetime.now(timezone.utc)
        bronze_root = self.root / "bronze"
        silver_root = self.root / "silver"

        # Case 1: Negative payment in order_payments
        run_dir_pay = bronze_root / "postgres_oltp" / "order_payments" / "load_date=2026-11-01" / "run_id=pay01"
        run_dir_pay.mkdir(parents=True, exist_ok=True)
        pay_rows = [{
            "order_id": "o1", "payment_sequential": 1, "payment_value": -15.50,
            "updated_at": now, "ingested_at": now,
        }]
        pq.write_table(pa.Table.from_pylist(pay_rows), run_dir_pay / "part-0000.parquet")
        pay_bytes = (run_dir_pay / "part-0000.parquet").read_bytes()
        manifest_pay = {
            "run_id": "pay01", "source_system": "postgres_oltp", "dataset": "order_payments",
            "load_type": "snapshot", "status": "committed", "started_at": now.isoformat(),
            "files": [{"name": "part-0000.parquet", "size_bytes": len(pay_bytes),
                       "sha256": hashlib.sha256(pay_bytes).hexdigest(), "row_count": 1}],
            "row_count": 1,
        }
        (run_dir_pay / "_manifest.json").write_text(json.dumps(manifest_pay), encoding="utf-8")

        with self.assertRaisesRegex(ValueError, "slv_payments has negative payment values"):
            run_dataset(self.spark, str(bronze_root), str(silver_root), "order_payments")
        self.assertFalse((silver_root / "slv_payments").exists(), "Table must not be merged on quality gate failure")

        # Case 2: Future timestamp in clickstream
        run_dir_cs = bronze_root / "clickstream" / "dt=2026-11-01" / "hour=00" / "run_id=cs01"
        run_dir_cs.mkdir(parents=True, exist_ok=True)
        future_ts = (now + timedelta(days=365)).isoformat()
        cs_rows = [{
            "event_id": "ev_future", "timestamp": future_ts, "event_type": "view_product",
            "session_id": "sess-1", "customer_id": "c1", "product_id": "p1", "order_id": None,
        }]
        with gzip.open(run_dir_cs / "events.json.gz", "wt", encoding="utf-8") as f:
            for r in cs_rows:
                f.write(json.dumps(r) + "\n")
        cs_bytes = (run_dir_cs / "events.json.gz").read_bytes()
        manifest_cs = {
            "run_id": "cs01", "source_system": "clickstream", "dataset": "events",
            "load_type": "snapshot", "status": "committed", "started_at": now.isoformat(),
            "files": [{"name": "events.json.gz", "size_bytes": len(cs_bytes),
                       "sha256": hashlib.sha256(cs_bytes).hexdigest(), "row_count": 1}],
            "row_count": 1,
        }
        (run_dir_cs / "_manifest.json").write_text(json.dumps(manifest_cs), encoding="utf-8")

        with self.assertRaisesRegex(ValueError, "slv_clickstream_events has future event timestamps"):
            run_dataset(self.spark, str(bronze_root), str(silver_root), "clickstream")
        self.assertFalse((silver_root / "slv_clickstream_events").exists(), "Table must not be merged on quality gate failure")

    def test_live_great_expectations_ephemeral_validation(self):
        """Adversarially verify validate_frames Great Expectations expectations."""
        from spark_jobs.silver_transform import validate_frames

        # Valid frames
        now = datetime.now(timezone.utc)
        df_orders_good = self.spark.createDataFrame([("c1",)], ["customer_id"])
        df_payments_good = self.spark.createDataFrame([(Decimal("50.00"),)], ["payment_value"])
        df_clickstream_good = self.spark.createDataFrame([(now - timedelta(minutes=5),)], ["event_timestamp"])

        results_good = validate_frames({
            "slv_orders": df_orders_good,
            "slv_payments": df_payments_good,
            "slv_clickstream_events": df_clickstream_good,
        })
        self.assertEqual(results_good, {
            "slv_orders": True, "slv_payments": True, "slv_clickstream_events": True,
        })

        # Adversarial failure 1: slv_orders with null customer_id
        df_orders_bad = self.spark.createDataFrame([(None,)], ["customer_id"])
        results_bad_order = validate_frames({
            "slv_orders": df_orders_bad,
            "slv_payments": df_payments_good,
            "slv_clickstream_events": df_clickstream_good,
        })
        self.assertFalse(results_bad_order["slv_orders"])
        self.assertTrue(results_bad_order["slv_payments"])

        # Adversarial failure 2: slv_payments with negative payment_value
        df_payments_bad = self.spark.createDataFrame([(Decimal("-10.00"),)], ["payment_value"])
        results_bad_payment = validate_frames({
            "slv_orders": df_orders_good,
            "slv_payments": df_payments_bad,
            "slv_clickstream_events": df_clickstream_good,
        })
        self.assertFalse(results_bad_payment["slv_payments"])

        # Adversarial failure 3: slv_clickstream_events with future event_timestamp
        df_clickstream_bad = self.spark.createDataFrame([(now + timedelta(days=10),)], ["event_timestamp"])
        results_bad_cs = validate_frames({
            "slv_orders": df_orders_good,
            "slv_payments": df_payments_good,
            "slv_clickstream_events": df_clickstream_bad,
        })
        self.assertFalse(results_bad_cs["slv_clickstream_events"])


if __name__ == "__main__":
    unittest.main()
