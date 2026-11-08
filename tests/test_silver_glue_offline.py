"""Offline proof for the actual Airflow-to-Glue argument and catalog contract."""

import argparse
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import yaml

from spark_jobs.silver_schema import SILVER_NAMES
from spark_jobs.silver_transform import parse_glue_args, register_glue_catalog, write_silver_metrics
from spark_jobs.ops_key import silver_metrics_key
from orchestration.package_glue_job import package


def glue_resolver(argv, names):
    parser = argparse.ArgumentParser()
    for name in names:
        parser.add_argument(f"--{name}", required=True)
    return vars(parser.parse_known_args(argv[1:])[0])


class GlueContractTest(unittest.TestCase):
    def test_glue_6_delta_runtime_contract(self):
        class CloudFormationLoader(yaml.SafeLoader):
            pass

        CloudFormationLoader.add_constructor(
            "!Ref", lambda loader, node: loader.construct_scalar(node)
        )
        root = Path(__file__).resolve().parent.parent
        template = yaml.load(
            (root / "infra" / "glue_silver_job.yaml").read_text(encoding="utf-8"),
            Loader=CloudFormationLoader,
        )
        job = template["Resources"]["SilverJob"]["Properties"]
        args = job["DefaultArguments"]
        self.assertEqual(job["GlueVersion"], "6.0")
        self.assertEqual(job["Command"]["Name"], "glueetl")
        self.assertEqual(job["Command"]["PythonVersion"], "3")
        self.assertEqual(args["--datalake-formats"], "delta")
        self.assertIn("--enable-glue-datacatalog", args)
        self.assertIn("--additional-python-modules", args)
        self.assertIn("--extra-py-files", args)
        self.assertNotIn("--extra-jars", args)

        required_conf = (
            "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        for setting in required_conf:
            self.assertIn(setting, args["--conf"])
        self.assertNotIn("spark.delta.logStore.class", args["--conf"])

        for path in (
            root / "airflow" / "dbt_profile" / "profiles.yml",
            root / "dbt_project" / "profiles.yml.example",
        ):
            outputs = yaml.safe_load(path.read_text(encoding="utf-8"))["ecommerce_aws"]["outputs"]
            for name, profile in outputs.items():
                with self.subTest(path=path.name, output=name):
                    self.assertEqual(profile["type"], "glue")
                    self.assertEqual(profile["glue_version"], "6.0")
                    self.assertEqual(profile["datalake_formats"], "delta")
                    for setting in required_conf:
                        self.assertIn(setting, profile["conf"])
                    self.assertNotIn("spark.delta.logStore.class", profile["conf"])
                    self.assertNotIn("extra_jars", profile)

    def test_canonical_underscore_arguments_resolve(self):
        argv = ["job.py", "--JOB_NAME", "silver", "--bronze_root", "s3://bucket",
                "--silver_root", "s3://bucket/silver/glue_v1", "--glue_database", "silver_aws",
                "--pipeline_run_id", "scheduled__2026-11-07"]
        parsed = parse_glue_args(argv, resolve_fn=glue_resolver)
        self.assertEqual(parsed.glue_database, "silver_aws")
        self.assertEqual(parsed.pipeline_run_id, "scheduled__2026-11-07")

    def test_hyphenated_glue_arguments_fail_before_spark(self):
        argv = ["job.py", "--JOB_NAME", "silver", "--bronze-root", "s3://bucket",
                "--silver_root", "s3://bucket/silver", "--glue_database", "silver_aws",
                "--pipeline_run_id", "run"]
        with self.assertRaisesRegex(ValueError, "require underscores"):
            parse_glue_args(argv, resolve_fn=glue_resolver)

    def test_glue_rejects_missing_bucket_and_partial_run(self):
        base = ["job.py", "--JOB_NAME", "silver", "--bronze_root", "s3:///",
                "--silver_root", "s3://bucket/silver", "--glue_database", "silver_aws",
                "--pipeline_run_id", "run"]
        with self.assertRaisesRegex(ValueError, "Invalid S3 path"):
            parse_glue_args(base, resolve_fn=glue_resolver)
        base[4] = "s3://bucket"
        with self.assertRaisesRegex(ValueError, "all datasets"):
            parse_glue_args(base + ["--dataset", "orders"], resolve_fn=glue_resolver)

    def test_registers_all_tables_after_gate_and_rejects_repoint(self):
        spark = MagicMock()
        spark.catalog.tableExists.return_value = False
        register_glue_catalog(spark, "s3://bucket/silver/glue_v1", "silver_aws")
        calls = [call.args[0] for call in spark.sql.call_args_list]
        self.assertEqual(sum("CREATE TABLE" in sql for sql in calls), len(SILVER_NAMES) + 1)
        self.assertIn("silver_quarantine", calls[-1])

        spark.reset_mock()
        spark.catalog.tableExists.return_value = True
        spark.sql.return_value.select.return_value.first.return_value.location = "s3://bucket/old/slv_customers"
        with self.assertRaisesRegex(ValueError, "already points"):
            register_glue_catalog(spark, "s3://bucket/silver/glue_v1", "silver_aws")
        self.assertFalse(any("CREATE TABLE" in call.args[0] for call in spark.sql.call_args_list))

    def test_metrics_use_measured_delta_counts_and_stable_run_key(self):
        spark = MagicMock()
        spark.read.format.return_value.load.return_value.count.return_value = 3
        with patch("boto3.client") as client:
            write_silver_metrics(spark, "s3://bucket/silver/glue_v1", "s3://bucket/isolated", "run/1")
        kwargs = client.return_value.put_object.call_args.kwargs
        self.assertEqual(kwargs["Bucket"], "bucket")
        self.assertEqual(kwargs["Key"], silver_metrics_key("isolated", "run/1"))
        data = json.loads(kwargs["Body"])
        self.assertEqual(data["silver_counts"]["slv_customers"], 3)
        self.assertEqual(len(data["silver_counts"]), len(SILVER_NAMES))

    def test_package_is_reproducible_and_contains_shared_code(self):
        with tempfile.TemporaryDirectory() as tmp:
            first = package(Path(tmp))
            second = package(Path(tmp))
            self.assertEqual(first, second)
            self.assertTrue((first / "silver_transform.py").exists())
            from zipfile import ZipFile
            with ZipFile(first / "spark_jobs.zip") as archive:
                self.assertIn("spark_jobs/silver_schema.py", archive.namelist())
                self.assertIn("spark_jobs/ops_key.py", archive.namelist())


if __name__ == "__main__":
    unittest.main()
