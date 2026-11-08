"""Offline verification unit tests for dbt AWS Glue migration.

Tests verify that:
1. `dbt parse` succeeds with returncode 0 offline without live AWS credentials or Databricks environment variables.
2. All 35 models, 2 snapshots, and 19 custom data tests are indexed in `manifest.json`.
3. Zero references to `dbt-databricks`, `DBT_DATABRICKS_HOST`, `DBT_DATABRICKS_HTTP_PATH`, or `DATABRICKS_TOKEN` remain in `dbt_project/`.
4. Staging sources resolve cleanly using a 2-tier namespace without requiring `DBT_CATALOG`.
5. Profile configurations across local, example, and Airflow targets match the AWS Glue adapter contract.
"""

import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DBT_PROJECT_DIR = ROOT / "dbt_project"
AIRFLOW_DBT_PROFILE_DIR = ROOT / "airflow" / "dbt_profile"
TARGET_DIR = DBT_PROJECT_DIR / "target"
MANIFEST_PATH = TARGET_DIR / "manifest.json"

EXPECTED_STAGING_MODELS = {
    "stg_campaigns",
    "stg_category_translation",
    "stg_clickstream_events",
    "stg_customers",
    "stg_exchange_rates",
    "stg_geolocation",
    "stg_order_items",
    "stg_orders",
    "stg_payments",
    "stg_products",
    "stg_reviews",
    "stg_sellers",
}

EXPECTED_INTERMEDIATE_MODELS = {
    "int_customer_orders",
    "int_orders_enriched",
}

EXPECTED_CORE_MODELS = {
    "dim_campaign",
    "dim_category",
    "dim_channel",
    "dim_customer",
    "dim_date",
    "dim_geography",
    "dim_product",
    "dim_seller",
    "fct_clickstream_events",
    "fct_exchange_rates",
    "fct_order_items",
    "fct_orders",
    "fct_payments",
    "fct_reviews",
    "fct_sessions",
}

EXPECTED_BUSINESS_MODELS = {
    "mart_customer_rfm",
    "mart_daily_sales",
    "mart_funnel_daily",
    "mart_order_experience_daily",
    "mart_payment_mix_daily",
    "mart_product_performance",
}

EXPECTED_SNAPSHOTS = {
    "customers_snapshot",
    "products_snapshot",
}

EXPECTED_CUSTOM_TESTS = {
    "assert_core_grains",
    "assert_core_reconciliation",
    "assert_daily_sales_grain",
    "assert_experience_totals",
    "assert_funnel_bounds",
    "assert_funnel_daily_grain",
    "assert_funnel_totals",
    "assert_item_revenue",
    "assert_nonnegative_items",
    "assert_order_experience_grain",
    "assert_payment_mix_grain",
    "assert_payment_totals",
    "assert_product_performance_grain",
    "assert_product_totals",
    "assert_rfm_scores",
    "assert_rfm_ties",
    "assert_rfm_totals",
    "assert_sales_totals",
    "assert_scd2_windows",
}

EXPECTED_SOURCES = {
    "slv_campaigns",
    "slv_category_translation",
    "slv_clickstream_events",
    "slv_customers",
    "slv_exchange_rates",
    "slv_geolocation",
    "slv_order_items",
    "slv_orders",
    "slv_payments",
    "slv_products",
    "slv_reviews",
    "slv_sellers",
}


def _run_dbt_parse(profiles_dir: Path) -> subprocess.CompletedProcess:
    """Execute dbt parse with an isolated offline environment."""
    env = os.environ.copy()
    # Strip any AWS or Databricks credentials
    for key in list(env.keys()):
        if "DATABRICKS" in key or key in {
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
            "AWS_SECURITY_TOKEN",
            "DBT_CATALOG",
        }:
            del env[key]

    # Provide safe fallback mock region/role for Glue profile without network access
    env["AWS_DEFAULT_REGION"] = "ap-southeast-1"
    env["AWS_REGION"] = "ap-southeast-1"
    env["AWS_EC2_METADATA_DISABLED"] = "true"
    env["DBT_TARGET_SCHEMA"] = "analytics_dev"
    env["DBT_SILVER_SCHEMA"] = "silver"
    env["GLUE_ROLE_ARN"] = "arn:aws:iam::111122223333:role/OfflineGlueRole"
    env["DBT_S3_LOCATION"] = "s3://offline-bucket/dbt"

    template = DBT_PROJECT_DIR / "profiles.yml.example"
    temporary = tempfile.TemporaryDirectory() if profiles_dir == DBT_PROJECT_DIR else None
    if temporary:
        profiles_dir = Path(temporary.name)
        shutil.copyfile(template, profiles_dir / "profiles.yml")
    cmd = [
        sys.executable,
        "-m",
        "dbt.cli.main",
        "parse",
        "--project-dir",
        str(DBT_PROJECT_DIR),
        "--profiles-dir",
        str(profiles_dir),
        "--no-version-check",
        "--no-send-anonymous-usage-stats",
    ]
    try:
        return subprocess.run(
            cmd, cwd=ROOT, capture_output=True, text=True, env=env,
        )
    finally:
        if temporary:
            temporary.cleanup()


class TestDbtOffline(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Run dbt parse to guarantee an up-to-date manifest
        cls.project_parse_result = _run_dbt_parse(DBT_PROJECT_DIR)
        if cls.project_parse_result.returncode != 0:
            raise RuntimeError(
                f"Initial dbt parse failed:\nSTDOUT:\n{cls.project_parse_result.stdout}\nSTDERR:\n{cls.project_parse_result.stderr}"
            )
        with open(MANIFEST_PATH, "r", encoding="utf-8") as f:
            cls.manifest = json.load(f)

    def test_dbt_parse_succeeds_offline_with_project_profiles(self):
        proc = self.project_parse_result
        self.assertEqual(
            proc.returncode,
            0,
            f"dbt parse failed with returncode {proc.returncode}.\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}",
        )
        self.assertTrue(MANIFEST_PATH.exists(), "target/manifest.json was not generated.")

    def test_dbt_parse_succeeds_offline_with_airflow_profiles(self):
        proc = _run_dbt_parse(AIRFLOW_DBT_PROFILE_DIR)
        self.assertEqual(
            proc.returncode,
            0,
            f"dbt parse with Airflow profile failed with returncode {proc.returncode}.\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}",
        )

    def test_manifest_indexes_all_35_models(self):
        models = {
            v["name"]: v
            for k, v in self.manifest["nodes"].items()
            if v["resource_type"] == "model" and v["package_name"] == "ecommerce_cdc"
        }
        self.assertEqual(len(models), 35, f"Expected exactly 35 models, got {len(models)}")

        # Verify model category distribution
        model_names = set(models.keys())
        self.assertEqual(model_names & EXPECTED_STAGING_MODELS, EXPECTED_STAGING_MODELS)
        self.assertEqual(model_names & EXPECTED_INTERMEDIATE_MODELS, EXPECTED_INTERMEDIATE_MODELS)
        self.assertEqual(model_names & EXPECTED_CORE_MODELS, EXPECTED_CORE_MODELS)
        self.assertEqual(model_names & EXPECTED_BUSINESS_MODELS, EXPECTED_BUSINESS_MODELS)

    def test_manifest_indexes_all_2_snapshots(self):
        snapshots = {
            v["name"]: v
            for k, v in self.manifest["nodes"].items()
            if v["resource_type"] == "snapshot" and v["package_name"] == "ecommerce_cdc"
        }
        self.assertEqual(len(snapshots), 2, f"Expected exactly 2 snapshots, got {len(snapshots)}")
        self.assertEqual(set(snapshots.keys()), EXPECTED_SNAPSHOTS)

    def test_manifest_indexes_all_19_custom_data_tests(self):
        all_tests = {
            v["name"]: v
            for k, v in self.manifest["nodes"].items()
            if v["resource_type"] == "test" and v["package_name"] == "ecommerce_cdc"
        }
        custom_tests = {
            v["name"]: v
            for name, v in all_tests.items()
            if "tests/" in v.get("original_file_path", "").replace("\\", "/")
        }
        self.assertEqual(len(custom_tests), 19, f"Expected 19 custom tests in tests/, found {len(custom_tests)}")
        self.assertEqual(set(custom_tests.keys()), EXPECTED_CUSTOM_TESTS)

    def test_manifest_indexes_all_12_sources(self):
        sources = {
            v["name"]: v
            for k, v in self.manifest["sources"].items()
            if v["package_name"] == "ecommerce_cdc" and v["source_name"] == "silver"
        }
        self.assertEqual(len(sources), 12, f"Expected exactly 12 Silver sources, got {len(sources)}")
        self.assertEqual(set(sources.keys()), EXPECTED_SOURCES)

    def test_zero_databricks_references_in_dbt_project(self):
        prohibited_terms = [
            "dbt-databricks",
            "DBT_DATABRICKS_HOST",
            "DBT_DATABRICKS_HTTP_PATH",
            "DATABRICKS_TOKEN",
            "type: databricks",
        ]
        violations = []
        for path in DBT_PROJECT_DIR.rglob("*"):
            if path.is_file() and not any(part in {"target", "dbt_packages", "logs", ".git"} for part in path.parts):
                try:
                    content = path.read_text(encoding="utf-8", errors="ignore")
                    for term in prohibited_terms:
                        if term in content:
                            violations.append(f"{path.relative_to(ROOT)}: contains prohibited term '{term}'")
                except Exception:
                    pass

        # Also check airflow profile
        airflow_profile = AIRFLOW_DBT_PROFILE_DIR / "profiles.yml"
        if airflow_profile.exists():
            content = airflow_profile.read_text(encoding="utf-8")
            for term in prohibited_terms:
                if term in content:
                    violations.append(f"{airflow_profile.relative_to(ROOT)}: contains prohibited term '{term}'")

        self.assertEqual(violations, [], f"Found Databricks references in dbt files:\n" + "\n".join(violations))

    def test_sources_resolve_without_dbt_catalog(self):
        sources_yml_path = DBT_PROJECT_DIR / "models" / "staging" / "sources.yml"
        content = sources_yml_path.read_text(encoding="utf-8")
        self.assertNotIn(
            "DBT_CATALOG",
            content,
            "dbt_project/models/staging/sources.yml must not reference DBT_CATALOG",
        )

        # Confirm 2-tier database/schema configuration in manifest
        for source_key, source_node in self.manifest["sources"].items():
            if source_node.get("source_name") == "silver":
                # In dbt-glue, database is omitted or matches schema
                schema = source_node.get("schema", "")
                self.assertEqual(schema, "silver", f"Expected source schema 'silver', got '{schema}'")
                database = source_node.get("database")
                self.assertTrue(
                    database is None or database == schema,
                    f"Source {source_key} has 3-tier database '{database}' distinct from schema '{schema}'",
                )

    def test_dbt_project_configuration_contract(self):
        dbt_project_yml = (DBT_PROJECT_DIR / "dbt_project.yml").read_text(encoding="utf-8")
        self.assertIn(
            "profile: ecommerce_aws",
            dbt_project_yml,
            "dbt_project.yml must configure profile 'ecommerce_aws'",
        )

    def test_profiles_yml_contracts(self):
        for path in [
            DBT_PROJECT_DIR / "profiles.yml.example",
            AIRFLOW_DBT_PROFILE_DIR / "profiles.yml",
        ]:
            content = path.read_text(encoding="utf-8")
            self.assertIn("ecommerce_aws:", content, f"{path} must contain profile 'ecommerce_aws'")
            self.assertIn("type: glue", content, f"{path} must specify 'type: glue'")
            self.assertNotIn("type: databricks", content, f"{path} must not specify 'type: databricks'")


if __name__ == "__main__":
    unittest.main()
