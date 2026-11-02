"""Run Great Expectations checkpoints on local Delta Silver before publication."""

import argparse
from datetime import datetime, timezone
from pathlib import Path

import great_expectations as gx

from spark_jobs.silver_transform import local_spark


def check_frame(context, name: str, frame, expectations: list) -> bool:
    source = context.data_sources.add_spark(name=f"source_{name}")
    asset = source.add_dataframe_asset(name=f"asset_{name}")
    batch = asset.add_batch_definition_whole_dataframe("current")
    suite = context.suites.add(gx.ExpectationSuite(name=f"suite_{name}"))
    for expectation in expectations:
        suite.add_expectation(expectation)
    definition = context.validation_definitions.add(
        gx.ValidationDefinition(name=f"validation_{name}", data=batch, suite=suite)
    )
    checkpoint = context.checkpoints.add(
        gx.Checkpoint(name=f"checkpoint_{name}", validation_definitions=[definition])
    )
    return checkpoint.run(batch_parameters={"dataframe": frame}).success


def validate(silver_root: Path) -> dict[str, bool]:
    spark = local_spark()
    try:
        context = gx.get_context(mode="ephemeral")
        checks = {
            "slv_orders": [gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_id")],
            "slv_payments": [gx.expectations.ExpectColumnValuesToBeBetween(column="payment_value", min_value=0)],
            "slv_clickstream_events": [
                gx.expectations.ExpectColumnValuesToBeBetween(
                    column="event_timestamp", max_value=datetime.now(timezone.utc)
                )
            ],
        }
        results = {}
        for table, expectations in checks.items():
            path = silver_root / table
            if not path.is_dir():
                raise FileNotFoundError(f"Missing Silver table: {path}")
            frame = spark.read.format("delta").load(str(path))
            results[table] = check_frame(context, table, frame, expectations)
        return results
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--silver-root", type=Path, required=True)
    args = parser.parse_args()
    results = validate(args.silver_root)
    for table, passed in results.items():
        print(f"{table}: {'PASS' if passed else 'FAIL'}")
    if not all(results.values()):
        raise SystemExit("Silver quality checkpoint failed; Gold was not run")


if __name__ == "__main__":
    main()
