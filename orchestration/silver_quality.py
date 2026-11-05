"""Run the Silver quality checks against a local Delta directory."""

import argparse
from pathlib import Path

from spark_jobs.silver_transform import local_spark, validate_frames


def validate(silver_root: Path) -> dict[str, bool]:
    spark = local_spark()
    try:
        frames = {}
        for table in ("slv_orders", "slv_payments", "slv_clickstream_events"):
            path = silver_root / table
            if not path.is_dir():
                raise FileNotFoundError(f"Missing Silver table: {path}")
            frames[table] = spark.read.format("delta").load(str(path))
        return validate_frames(frames)
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
