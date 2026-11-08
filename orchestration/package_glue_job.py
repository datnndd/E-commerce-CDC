"""Build immutable S3-ready Glue script and Python package artifacts."""

import argparse
from hashlib import sha256
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

ROOT = Path(__file__).resolve().parents[1]
FILES = ("__init__.py", "silver_schema.py", "ops_key.py")


def package(output_root: Path) -> Path:
    source = ROOT / "spark_jobs" / "silver_transform.py"
    payloads = [("silver_transform.py", source.read_bytes())]
    payloads.extend((f"spark_jobs/{name}", (ROOT / "spark_jobs" / name).read_bytes()) for name in FILES)
    digest = sha256(b"".join(name.encode() + content for name, content in payloads)).hexdigest()[:16]
    target = output_root / digest
    target.mkdir(parents=True, exist_ok=True)
    (target / "silver_transform.py").write_bytes(payloads[0][1])
    with ZipFile(target / "spark_jobs.zip", "w", ZIP_DEFLATED) as archive:
        for name, content in payloads[1:]:
            archive.writestr(name, content)
    return target


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", type=Path, default=ROOT / "dist" / "glue")
    args = parser.parse_args()
    print(package(args.output_root))
