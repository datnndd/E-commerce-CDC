"""Copy only manifest-committed S3 Bronze runs to the local Silver input."""

import argparse
import hashlib
import json
import os
import tempfile
from pathlib import Path

from ingestion.s3 import pipeline_s3_prefix, s3_client_and_bucket


def _atomic_download(s3, bucket: str, key: str, target: Path, expected: dict) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.is_file() and target.stat().st_size == expected["size_bytes"]:
        with target.open("rb") as stream:
            if hashlib.file_digest(stream, "sha256").hexdigest() == expected["sha256"]:
                return
    with tempfile.NamedTemporaryFile(dir=target.parent, delete=False) as stream:
        temporary = Path(stream.name)
    try:
        s3.download_file(bucket, key, str(temporary))
        with temporary.open("rb") as stream:
            digest = hashlib.file_digest(stream, "sha256").hexdigest()
        if temporary.stat().st_size != expected["size_bytes"] or digest != expected["sha256"]:
            raise ValueError(f"{key}: size or SHA-256 differs from committed manifest")
        os.replace(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)


def sync_committed(s3, bucket: str, root: Path) -> tuple[int, int]:
    manifests = files = 0
    namespace = pipeline_s3_prefix()
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=namespace + "bronze/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith("/_manifest.json"):
                continue
            if ".." in Path(key).parts:
                raise ValueError(f"Unsafe Bronze key: {key}")
            manifest = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
            if manifest.get("status") != "committed":
                continue
            if not manifest.get("files"):
                raise ValueError(f"{key}: committed manifest has no files")
            prefix = key.rsplit("/", 1)[0] + "/"
            local_prefix = prefix.removeprefix(namespace)
            for item in manifest["files"]:
                name = item["name"]
                if Path(name).name != name or name in (".", ".."):
                    raise ValueError(f"{key}: unsafe file name {name}")
                _atomic_download(s3, bucket, prefix + name, root / local_prefix / name, item)
                files += 1
            target = root / key.removeprefix(namespace)
            target.parent.mkdir(parents=True, exist_ok=True)
            with tempfile.NamedTemporaryFile(dir=target.parent, delete=False) as stream:
                temporary = Path(stream.name)
                stream.write(json.dumps(manifest).encode("utf-8"))
            os.replace(temporary, target)
            manifests += 1
    if not manifests:
        raise ValueError("No committed Bronze manifests found in S3")
    return manifests, files


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    args = parser.parse_args()
    s3, bucket = s3_client_and_bucket()
    manifests, files = sync_committed(s3, bucket, args.root)
    print(f"Synced {files} files from {manifests} committed Bronze runs")


if __name__ == "__main__":
    main()
