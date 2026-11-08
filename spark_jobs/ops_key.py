"""Stable S3 key for a Silver job's per-run metrics."""

from hashlib import sha256


def silver_metrics_key(prefix: str, run_id: str) -> str:
    digest = sha256(run_id.encode("utf-8")).hexdigest()
    root = f"{prefix.strip('/')}/" if prefix else ""
    return f"{root}ops/silver/{digest}.json"
