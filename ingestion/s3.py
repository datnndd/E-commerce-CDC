"""S3 credentials and bucket settings shared by Bronze ingesters."""

import os
from pathlib import Path

import boto3
from dotenv import dotenv_values


ROOT = Path(__file__).resolve().parents[1]
AWS_KEY_NAMES = ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN")


def s3_session(local):
    environment_keys = {name: os.environ.get(name) for name in AWS_KEY_NAMES}
    file_keys = {name: local.get(name) for name in AWS_KEY_NAMES}
    environment_has_keys = any(name in os.environ for name in AWS_KEY_NAMES)
    file_has_keys = any(name in local for name in AWS_KEY_NAMES)
    keys = environment_keys if environment_has_keys else file_keys
    if environment_has_keys or file_has_keys:
        if not keys["AWS_ACCESS_KEY_ID"] or not keys["AWS_SECRET_ACCESS_KEY"]:
            raise ValueError("Set both AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY for S3 access")
        return boto3.Session(
            aws_access_key_id=keys["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=keys["AWS_SECRET_ACCESS_KEY"],
            aws_session_token=keys["AWS_SESSION_TOKEN"],
        )
    profile = os.environ.get("AWS_PROFILE") or local.get("AWS_PROFILE")
    return boto3.Session(profile_name=profile)


def s3_client_and_bucket():
    local = dotenv_values(ROOT / ".env")
    bucket = os.environ.get("S3_BUCKET") or local.get("S3_BUCKET")
    if not bucket or bucket == "your-project-bucket":
        raise ValueError("Set S3_BUCKET in the environment or .env")
    region = os.environ.get("AWS_REGION") or local.get("AWS_REGION") or "ap-southeast-1"
    return s3_session(local).client("s3", region_name=region), bucket
