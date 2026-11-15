"""Write Debezium Kafka partitions as manifest-committed S3 Bronze batches."""

import hashlib
import json
import os
import time
from datetime import datetime, timezone
from urllib.parse import quote

from botocore.exceptions import ClientError
from confluent_kafka import Consumer, KafkaException, TopicPartition

from ingestion.s3 import pipeline_s3_prefix, s3_client_and_bucket
from spark_jobs.silver_schema import OLTP_NAMES


def _get(s3, bucket: str, key: str) -> bytes | None:
    try:
        return s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    except ClientError as exc:
        if exc.response["Error"]["Code"] in {"NoSuchKey", "404"}:
            return None
        raise


def dataset_for_topic(topic: str, prefix: str) -> str:
    head = f"{prefix}.public."
    if not topic.startswith(head) or topic[len(head):] not in OLTP_NAMES:
        raise ValueError(f"Unexpected CDC topic: {topic}")
    return topic[len(head):]


def bronze_partition_prefix(topic: str, partition: int, prefix: str) -> str:
    dataset = dataset_for_topic(topic, prefix)
    if partition != 0:
        raise ValueError(f"{topic}: expected one partition, got {partition}")
    return pipeline_s3_prefix() + f"bronze/postgres_debezium/{dataset}/topic={quote(topic, safe='._-')}/partition=0/"


def encode_message(message) -> dict:
    value = json.loads(message.value()) if message.value() is not None else None
    key = json.loads(message.key()) if message.key() is not None else None
    if value is not None:
        payload = value.get("payload", value)
        if payload.get("op") not in {"r", "c", "u", "d"} or not isinstance(payload.get("source"), dict):
            raise ValueError(f"{message.topic()} offset {message.offset()}: invalid Debezium envelope")
    _, timestamp_ms = message.timestamp()
    if timestamp_ms < 0:
        raise ValueError(f"{message.topic()} offset {message.offset()}: missing Kafka timestamp")
    return {
        "topic": message.topic(), "partition": message.partition(), "offset": message.offset(),
        "key": key, "value": value,
        "ingested_at": datetime.fromtimestamp(timestamp_ms / 1000, timezone.utc).isoformat(),
    }


def publish_batch(s3, bucket: str, records: list[dict], prefix: str) -> int:
    if not records:
        raise ValueError("Cannot publish an empty CDC batch")
    topic, partition = records[0]["topic"], records[0]["partition"]
    offsets = [record["offset"] for record in records]
    if any(record["topic"] != topic or record["partition"] != partition for record in records):
        raise ValueError("A CDC batch must contain one Kafka partition")
    if offsets != list(range(offsets[0], offsets[-1] + 1)):
        raise ValueError(f"{topic}: CDC batch offsets are not contiguous")
    base = bronze_partition_prefix(topic, partition, prefix)
    run_id = f"offset={offsets[0]:020d}-{offsets[-1]:020d}"
    path = f"{base}{run_id}/"
    data = b"".join(json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8") + b"\n"
                    for record in records)
    digest = hashlib.sha256(data).hexdigest()
    file_key = path + "events.jsonl"
    manifest_key = path + "_manifest.json"
    existing = _get(s3, bucket, file_key)
    if existing is not None and hashlib.sha256(existing).hexdigest() != digest:
        raise ValueError(f"{file_key}: existing batch differs from Kafka replay")
    if existing is None:
        s3.put_object(Bucket=bucket, Key=file_key, Body=data, ContentType="application/x-ndjson")
    manifest = {
        "run_id": run_id, "source_system": "postgres_debezium",
        "dataset": dataset_for_topic(topic, prefix), "load_type": "cdc",
        "status": "committed", "topic": topic, "partition": partition,
        "offset_start": offsets[0], "offset_end": offsets[-1],
        "row_count": len(records),
        "started_at": records[0]["ingested_at"],
        "finished_at": records[-1]["ingested_at"],
        "files": [{"name": "events.jsonl", "row_count": len(records),
                   "size_bytes": len(data), "sha256": digest}],
    }
    previous = _get(s3, bucket, manifest_key)
    encoded = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode("utf-8")
    if previous is not None and previous != encoded:
        raise ValueError(f"{manifest_key}: existing manifest differs from Kafka replay")
    if previous is None:
        s3.put_object(Bucket=bucket, Key=manifest_key, Body=encoded, ContentType="application/json")
    if _get(s3, bucket, manifest_key) != encoded:
        raise RuntimeError(f"{manifest_key}: manifest readback failed")
    return offsets[-1]


def published_high_water(s3, bucket: str, topic: str, partition: int, prefix: str,
                         verify_files: bool = True) -> int:
    base = bronze_partition_prefix(topic, partition, prefix)
    ranges = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=base):
        for item in page.get("Contents", []):
            key = item["Key"]
            if not key.endswith("/_manifest.json"):
                continue
            manifest = json.loads(_get(s3, bucket, key))
            if manifest.get("status") != "committed" or manifest.get("topic") != topic or manifest.get("partition") != partition:
                raise ValueError(f"{key}: invalid CDC manifest")
            if verify_files:
                data = _get(s3, bucket, key[:-len("_manifest.json")] + manifest["files"][0]["name"])
                if data is None or hashlib.sha256(data).hexdigest() != manifest["files"][0]["sha256"]:
                    raise ValueError(f"{key}: missing or corrupt CDC data")
            ranges.append((manifest["offset_start"], manifest["offset_end"]))
    expected = 0
    for start, end in sorted(ranges):
        if start != expected or end < start:
            raise ValueError(f"{topic}: gap or overlap in published Kafka offsets at {expected}")
        expected = end + 1
    return expected - 1


def run() -> None:
    s3, bucket = s3_client_and_bucket()
    if os.environ.get("CDC_LOCAL_CREATE_BUCKET") == "true":
        if not os.environ.get("S3_ENDPOINT_URL"):
            raise ValueError("CDC_LOCAL_CREATE_BUCKET requires an S3-compatible endpoint")
        try:
            s3.head_bucket(Bucket=bucket)
        except ClientError as exc:
            if exc.response["Error"]["Code"] not in {"404", "NoSuchBucket"}:
                raise
            s3.create_bucket(Bucket=bucket)
    prefix = os.environ.get("CDC_TOPIC_PREFIX", "ecommerce_cdc")
    topics = [f"{prefix}.public.{name}" for name in OLTP_NAMES]
    consumer = Consumer({
        "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
        "group.id": os.environ.get("CDC_SINK_GROUP", "ecommerce-cdc-bronze-v1"),
        "enable.auto.commit": False, "enable.auto.offset.store": False,
        "auto.offset.reset": "earliest",
    })
    buffers: dict[tuple[str, int], list[dict]] = {}
    high: dict[tuple[str, int], int] = {}
    last_flush = time.monotonic()
    max_records = int(os.environ.get("CDC_BATCH_RECORDS", "5000"))
    flush_seconds = float(os.environ.get("CDC_FLUSH_SECONDS", "5"))
    if max_records < 1 or flush_seconds <= 0:
        raise ValueError("CDC_BATCH_RECORDS and CDC_FLUSH_SECONDS must be positive")

    def flush() -> None:
        nonlocal last_flush
        for (topic, partition), records in list(buffers.items()):
            if not records:
                continue
            end = publish_batch(s3, bucket, records, prefix)
            consumer.commit(offsets=[TopicPartition(topic, partition, end + 1)], asynchronous=False)
            high[(topic, partition)] = end
            buffers[(topic, partition)] = []
            print(f"{topic}[{partition}] committed through offset {end}")
        last_flush = time.monotonic()

    def on_revoke(_, __) -> None:
        flush()

    try:
        consumer.subscribe(topics, on_revoke=on_revoke)
        while True:
            message = consumer.poll(1)
            if message is not None:
                if message.error():
                    raise KafkaException(message.error())
                identity = (message.topic(), message.partition())
                if identity not in high:
                    high[identity] = published_high_water(s3, bucket, *identity, prefix)
                    committed = consumer.committed([TopicPartition(*identity)], timeout=10)[0].offset
                    if committed >= 0 and committed > high[identity] + 1:
                        raise ValueError(f"{identity}: Kafka committed beyond published Bronze")
                if message.offset() <= high[identity]:
                    if message.offset() == high[identity]:
                        consumer.commit(offsets=[TopicPartition(*identity, high[identity] + 1)], asynchronous=False)
                    continue
                records = buffers.setdefault(identity, [])
                expected = records[-1]["offset"] + 1 if records else high[identity] + 1
                if message.offset() != expected:
                    raise ValueError(f"{identity}: missing Kafka offset before {message.offset()}")
                records.append(encode_message(message))
                if len(records) >= max_records:
                    flush()
            if time.monotonic() - last_flush >= flush_seconds:
                flush()
    finally:
        if buffers:
            flush()
        consumer.close()


if __name__ == "__main__":
    run()
