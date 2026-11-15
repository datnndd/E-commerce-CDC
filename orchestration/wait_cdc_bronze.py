"""Gate a batch run on committed Bronze through Kafka's current offsets."""

import json
import os
import time
from urllib.request import urlopen

from confluent_kafka import Consumer, TopicPartition

from ingestion.kafka_to_s3 import published_high_water
from ingestion.s3 import s3_client_and_bucket
from spark_jobs.silver_schema import OLTP_NAMES


def main() -> None:
    connector = os.environ.get("CDC_CONNECTOR_NAME", "ecommerce-postgres-cdc")
    connect_url = os.environ.get("KAFKA_CONNECT_URL", "http://connect:8083").rstrip("/")
    with urlopen(f"{connect_url}/connectors/{connector}/status", timeout=10) as response:
        status = json.load(response)
    if status["connector"]["state"] != "RUNNING" or any(
        task["state"] != "RUNNING" for task in status["tasks"]
    ):
        raise RuntimeError(f"Debezium connector {connector} is not running")

    prefix = os.environ.get("CDC_TOPIC_PREFIX", "ecommerce_cdc")
    consumer = Consumer({"bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092"),
                         "group.id": "ecommerce-cdc-bronze-gate", "enable.auto.commit": False})
    try:
        targets = {}
        for table in OLTP_NAMES:
            topic = f"{prefix}.public.{table}"
            metadata = consumer.list_topics(topic, timeout=10)
            if topic not in metadata.topics or len(metadata.topics[topic].partitions) != 1:
                raise ValueError(f"{topic}: expected one Kafka partition")
            _, high = consumer.get_watermark_offsets(TopicPartition(topic, 0), timeout=10)
            targets[topic] = high - 1
    finally:
        consumer.close()

    s3, bucket = s3_client_and_bucket()
    deadline = time.monotonic() + int(os.environ.get("CDC_BRONZE_WAIT_SECONDS", "300"))
    while True:
        lagging = {topic: target for topic, target in targets.items()
                   if published_high_water(s3, bucket, topic, 0, prefix, verify_files=False) < target}
        if not lagging:
            print("All nine CDC topics are manifest-committed through their starting Kafka offsets")
            return
        if time.monotonic() >= deadline:
            raise TimeoutError(f"CDC Bronze is behind Kafka: {lagging}")
        time.sleep(5)


if __name__ == "__main__":
    main()
