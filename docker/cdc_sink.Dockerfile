FROM python:3.13-slim

RUN pip install --no-cache-dir 'boto3>=1.40,<2' 'confluent-kafka>=2.12,<3' 'python-dotenv>=1,<2'
WORKDIR /workspace
COPY ingestion/__init__.py ingestion/s3.py ingestion/kafka_to_s3.py /workspace/ingestion/
COPY spark_jobs/__init__.py spark_jobs/silver_schema.py /workspace/spark_jobs/
CMD ["python", "-m", "ingestion.kafka_to_s3"]
