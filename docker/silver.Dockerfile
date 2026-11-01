FROM python:3.13-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jdk-headless \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir uv==0.11.21

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV UV_PROJECT_ENVIRONMENT=/opt/venv
WORKDIR /workspace
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

CMD ["uv", "run", "--frozen", "python", "-m", "spark_jobs.silver_transform", "--help"]
