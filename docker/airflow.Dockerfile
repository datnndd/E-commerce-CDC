FROM apache/airflow:3.3.2-python3.13

USER root
ENV UV_PROJECT_ENVIRONMENT=/opt/project-venv
ENV PYTHONPATH=/workspace
RUN mkdir -p /opt/project-venv /opt/project && chown -R airflow:root /opt/project-venv /opt/project
COPY --chown=airflow:root pyproject.toml uv.lock /opt/project/
USER airflow
WORKDIR /opt/project
RUN pip install --no-cache-dir uv==0.11.21
RUN uv sync --frozen --no-dev
RUN pip install --no-cache-dir apache-airflow-providers-amazon
WORKDIR /workspace
