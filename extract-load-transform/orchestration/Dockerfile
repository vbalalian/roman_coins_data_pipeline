FROM python:3.10-slim

RUN mkdir -p /opt/dagster/dagster_home /opt/dagster/app

RUN pip install dagster dagster-webserver dagster-postgres 
RUN pip install dagster-airbyte dagster-duckdb minio duckdb

# Copy your code and workspace to /opt/dagster/app
COPY . /opt/dagster/app/

ENV DAGSTER_HOME=/opt/dagster/dagster_home/
ENV PYTHONPATH="/opt/dagster/app:${PYTHONPATH}"

# Copy dagster instance YAML to $DAGSTER_HOME
COPY dagster.yaml workspace.yaml /opt/dagster/dagster_home/

WORKDIR /opt/dagster/app