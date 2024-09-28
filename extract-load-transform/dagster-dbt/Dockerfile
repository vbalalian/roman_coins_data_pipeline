FROM python:3.10-slim

RUN mkdir -p /opt/dagster-dbt/dagster_home /opt/dagster-dbt/app

RUN pip install dagster dagster-webserver dagster-postgres 
RUN pip install dagster-airbyte dagster-duckdb minio duckdb
RUN pip install dbt-duckdb dagster-dbt

# Copy your code and workspace to /opt/dagster/app
COPY . /opt/dagster-dbt/app/

ENV DAGSTER_HOME=/opt/dagster-dbt/dagster_home/
ENV PYTHONPATH="/opt/dagster-dbt/app:${PYTHONPATH}"
ENV DAGSTER_DBT_PARSE_PROJECT_ON_LOAD=1

# Copy dagster instance YAML to $DAGSTER_HOME
COPY dagster.yaml workspace.yaml /opt/dagster-dbt/dagster_home/

WORKDIR /opt/dagster-dbt/app