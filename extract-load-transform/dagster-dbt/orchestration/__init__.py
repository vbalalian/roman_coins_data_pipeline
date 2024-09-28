from dagster import Definitions, load_assets_from_modules
from dagster_airbyte import load_assets_from_airbyte_instance
from .assets import load_assets, dbt_assets
from .resources import database_resource, airbyte_instance, minio_resource, dbt_resource
from .jobs import extract_job, loading_job, transform_job
from .sensors import api_sensor, extracted_file_sensor, loaded_data_sensor

airbyte_assets = load_assets_from_airbyte_instance(airbyte_instance)
loading_assets = load_assets_from_modules(modules=[load_assets], group_name="load")
transform_assets = load_assets_from_modules(modules=[dbt_assets], group_name="transform")

all_jobs = [extract_job, loading_job, transform_job]
all_sensors = [api_sensor, extracted_file_sensor, loaded_data_sensor]


defs = Definitions(
    assets=[airbyte_assets, *loading_assets, *transform_assets],
    resources={
        "database":database_resource,
        "storage":minio_resource,
        "dbt":dbt_resource
    },
    jobs=all_jobs,
    sensors=all_sensors
)
