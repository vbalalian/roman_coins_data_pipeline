from dagster import Definitions, load_assets_from_modules
from dagster_airbyte import load_assets_from_airbyte_instance
from .assets import raw_roman_coins
from .resources import database_resource, airbyte_instance, minio_resource
from .jobs import airbyte_sync_job, loading_job
from .sensors import api_sensor, extracted_file_sensor

airbyte_assets = load_assets_from_airbyte_instance(airbyte_instance)
loading_assets = load_assets_from_modules(modules=[raw_roman_coins], group_name="load")

all_jobs = [airbyte_sync_job, loading_job]
all_sensors = [api_sensor, extracted_file_sensor]


defs = Definitions(
    assets=[airbyte_assets, *loading_assets],
    resources={
        "database":database_resource,
        "storage":minio_resource
    },
    jobs=all_jobs,
    sensors=all_sensors
)
