from dagster import Definitions
from dagster_airbyte import load_assets_from_airbyte_instance
from .jobs import airbyte_sync_job
from .schedules import airbyte_sync_schedule
from .resources import airbyte_instance

airbyte_assets = load_assets_from_airbyte_instance(airbyte_instance)

all_jobs = [airbyte_sync_job]
all_schedules = [airbyte_sync_schedule]

defs = Definitions(
    assets=[airbyte_assets],
    jobs=all_jobs,
    schedules=all_schedules
)
