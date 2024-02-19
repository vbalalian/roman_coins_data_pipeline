from dagster import ScheduleDefinition, DefaultScheduleStatus
from ..jobs import airbyte_sync_job

airbyte_sync_schedule = ScheduleDefinition(
    job=airbyte_sync_job,
    cron_schedule="*/30 * * * *",
    default_status=DefaultScheduleStatus.RUNNING
)