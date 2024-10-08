from dagster import RunRequest, SensorResult, sensor, DefaultSensorStatus, run_status_sensor, DagsterRunStatus, RunStatusSensorContext
from ..jobs import extract_job, loading_job, transform_job
from ..resources import MinioResource
import os
import requests

@sensor(
    job=extract_job,
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=300,
    description="Triggers extract_job when modified records are detected in the api."
)
def api_sensor(context):
    previous_state = context.cursor if context.cursor else None
    run_requests = []

    endpoint = f'http://{os.getenv("HOST")}:8010/v1/coins/?sort_by=modified&desc=true'
    response = requests.get(endpoint)
    
    try:
        current_state = response.json()["data"][0]["modified"]
        if current_state != previous_state:
            run_requests.append(RunRequest())
    except IndexError as e:
        # Assuming no data has been added yet
        current_state = None

    return SensorResult(
        run_requests=run_requests,
        cursor=current_state
    )

@sensor(
    job=loading_job,
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=60,
    description="Triggers loading_job when new files are detected in storage."
)
def extracted_file_sensor(context, storage:MinioResource):
    previous_state = context.cursor if context.cursor else None
    run_requests = []

    client = storage.client()
    objects = client.list_objects(os.getenv("MINIO_BUCKET_NAME"), recursive=True)
    mod_times = [str(obj.last_modified) for obj in objects]

    current_state = max(mod_times, default=None)

    if current_state != previous_state:
        run_requests.append(RunRequest())

    return SensorResult(
        run_requests=run_requests,
        cursor=current_state
    )

@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    monitored_jobs=[loading_job],
    request_job=transform_job,
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=60
)
def loaded_data_sensor(context: RunStatusSensorContext):
    yield RunRequest()