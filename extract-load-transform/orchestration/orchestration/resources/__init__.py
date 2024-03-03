from dagster import EnvVar
from dagster_airbyte import AirbyteResource

airbyte_instance = AirbyteResource(
    host=EnvVar("HOST"),
    port="8000",
    username=EnvVar("AIRBYTE_USERNAME"),
    password=EnvVar("AIRBYTE_PASSWORD"),
)
