from dagster import EnvVar, ConfigurableResource
from dagster_airbyte import AirbyteResource
from dagster_duckdb import DuckDBResource
from dagster_dbt import DbtCliResource
from minio import Minio
import os
from pathlib import Path

airbyte_instance = AirbyteResource(
    host=EnvVar("HOST"),
    port="8000",
    username=EnvVar("AIRBYTE_USERNAME"),
    password=EnvVar("AIRBYTE_PASSWORD"),
    request_max_retries=6,
    request_retry_delay=5
)

database_resource = DuckDBResource(
    database=EnvVar("DUCKDB_DATABASE")
)

class MinioResource(ConfigurableResource):

    endpoint:str
    access_key:str
    secret_key:str
    session_token:str
    secure:bool
    region:str
    
    def client(self):
        return Minio(self.endpoint, self.access_key, self.secret_key, 
                     self.session_token, self.secure, self.region)
    
minio_resource = MinioResource(
    endpoint=EnvVar("MINIO_ENDPOINT"),
    access_key=EnvVar("MINIO_USER"),
    secret_key=EnvVar("MINIO_PASSWORD"),
    session_token="",
    secure=False,
    region="")

dbt_project_dir = Path(__file__).joinpath("..", "..", "..", "dbt_project").resolve()

dbt_resource = DbtCliResource(project_dir=os.fspath(dbt_project_dir))

# If DAGSTER_DBT_PARSE_PROJECT_ON_LOAD is set, a manifest will be created at run time.
# Otherwise, we expect a manifest to be present in the project's target directory.
if os.getenv("DAGSTER_DBT_PARSE_PROJECT_ON_LOAD"):
    dbt_manifest_path = (
        dbt_resource.cli(
            ["--quiet", "parse"],
            target_path=Path("target"),
        )
        .wait()
        .target_path.joinpath("manifest.json")
    )
else:
    dbt_manifest_path = dbt_project_dir.joinpath("target", "manifest.json")