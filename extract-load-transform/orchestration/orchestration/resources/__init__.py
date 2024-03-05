from dagster import EnvVar, ConfigurableResource
from dagster_airbyte import AirbyteResource
from dagster_duckdb import DuckDBResource
from minio import Minio

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