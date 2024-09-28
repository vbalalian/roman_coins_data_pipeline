from dagster import asset
from dagster_duckdb import DuckDBResource
from ..resources import MinioResource
import os
import tempfile
import duckdb

@asset(
        deps=["roman_coin_api_stream"]
)
def processed_files_set(context, database: DuckDBResource) -> set:
    """
    Fetches the set of filenames that have been processed and stored in the 
    database to avoid reprocessing.
    """
    query = "SELECT filename FROM processed_files;"

    with database.get_connection() as conn:
        with conn.cursor() as cursor: # Cursor needed for concurrency
            try:
                result = cursor.execute(query).fetchall()
                processed_files_set = set([row[0] for row in result])
            except duckdb.CatalogException:
                # Assuming the table doesn't exist, create it and return an empty set
                cursor.execute("CREATE TABLE IF NOT EXISTS processed_files (filename VARCHAR);")
                processed_files_set = set()
    
    return processed_files_set

@asset()
def unprocessed_files_list(storage: MinioResource, processed_files_set:set) -> list:
    """
    Returns a list of files in minio which haven't been processed yet
    """
    unprocessed_files_list = []
    client = storage.client()
    objects = client.list_objects(os.getenv("MINIO_BUCKET_NAME"), recursive=True)

    for obj in objects:
        if obj.object_name not in processed_files_set:
            unprocessed_files_list.append(obj.object_name)

    return unprocessed_files_list

@asset
def raw_roman_coins(database:DuckDBResource, storage:MinioResource, unprocessed_files_list:list) -> None:
    """
    The raw roman coins dataset, loaded from incremental csv files stored in 
    minio into a DuckDB database.
    """
    client = storage.client()
    table_name = os.getenv("LOADING_TABLE")

    with database.get_connection() as conn:
        with conn.cursor() as cursor: # Cursor needed for concurrency
            # Download the file to a temporary location
            for file_name in unprocessed_files_list:
                with tempfile.NamedTemporaryFile(delete=True) as temp_file:
                    client.fget_object(os.getenv("MINIO_BUCKET_NAME"), file_name, temp_file.name)
                    print(f"Loading {file_name.split('/')[2]}")
        
                    # Load data from the temporary file into DuckDB
                    try:
                        cursor.execute(f"COPY {table_name} FROM '{temp_file.name}' (FORMAT 'csv', HEADER);")
                    except duckdb.CatalogException:
                        cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS FROM read_csv('{temp_file.name}', AUTO_DETECT=TRUE);")

                    # Mark the file as processed to avoid reprocessing
                    cursor.execute("INSERT INTO processed_files VALUES (?);", (file_name,))
