import requests
import json
import base64
import os

class airbyte_instance(object):

    def __init__(self, 
                 api_host:str, 
                 config_api_host: str, 
                 username:str, 
                 password:str,
                 source_name:str,
                 destination_name:str,
                 connection_name:str,
                 source_definition:dict = None, 
                 destination_config:dict = None):
        self.api_host = api_host
        self.config_api_host = config_api_host
        self.source_name = source_name
        self.destination_name = destination_name
        self.connection_name = connection_name
        self.source_definition = source_definition
        self.destination_config = destination_config
        self.authorization = base64.b64encode(f"{username}:{password}".encode()).decode()
        self.headers = {'accept': 'application/json',
                        'authorization': f'Basic {self.authorization}',
                        'content-type': 'application/json'}
        self.workspaces = self.get_workspaces()

    def status(self):
        url = self.api_host + "/health"
        response = requests.get(url, headers=self.headers)
        try:
            return response.text
        except:
            return None

    def get_workspaces(self):
        url = self.api_host + "/v1/workspaces?includeDeleted=false&limit=20&offset=0"
        response = requests.get(url, headers=self.headers)
        return json.loads(response.text)["data"]
    
    def workspaceId(self):
        return self.workspaces[0]["workspaceId"]
    
    def activeSources(self):
        url = self.api_host + "/v1/sources"
        response = requests.get(url, headers=self.headers)
        try:
            return json.loads(response.text)["data"]
        except:
            return None
    
    def activeDestinations(self):
        url = self.api_host + "/v1/destinations"
        response = requests.get(url, headers=self.headers)
        try:
            return json.loads(response.text)["data"]
        except:
            return None
    
    def activeConnections(self):
        url = self.api_host + "/v1/connections"
        response = requests.get(url, headers=self.headers)
        try:
            return json.loads(response.text)["data"]
        except:
            return None

    def instanceSources(self):
        url = self.config_api_host + '/v1/source_definitions/list'
        response = requests.post(url, headers=self.headers)
        data = json.loads(response.text)
        return data["sourceDefinitions"]
    
    def sourceId(self):
        for source in self.activeSources():
            if source["name"] == self.source_name:
                return source["sourceId"]
    
    def destinationId(self):
        for destination in self.activeDestinations():
            if destination["name"] == self.destination_name:
                return destination["destinationId"]
            
    def connectionId(self):
        for connection in self.activeConnections():
            if connection["name"] == self.connection_name:
                return connection["connectionId"]
            
    def sourceDefinitionId(self):
        if self.source_definition:
            for source in self.instanceSources():
                if source["name"] == self.source_definition["name"]:
                    return source["sourceDefinitionId"]
        return None

    def sourceDetails(self):
        try:
            url = self.api_host + "/v1/sources/" + self.sourceId()
            response = requests.get(url, headers=self.headers)
            data = json.loads(response.text)
            return data
        except:
            print("Error returning source details")
    
    def add_custom_source(self):
        '''Adds custom source connector to Airbyte instance. 
           Returns corresponding source connector ID.'''
        if self.source_definition:
            url = self.config_api_host + '/v1/source_definitions/create_custom'
            create_source_json = {"workspaceId": self.workspaceId(), "sourceDefinition": self.source_definition}
            response = requests.post(url, json.dumps(create_source_json), headers=self.headers)
            if response.status_code != 200:
                print(f"Failed to add custom source: {response.text}")
            return response.text
        else:
            print("Missing source_definition")
    
    def create_source(self, start_date:str):
        '''Creates a source (in the workspace)'''
        url = self.api_host + '/v1/sources'
        payload = {"configuration": {"start_date":start_date},
                   "name": self.source_name,
                   "definitionId":self.sourceDefinitionId(),
                   "workspaceId": self.workspaceId()
        }
        response = requests.post(url, json.dumps(payload), headers=self.headers)
        if response.status_code != 200:
            print("Error:", response.text)
        return response.text
    
    def delete_source(self, sourceId:str):
        '''Deletes a source (from the workspace)'''
        url = self.api_host + f'/v1/sources/{sourceId}'
        response = requests.delete(url, headers=self.headers)
        return response.text

    def create_destination(self):
        '''Creates a destination (in the workspace)'''
        url = self.api_host + '/v1/destinations'
        payload = { 
            "configuration": self.destination_config,
            "name": self.destination_name,
            "workspaceId": self.workspaceId()
            }
        response = requests.post(url, json.dumps(payload), headers=self.headers)
        if response.status_code != 200:
            print("Error:", response.text)
        return response.text
    
    def delete_destination(self, destinationId:str):
        '''Deletes a destination (from the workspace)'''
        url = self.api_host + f'/v1/destinations/{destinationId}'
        response = requests.delete(url, headers=self.headers)
        return response.text
    
    def create_connection(self):
        '''Creates a connection'''
        url = self.api_host + '/v1/connections'
        payload = {
            "configurations": { "streams": [
                    {
                        "name": self.streamName(),
                        "syncMode": "incremental_append",
                        "cursorField": ["modified"],
                        "primaryKey": [["id"]]
                    }
                ] },
            "schedule": { "scheduleType": "manual" },
            "name": self.connection_name,
            "sourceId": self.sourceId(),
            "destinationId": self.destinationId()
        }
        print(f"create_connection url:{url}")
        print(f"create_connection payload: {payload}")
        response = requests.post(url, json.dumps(payload), headers=self.headers)
        if response.status_code != 200:
            print("Error:", response.text)
        return response.text
    
    def delete_connection(self, connectionId:str):
        '''Deletes a connection (from the workspace)'''
        url = self.api_host + f'/v1/connections/{connectionId}'
        response = requests.delete(url, headers=self.headers)
        return response.text
    
    def sync_connection(self):
        '''Syncs the connection'''
        url = self.api_host + '/v1/jobs'
        payload = {
            "jobType": "sync",
            "connectionId": self.connectionId()
        }
        response = requests.post(url, json.dumps(payload), headers=self.headers)
        if response.status_code != 200:
            print("Error:", response.text)
        return response.text
    
    def reset_connection(self):
        '''Resets the connection'''
        url = self.api_host + '/v1/jobs'
        payload = {
            "jobType": "reset",
            "connectionId": self.connectionId()
        }
        response = requests.post(url, json.dumps(payload), headers=self.headers)
        if response.status_code != 200:
            print("Error:", response.text)
        return response.text
    
    def streams(self):
        url = self.api_host + f"/v1/streams?sourceId={self.sourceId()}&destinationId={self.destinationId()}&ignoreCache=True"
        response = requests.get(url, headers=self.headers)
        try:
            return json.loads(response.text)
        except:
            return None
        
    def streamName(self):
        try:
            return self.streams()[0]["streamName"]
        except KeyError as e:
            print(f"Error retrieving streamName: {e}")
        
    def jobs(self):
        url = self.api_host + "/v1/jobs?includeDeleted=false&limit=20&offset=0"
        response = requests.get(url, headers=self.headers)
        try:
            return json.loads(response.text)
        except:
            return None
    
    def job_status(self, jobId:str):
        url = self.api_host + "/v1/jobs/" + jobId
        response = requests.get(url, headers=self.headers)
        data = json.loads(response.text)
        return data
    
    def cancel_job(self, jobId:str):
        '''Cancels a job'''
        url = self.api_host + f'/v1/jobs/{jobId}'
        response = requests.delete(url, headers=self.headers)
        return response.text

if __name__ == '__main__':
    
    custom_source_definition = {
        "name": "Roman Coins API",
        "documentationUrl": "",
        "dockerImageTag": "latest",
        "dockerRepository": "airbyte/source-roman-coins-api"
        }
    
    destination_config = {
        "destinationType": "s3",
        "s3_bucket_region": "us-west-2",
        "format": {"format_type": "CSV", "flattening": "Root level flattening", 
                "compression": { "compression_type": "No Compression" }},
        "access_key_id": os.getenv("MINIO_NEW_USER", "roman-coins-user"),
        "secret_access_key": os.getenv("MINIO_NEW_USER_PASSWORD", "nonprodpasswd"),
        "s3_bucket_name": os.getenv("MINIO_BUCKET_NAME", "roman-coins"),
        "s3_bucket_path": "data",
        "s3_endpoint": f'{os.getenv("AIRBYTE_HOST", "http://localhost")}:9000'
        }

    api_host = f'{os.getenv("AIRBYTE_HOST", "http://localhost")}:8006'
    config_api_host = f'{os.getenv("AIRBYTE_HOST", "http://localhost")}:8000/api'

    airbyte = airbyte_instance(api_host=api_host, 
                            config_api_host=config_api_host, 
                            username=os.getenv("AIRBYTE_USERNAME", "airbyte"), 
                            password=os.getenv("AIRBYTE_PASSWORD", "password"), 
                            source_name='Roman Coins API', 
                            destination_name='MinIO',
                            connection_name='Extract',
                            source_definition=custom_source_definition,
                            destination_config=destination_config)
    
    print(f"AIRBYTE STATUS: {airbyte.status()}")
    
    # Add custom source connector to airbyte instance if it doesn't already exist
    custom_source_exists = any(source['name'] == 'Roman Coins API' for source in (airbyte.instanceSources() or []))
    if not custom_source_exists:
        print('ADDING CUSTOM SOURCE...')
        print(airbyte.add_custom_source())
    else:
        print('CUSTOM SOURCE CONNECTOR ALREADY EXISTS.')

    # Create airbyte source if it doesn't already exist
    active_source_exists = any(source['name'] == 'Roman Coins API' for source in (airbyte.activeSources() or []))
    if not active_source_exists:
        print('CREATING SOURCE...')
        print(airbyte.create_source(start_date=os.getenv("CONNECTOR_START_DATE", "2024-01-01")))
    else:
        print('SOURCE ALREADY EXISTS.')

    # Create airbyte destination if it doesn't already exist
    active_destination_exists = any(source['name'] == 'MinIO' for source in (airbyte.activeDestinations() or []))
    if not active_destination_exists:
        print('CREATING DESTINATION...')
        print(airbyte.create_destination())
    else:
        print('DESTINATION ALREADY EXISTS.')

    # Create airbyte connection if it doesn't already exist
    active_connection_exists = any(source['name'] == 'Extract' for source in (airbyte.activeConnections() or []))
    if not active_connection_exists:
        print('CREATING CONNECTION...')
        print(airbyte.create_connection())
    else:
        print('CONNECTION ALREADY EXISTS.')
