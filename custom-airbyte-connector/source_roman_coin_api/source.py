#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
import os
from datetime import datetime, timedelta
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream, IncrementalMixin
from airbyte_cdk.sources.streams.http import HttpStream
# from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator # Authentication not currently implemented

url_base = f'{os.getenv("HOST", "http://host.docker.internal")}:8010/v1/'

# Incremental stream
class RomanCoinApiStream(HttpStream, IncrementalMixin):

    # Save the state every 100 records
    state_checkpoint_interval = 100

    url_base = url_base
    cursor_field = "modified"
    primary_key = "id"

    def __init__(self, config:Mapping[str, Any], **kwargs):
        super().__init__()
        self.start_date = datetime.strptime(config["start_date"], '%Y-%m-%d')
        self._cursor_value = datetime.strptime(self.start_date, "%Y-%m-%dT%H:%M:%S.%f") if isinstance(self.start_date, str) else self.start_date
    
    @property
    def state(self) -> Mapping[str, Any]:
        return {self.cursor_field: self._cursor_value.strftime("%Y-%m-%dT%H:%M:%S.%f")}
    
    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._cursor_value = datetime.strptime(value[self.cursor_field], "%Y-%m-%dT%H:%M:%S.%f")

    def path(self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> str:
        return "coins/"
    
    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        json_response = response.json()
        pagination_info = json_response.get("pagination", {})
        current_page = pagination_info.get("current_page")
        total_pages = pagination_info.get("total_pages")

        if current_page and total_pages and current_page < total_pages:
            return {"page": current_page + 1}
        else:
            return None

    def parse_response(self, response: requests.Response, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> Iterable[Mapping]:
        json_response = response.json()
        records = json_response.get('data', []) 
        for record in records:
            yield record

    def read_records(self, *args, **kwargs) -> Iterable[Mapping[str, Any]]:
        for record in super().read_records(*args, **kwargs):
            record_cursor_value = datetime.strptime(record[self.cursor_field], "%Y-%m-%dT%H:%M:%S.%f")
            if record_cursor_value > self._cursor_value:
                yield record
                self._cursor_value = record_cursor_value

    def request_params(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> MutableMapping[str, Any]:
        params = {
            "page": next_page_token["page"] if next_page_token else 1,
            "page_size": 100,
            "sort_by": "modified"
        }
        if stream_state:
            last_synced_time = datetime.strptime(stream_state[self.cursor_field], "%Y-%m-%dT%H:%M:%S.%f")
            next_start_time = last_synced_time + timedelta(microseconds=1)
            params["start_modified"] = next_start_time.strftime("%Y-%m-%dT%H:%M:%S.%f")
        else:
            params["start_modified"] = self.start_date.strftime("%Y-%m-%dT%H:%M:%S.%f")        
        return params

# Source
class SourceRomanCoinApi(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            response = requests.get(url_base)
            response.raise_for_status()
            return True, None
        except requests.exceptions.RequestException as e:
            return False, f"Connection check failed: {e}"        

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        return [RomanCoinApiStream(config=config)]