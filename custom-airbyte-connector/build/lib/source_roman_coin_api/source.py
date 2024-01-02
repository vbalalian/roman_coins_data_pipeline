#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from datetime import datetime, timedelta
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
# from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator # Authentication not currently implemented

url_base = "http://host.docker.internal:8010/v1/"

# Basic full refresh stream
class RomanCoinApiStream(HttpStream):

    url_base = url_base
    cursor_field = "modified"
    primary_key = "id"

    def __init__(self, config:Mapping[str, Any], start_date:datetime, **kwargs):
        super().__init__()
        self.start_date = start_date
        self._cursor_value = datetime.min

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, any]:
        latest_state = max(latest_record[self.cursor_field], current_stream_state.get(self.cursor_field, self.start_date.strftime("%Y-%m-%d")))
        return {self.cursor_field: latest_state}
    
    def _chunk_date_range(self, start_date: datetime) -> List[Mapping[str, Any]]:
        """
        Returns a list of each day between the start date and now.
        The return value is a list of dicts {'date': date_string}.
        """
        dates = []
        while start_date < datetime.now():
            dates.append({self.cursor_field: start_date.strftime('%Y-%m-%d')})
            start_date += timedelta(days=1)
        return dates
    
    def stream_slices(self, sync_mode, cursor_field: List[str] = None, stream_state: Mapping[str, Any] = None) -> Iterable[Optional[Mapping[str, Any]]]:
        start_date = datetime.strptime(stream_state[self.cursor_field], '%Y-%m-%d') if stream_state and self.cursor_field in stream_state else self.start_date
        return self._chunk_date_range(start_date)

    def path(
        self, 
        stream_state: Mapping[str, Any] = None, 
        stream_slice: Mapping[str, Any] = None, 
        next_page_token: Mapping[str, Any] = None
    ) -> str:
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
        records = json_response.get('data', [])  # Extract records from 'data' key

        for record in records:
            record_modified = datetime.fromisoformat(record[self.cursor_field])
            self._cursor_value = max(self._cursor_value, record_modified)          
            yield record

        if self._cursor_value:
            self.state = {self.cursor_field: self._cursor_value.isoformat()}
    
    def request_params(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> MutableMapping[str, Any]:
        params = {
            "page": next_page_token["page"] if next_page_token else 1,
            "page_size": 10,
            "sort_by": "modified",
            "desc": True
        }
        if stream_state and self.cursor_field in stream_state:
            params["start_modified"] = stream_state[self.cursor_field]
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
        start_date = datetime.strptime(config["start_date"], '%Y-%m-%d')
        return [RomanCoinApiStream(config=config, start_date=start_date)]