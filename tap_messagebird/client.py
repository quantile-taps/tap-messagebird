"""REST client handling, including MessagebirdStream base class."""

from __future__ import annotations
from singer_sdk import metrics
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Optional
from urllib.parse import parse_qsl, urlparse

import requests
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseHATEOASPaginator, BaseOffsetPaginator
from singer_sdk.streams import RESTStream

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class MessagebirdHATEOASPaginator(BaseHATEOASPaginator):
    def get_next_url(self, response):
        return response.json()["links"]["next"]


class MessagebirdOffsetPaginator(BaseOffsetPaginator):
    def __init__(self, start_value: int, page_size: int, starting_replication_value: str | None = None):
        """Initialize paginator.
        
        Args:
            start_value: Starting offset value
            page_size: Number of records per page
            starting_replication_value: ISO8601 datetime string of the bookmark to stop at
        """
        super().__init__(start_value, page_size)
        self.starting_replication_value = starting_replication_value
    
    def get_next(self, response) -> int | None:
        response_json = response.json()
        return response_json["offset"] + response_json["limit"]

    def has_more(self, response) -> bool:
        response_json = response.json()
        items = response_json["items"]

        # Stop fetching if we've reached records older than our bookmark
        if items and self.starting_replication_value:
            last_item = items[-1]
            last_updated_datetime_str = last_item.get("updatedDatetime")
            if last_updated_datetime_str:
                # Parse to date only to avoid timezone issues
                last_updated_date = datetime.fromisoformat(last_updated_datetime_str.replace('Z', '+00:00')).date()
                starting_date = datetime.fromisoformat(self.starting_replication_value.replace('Z', '+00:00')).date()
                
                # If the last item in this page is older than our starting bookmark,
                # we've gone back far enough and can stop
                if last_updated_date < starting_date:
                    return False

        offset = response_json["offset"]
        count = response_json["count"]
        total_count = response_json["totalCount"]
        return count + offset <= total_count


class MessagebirdStream(RESTStream):
    """Messagebird stream class."""

    url_base = "https://rest.messagebird.com"

    records_jsonpath = "$.items[*]"
    next_page_token_jsonpath = "$.next_page"  # Or override `get_next_page_token`.
    _LOG_REQUEST_METRIC_URLS: bool = True

    def get_new_paginator(self, context: dict | None) -> BaseOffsetPaginator:
        return MessagebirdHATEOASPaginator()

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        headers["Authorization"] = f"AccessKey {self.config['api_key']}"
        return headers

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        if next_page_token:
            return dict(parse_qsl(next_page_token.query))
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key
        return params

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records."""
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())

    def response_error_message(self, response: requests.Response) -> str:
        """Build error message for invalid http statuses.

        WARNING - Override this method when the URL path may contain secrets or PII

        Args:
            response: A `requests.Response`_ object.

        Returns:
            str: The error message
        """
        full_path = urlparse(response.url).path or self.path
        error_content = ""
        if 400 <= response.status_code < 500:
            error_type = "Client"
            error_content = response.json()
        else:
            error_type = "Server"

        return (
            f"{response.status_code} {error_type} Error: "
            f"{response.reason} for path: {full_path} . "
            f"{error_content=}"
        )
    
    def request_records(self, context: dict | None) -> t.Iterable[dict]:
        """Request records from REST endpoint(s), returning response records.

        If pagination is detected, pages will be recursed automatically.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            An item for every record in the response.
        """
        paginator = self.get_new_paginator(context=context)
        decorated_request = self.request_decorator(self._request)
        pages = 0

        with metrics.http_request_counter(self.name, self.path) as request_counter:
            request_counter.context = context

            while not paginator.finished:
                prepared_request = self.prepare_request(
                    context,
                    next_page_token=paginator.current_value,
                )
                resp = decorated_request(prepared_request, context)
                request_counter.increment()
                self.update_sync_costs(prepared_request, resp, context)
                records = iter(self.parse_response(resp))
                try:
                    first_record = next(records)
                except StopIteration:
                    self.logger.info(
                        "Pagination stopped after %d pages because no records were "
                        "found in the last response",
                        pages,
                    )
                    break
                yield first_record
                yield from records
                pages += 1

                paginator.advance(resp)
