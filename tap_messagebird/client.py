"""REST client handling, including MessagebirdStream base class."""

from __future__ import annotations

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
    def get_next(self, response) -> int | None:
        response_json = response.json()
        return response_json["offset"] + response_json["limit"]

    def has_more(self, response) -> bool:
        response_json = response.json()
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

    def get_new_paginator(self):
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
