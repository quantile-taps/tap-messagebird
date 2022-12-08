"""Stream type classes for tap-messagebird."""

from typing import Any, Dict, Optional

from tap_messagebird.client import MessagebirdOffsetPaginator, MessagebirdStream
from singer_sdk import typing as th


class ConversationsStream(MessagebirdStream):
    """Conversations stream."""

    url_base = "https://conversations.messagebird.com/v1"
    name = "messagebird__conversation"
    path = "/conversations"
    primary_keys = ["id"]
    replication_key = None
    limit = 20

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("status", th.StringType),
        th.Property(
            "contact",
            th.ObjectType(
                th.Property("id", th.StringType),
                th.Property("msisdn", th.IntegerType),
                th.Property("firstName", th.StringType),
                th.Property("lastName", th.StringType),
            ),
        ),
        th.Property("createdDatetime", th.DateTimeType),
        th.Property("updatedDatetime", th.DateTimeType),
        th.Property("lastReceivedDatetime", th.DateTimeType),
    ).to_dict()

    def get_new_paginator(self):
        return MessagebirdOffsetPaginator(start_value=0, page_size=self.limit)

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.
        Overrode as we have a different paginator for this api
        """
        params = {}
        if next_page_token:
            params["offset"] = next_page_token
        params["status"] = "all"
        params["limit"] = self.limit
        return params
