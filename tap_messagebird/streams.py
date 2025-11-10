"""Stream type classes for tap-messagebird."""

from typing import Any, Dict, Optional

from tap_messagebird.client import MessagebirdOffsetPaginator, MessagebirdStream
from singer_sdk import typing as th


class ConversationsStream(MessagebirdStream):
    """Conversations stream."""

    url_base = "https://conversations.messagebird.com/v1"
    name = "messagebird__conversations"
    path = "/conversations"
    primary_keys = ["id"]
    replication_key = "updatedDatetime"
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

    def get_new_paginator(self, context: Optional[dict] = None):
        starting_replication_value = self.get_starting_replication_key_value(context=None)
        return MessagebirdOffsetPaginator(
            start_value=0, 
            page_size=self.limit,
            starting_replication_value=starting_replication_value
        )

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

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        if record["status"] == "deleted":
            return None

        return {
            "conversation_id": record["id"],
            "replication_value": self.get_starting_replication_key_value(context=None),
        }


class MessagesStream(MessagebirdStream):
    """Messages stream."""

    url_base = "https://conversations.messagebird.com/v1"
    name = "messagebird__messages"
    path = "/conversations/{conversation_id}/messages"
    parent_stream_type = ConversationsStream
    primary_keys = ["id"]
    limit = 20
    state_partitioning_keys = []

    def get_new_paginator(self, context: Optional[dict] = None):
        return MessagebirdOffsetPaginator(
            start_value=0, 
            page_size=self.limit,
            starting_replication_value=context.get("replication_value")
        )

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

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("conversation_id", th.StringType),
        th.Property("platform", th.StringType),
        th.Property("to", th.StringType),
        th.Property("from", th.StringType),
        th.Property("channelId", th.StringType),
        th.Property("type", th.StringType),
        th.Property(
            "content",
            th.ObjectType(
                th.Property("text", th.StringType),
                th.Property(
                    "hsm",
                    th.ObjectType(
                        th.Property("namespace", th.StringType),
                        th.Property("templateName", th.StringType),
                        th.Property(
                            "language",
                            th.ObjectType(
                                th.Property("policy", th.StringType),
                                th.Property("code", th.StringType),
                            ),
                        ),
                        th.Property(
                            "components",
                            th.ArrayType(
                                th.ObjectType(
                                    th.Property("type", th.StringType),
                                    th.Property("sub_type", th.StringType),
                                    th.Property(
                                        "parameters",
                                        th.ArrayType(
                                            th.ObjectType(
                                                th.Property("type", th.StringType),
                                                th.Property("text", th.StringType),
                                            )
                                        ),
                                    ),
                                )
                            ),
                        ),
                    ),
                ),
            ),
        ),
        th.Property("direction", th.StringType),
        th.Property("status", th.StringType),
        th.Property(
            "error",
            th.ObjectType(
                th.Property("code", th.IntegerType),
                th.Property("description", th.StringType),
            ),
        ),
        th.Property("createdDatetime", th.DateTimeType),
        th.Property("updatedDatetime", th.DateTimeType),
    ).to_dict()
