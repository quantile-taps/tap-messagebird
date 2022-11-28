"""Stream type classes for tap-messagebird."""

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_messagebird.client import MessagebirdStream

# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")
# TODO: - Override `UsersStream` and `GroupsStream` with your own stream definition.
#       - Copy-paste as many times as needed to create multiple stream types.

class MessagebirdConversations(MessagebirdStream):
    """Conversations API has some differeences from the main API"""
    url_base = "https://conversations.messagebird.com/v1"

class ConversationsStream(MessagebirdConversations):
    """Conversations stream"""
    name = "conversation"
    path = "/conversations"
    primary_keys = ["id"]
    replication_key = None
    # Optionally, you may also use `schema_filepath` in place of `schema`:
    schema_filepath = SCHEMAS_DIR / "conversation.json"
    
    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "_sdc_conversations_id": record["id"],
        }


class MessagesStream(MessagebirdConversations):
    """Messages stream"""
    name = "message"
    path = "/conversations/{_sdc_conversations_id}/messages"
    primary_keys = ["id"]
    replication_key = None
    # Optionally, you may also use `schema_filepath` in place of `schema`:
    schema_filepath = SCHEMAS_DIR / "message.json"
    parent_stream_type = ConversationsStream
