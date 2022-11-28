"""Stream type classes for tap-messagebird."""

from pathlib import Path
from typing import Any, Dict, Optional

from tap_messagebird.client import MessagebirdStream

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class MessagebirdConversations(MessagebirdStream):
    """Conversations API has some differeences from the main API."""

    url_base = "https://conversations.messagebird.com/v1"


class ConversationsStream(MessagebirdConversations):
    """Conversations stream."""

    name = "conversation"
    path = "/conversations"
    primary_keys = ["id"]
    replication_key = None
    # Optionally, you may also use `schema_filepath` in place of `schema`:
    schema_filepath = SCHEMAS_DIR / "conversation.json"


class MessagesStream(MessagebirdStream):
    """Messages stream."""

    name = "message"
    path = "/messages"
    primary_keys = ["id"]
    replication_key = None
    # Optionally, you may also use `schema_filepath` in place of `schema`:
    schema_filepath = SCHEMAS_DIR / "message.json"

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params = super().get_url_params(
            context=context, next_page_token=next_page_token
        )
        params["from"] = self.config["start_date"]
        return params
