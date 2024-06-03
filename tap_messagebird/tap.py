"""Messagebird tap class."""

from typing import List

import pendulum
from singer_sdk import Stream, Tap
from singer_sdk import typing as th

from tap_messagebird.streams import ConversationsStream, MessagesStream

STREAM_TYPES = [
    ConversationsStream,
    MessagesStream,
]


class TapMessagebird(Tap):
    """Messagebird tap class."""

    name = "tap-messagebird"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_key",
            th.StringType,
            required=True,
            secret=True,
            description=(
                "The token to authenticate against the API service. "
                "Test keys are not supported for Conversations see "
                "https://support.messagebird.com/hc/en-us/articles/360000670709-What-is-the-difference-between-a-live-key-and-a-test-key-"  # noqa: E501
            ),
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            default=pendulum.now().subtract(years=3).set(tz="UTC").to_iso8601_string(),
            description=(
                "When to pull records starting at what date. "
                "ISO8601 format of date, defaults to 3 years ago."
            ),
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]


if __name__ == "__main__":
    TapMessagebird.cli()
