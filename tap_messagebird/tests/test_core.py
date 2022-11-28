"""Tests standard tap features using the built-in SDK tests library."""

import os

from singer_sdk.testing import get_standard_tap_tests

from tap_messagebird.tap import TapMessagebird

SAMPLE_CONFIG = {
    "api_key": os.getenv("TAP_MESSAGEBIRD_API_KEY"),
}


# Run standard built-in tap tests from the SDK:
def test_standard_tap_tests():
    """Run standard tap tests from the SDK."""
    tests = get_standard_tap_tests(TapMessagebird, config=SAMPLE_CONFIG)
    for test in tests:
        test()
