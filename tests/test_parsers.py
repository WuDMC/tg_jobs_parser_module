import pytest
from telegram_helper.channel_parser import ChannelParser
from telegram_helper.message_parser import MessageParser

@pytest.fixture
def channel_parser():
    return ChannelParser()

@pytest.fixture
def message_parser():
    return MessageParser()

def test_parse_channel_metadata(channel_parser):
    # Add test logic here
    pass

def test_parse_channel_messages(message_parser):
    # Add test logic here
    pass
