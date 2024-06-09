from datetime import datetime
from configs.telegram_config import TelegramConfig
from telegram_helper.telegram_client import TelegramClient




class ChannelParser:
    def __init__(self):
        self.config = TelegramConfig()
        self.client = TelegramClient(self.config.get_api_id(), self.config.get_api_hash()).get_client()
        self.channels = {}

    async def get_all_channels(self):
        async with self.client:
            async for dialog in self.client.get_dialogs():
                chat = dialog.chat
                channel_info = {
                    'id': chat.id,
                    'title': chat.title or chat.first_name,
                    'username': chat.username,
                    'members': chat.members_count,
                    'type': str(chat.type),
                    'url': f"https://t.me/{chat.username}" if chat.username else '',
                    'last_posted_message_id': dialog.top_message.id,
                    'last_posted_message_data': str(dialog.top_message.date),
                    'updated_at': str(datetime.now())
                }
                self.channels[chat.id] = channel_info
                print(f"Collected data: {channel_info}")

    def get_channels(self):
        self.client.run(self.get_all_channels())
        return self.channels


# example
# parser = ChannelParser()
# result = parser.get_channels()
