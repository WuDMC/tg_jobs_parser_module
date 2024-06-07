import asyncio
import json
import os
from datetime import datetime, timezone
from configs.telegram_config import TelegramConfig
from telegram.telegram_client import TelegramClient


class ChannelParser:
    def __init__(self):
        self.config = TelegramConfig()
        self.client = TelegramClient(self.config.get_api_id(), self.config.get_api_hash()).get_client()
        self.start_date = datetime(2024, 4, 1, tzinfo=timezone.utc)

    # async def get_message_id(self, chat_id):
    #     print(f'finding id of msg for {self.start_date} in chat {chat_id}')
    #     async for message in self.client.get_chat_history(chat_id, limit=1, offset_date=self.start_date):
    #         await asyncio.sleep(1)
    #         return message.id
    #     return None

    async def get_all_channels(self):
        await self.client.start()
        channels = {}
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
            channels[chat.id] = channel_info
            print(f"data {str(channel_info)}")
        print('done')
        return channels

    def get_channels(self):
        loop = asyncio.get_event_loop()
        task = loop.create_task(self.get_all_channels())
        return loop.run_until_complete(task)

# parser = ChannelParser()
# parser.get_channels()
# parser.client.stop()