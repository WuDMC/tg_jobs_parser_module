import os
import asyncio
from pyrogram import Client
from pyrogram.errors import PeerIdInvalid
from configs import vars


class TelegramClient:
    def __init__(self, api_id=None, api_hash=None, session_string=None):
        if session_string:
            print('authorize with session string')
            self.app = Client(vars.ACCOUNT_NAME, session_string=session_string, in_memory=True)
        elif api_id and api_hash:
            print('authorize with api key and api hash')
            self.app = Client(vars.ACCOUNT_NAME, api_id=api_id, api_hash=api_hash, in_memory=True)
        else:
            print('not enough data for authorization')
            raise

    def get_client(self):
        return self.app

    async def get_session_string(self):
        async with self.app as app:
            self.session_string = await app.export_session_string()
            print(f'Session string: {self.session_string}')
            return self.session_string

    async def get_message_id(self, chat_id=None, username=None):
        try:
            async with self.app as app:
                if chat_id:
                    async for message in app.get_chat_history(int(chat_id), limit=1):
                        print(message.text)
                elif username:
                    chat = await app.get_chat(username)
                    async for message in app.get_chat_history(chat.id, limit=1):
                        print(message.text)
        except PeerIdInvalid:
            print("Error: Invalid peer ID. Ensure the bot/client has access to this chat.")
        finally:
            print(f'{self} done')

    async def get_me(self):
        async with self.app as app:
            self.me = await app.get_me()
            print(f'me is {self.me}')

    def test_client(self):
        self.app.run(self.get_me())

    def test_message(self):
        # Replace `chat_id` or `username` with a valid one
        self.app.run(self.get_message_id(chat_id='-1001140315542'))  # or username='valid_username'

from configs.telegram_config import TelegramConfig

# config = TelegramConfig()
# client = TelegramClient(session_string=config.get_session())
# client.test_client()
#
# # To test fetching messages
# client.test_message()
