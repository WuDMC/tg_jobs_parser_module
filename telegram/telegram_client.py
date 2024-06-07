from pyrogram import Client


class TelegramClient:
    def __init__(self, api_id, api_hash):
        self.app = Client("my_account", api_id=api_id, api_hash=api_hash)

    def get_client(self):
        return self.app
