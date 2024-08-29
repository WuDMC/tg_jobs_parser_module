from pyrogram import Client
from pyrogram.errors import PeerIdInvalid
from tg_jobs_parser.configs import vars

import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class TelegramClient:
    def __init__(self, api_id=None, api_hash=None, session_string=None):
        self.session_string = None
        if session_string:
            logging.info("authorize with session string")
            self.app = Client(
                vars.ACCOUNT_NAME, session_string=session_string, in_memory=True
            )
        elif api_id and api_hash:
            logging.info("authorize with api key and api hash")
            self.app = Client(
                vars.ACCOUNT_NAME, api_id=api_id, api_hash=api_hash, in_memory=True
            )
        else:
            logging.warning("not enough data for authorization")
            raise

    def get_client(self):
        return self.app

    async def get_session_string(self):
        async with self.app as app:
            self.session_string = await app.export_session_string()
            logging.info(f"Session string: {self.session_string}")
            return self.session_string

    async def get_message_id(self, chat_id=None, username=None):
        try:
            async with self.app as app:
                if chat_id:
                    async for message in app.get_chat_history(int(chat_id), limit=1):
                        logging.info(f"{message.text}")
                elif username:
                    chat = await app.get_chat(username)
                    async for message in app.get_chat_history(chat.id, limit=1):
                        logging.info(f"{message.text}")
        except PeerIdInvalid:
            logging.error(
                "Error: Invalid peer ID. Ensure the bot/client has access to this chat."
            )
        finally:
            logging.info(f"{self} done")

    async def get_me(self):
        async with self.app as app:
            self.me = await app.get_me()
            logging.info(f"me is {self.me}")

    def test_client(self):
        self.app.run(self.get_me())

    def test_message(self):
        self.app.run(
            self.get_message_id(chat_id="-1001140315542")
        )  # or username='valid_username'
