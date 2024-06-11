# some tests

from pyrogram import Client
from tg_jobs_parser.configs import TelegramConfig
from tg_jobs_parser.configs import vars, volume_folder_path
import os

SESSION_STRING_FILE = 'session_string.txt'


class TelegramHelper:
    def __init__(self):
        self.me = None
        self.config = TelegramConfig()
        session_string = self.config.get_session()
        if session_string:
            print('using session string')
            self.session_string = session_string
            self.client = Client(vars.ACCOUNT_NAME, in_memory=True, session_string=session_string)
        else:
            print('using api key & hash')
            self.client = Client(vars.ACCOUNT_NAME, api_id=self.config.get_api_id(), api_hash=self.config.get_api_hash(),
                                 in_memory=True)

    async def get_session_string(self):
        try:
            async with self.client as app:
                self.session_string = await app.export_session_string()
        finally:
            print(f'{self} done')

    async def get_me(self):
        try:
            async with self.client as app:
                self.me = await app.get_me()
                print(f'me is {self.me}')
        finally:
            print(f'{self} done')

    def get_new_session(self):
        self.client.run(self.get_session_string())
        print(f'Obtained session string: {self.session_string}')
        session_path = os.path.join(volume_folder_path, SESSION_STRING_FILE)
        with open(session_path, "w") as file:
            file.write(self.session_string)
            print(f"Session string saved to {session_path}")

    def test_new_session(self):
        self.client.run(self.get_me())



# helper = TelegramHelper()
# helper.get_new_session()
