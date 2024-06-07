from dotenv import load_dotenv
import os


class TelegramConfig:
    def __init__(self):
        load_dotenv()
        self.api_id = os.getenv('TG_API_ID')
        self.api_hash = os.getenv('TG_API_HASH')

    def get_api_id(self):
        return self.api_id

    def get_api_hash(self):
        return self.api_hash
