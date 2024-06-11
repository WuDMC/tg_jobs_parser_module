from tg_jobs_parser.configs import vars
from dotenv import load_dotenv
import os

# edit if needed => to store messages and
volume_folder_path = os.path.abspath(os.path.join(os.getcwd(), '..', '..', vars.VOLUME_DIR))


class GoogleCloudConfig:

    def __init__(self):
        # login into G-cloud
        load_dotenv()
        self.bucket_name = vars.GCS_BUCKET_NAME
        self.source_channels_blob = vars.CHANNEL_METADATA_PATH

    def get_bucket_name(self):
        return self.bucket_name

    def source_channels_blob(self):
        return self.source_channels_blob

    def get_bigquery_config(self):
        pass


class TelegramConfig:
    def __init__(self):
        load_dotenv()
        self.session_string = os.getenv('SESSION_STRING')
        self.api_id = os.getenv('TG_API_ID')
        self.api_hash = os.getenv('TG_API_HASH')

    def get_api_id(self):
        return self.api_id

    def get_api_hash(self):
        return self.api_hash

    def get_session(self):
        # return None
        return self.session_string
