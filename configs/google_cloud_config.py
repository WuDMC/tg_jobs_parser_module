from configs import vars
from dotenv import load_dotenv


class GoogleCloudConfig:

    def __init__(self):
        load_dotenv()
        self.bucket_name = vars.GCS_BUCKET_NAME
        self.source_channels_blob = vars.CHANNEL_METADATA_PATH

    def get_bucket_name(self):
        return self.bucket_name

    def source_channels_blob(self):
        return self.source_channels_blob

    def get_bigquery_config(self):
        pass
