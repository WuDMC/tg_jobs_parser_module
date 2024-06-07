from configs import vars


class GoogleCloudConfig:

    def __init__(self):
        self.bucket_name = vars.GCS_BUCKET_NAME
        self.source_channels_blob = vars.CHANNEL_METADATA_PATH

    def get_bucket_name(self):
        return self.bucket_name

    def source_channels_blob(self):
        return self.source_channels_blob

    def get_bigquery_config(self):
        pass
