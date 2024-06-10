import google
from google.cloud import storage
from configs.google_cloud_config import GoogleCloudConfig

class StorageManager:
    def __init__(self):
        self.config = GoogleCloudConfig()
        self.client = storage.Client()

    def upload_file(self, source_file_name, destination_blob_name):
        bucket = self.client.bucket(self.config.bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_name)

    def update_channels_metadata(self, source_file_name):
        bucket = self.client.bucket(self.config.bucket_name)
        blob = bucket.blob(self.config.source_channels_blob)
        blob.upload_from_filename(source_file_name)

    def download_channels_metadata(self, path):
        bucket = self.client.bucket(self.config.bucket_name)
        blob = bucket.blob(self.config.source_channels_blob)
        try:
            blob.download_to_filename(path)
        except google.api_core.exceptions.NotFound as e:
            print(f"Ошибка: {e}")
            return None


