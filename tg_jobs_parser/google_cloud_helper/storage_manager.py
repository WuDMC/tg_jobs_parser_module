import google
from google.cloud import storage
from tg_jobs_parser.configs import GoogleCloudConfig
import logging
import os
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class StorageManager:
    def __init__(self):
        self.config = GoogleCloudConfig()
        self.client = storage.Client()

    def upload_file(self, source_file_name, destination_blob_name):
        bucket = self.client.bucket(self.config.bucket_name)
        blob = bucket.blob(destination_blob_name)
        if blob.exists():
            logging.info(f"File: {destination_blob_name} already exists")
            return False
        blob.upload_from_filename(source_file_name)
        if blob.exists():
            logging.info(f"Upload  to GCS  successful: {destination_blob_name}")
            return True
        else:
            logging.error(f"Upload to GCS failed: {destination_blob_name}")
            return False

    def upload_message(self, source_file_name, destination_blob_name):
        bucket = self.client.bucket(self.config.bucket_name)
        path = f'{self.config.source_msg_blob}/{destination_blob_name}'
        blob = bucket.blob(path)
        blob.upload_from_filename(source_file_name)
        if blob.exists():
            logging.info(f"Upload msg  to GCS  successful: {path}")
            return True
        else:
            logging.error(f"Upload msg to GCS failed: {path}")
            return False

    def update_channels_metadata(self, source_file_name):
        bucket = self.client.bucket(self.config.bucket_name)
        blob = bucket.blob(self.config.source_channels_blob)
        try:
            blob.upload_from_filename(source_file_name)
            logging.info(f"File {source_file_name} uploded successfully to {blob}")
            return True
        except Exception as e:
            logging.error(f"Error: {e}")
            return None

    def download_channels_metadata(self, path):
        bucket = self.client.bucket(self.config.bucket_name)
        blob = bucket.blob(self.config.source_channels_blob)
        try:
            blob.download_to_filename(path)
            logging.info(f"File downloaded successfully to {path}")
            return True
        except Exception as e:
            logging.error(f"Error: {e}")
            return None

    def delete_local_file(self, file_path):
        """Deletes a local file."""
        try:
            os.remove(file_path)
            logging.info(f"Local file {file_path} deleted successfully.")
            return True
        except OSError as e:
            logging.error(f"Error deleting file {file_path}: {e}")
            return False
