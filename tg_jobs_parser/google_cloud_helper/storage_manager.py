import google
from google.cloud import storage
from tg_jobs_parser.configs import GoogleCloudConfig, volume_folder_path
from tg_jobs_parser.utils.json_helper import read_json

import logging
import os

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class StorageManager:
    def __init__(self):
        self.config = GoogleCloudConfig()
        self.client = storage.Client()
        self.statistics = {
            "channels_total": 0,
            "channels_done": 0,
            "total_downloaded": 0,
            "total_difference": 0,
            "channels_to_update": 0,
            "bad_channels": 0,
            "bad_channels_ids": [],
            "to_upd_channels_ids": [],
        }

    def list_msgs_with_metadata(self):
        try:
            bucket = storage.Bucket(
                self.client, self.config.bucket_name, user_project=self.config.project
            )
            all_blobs = list(
                self.client.list_blobs(bucket, prefix=self.config.source_msg_blob)
            )
            files_metadata = []
            for blob in all_blobs:
                metadata = {
                    "name": blob.name.split("/")[-1],
                    "full_path": blob.name,
                    "size": blob.size,
                    "content_type": blob.content_type,
                    "updated": blob.updated,
                    "time_created": blob.time_created,
                }
                files_metadata.append(metadata)
                logging.info(f"metadata appended: {metadata},")

            return files_metadata

        except google.api_core.exceptions.GoogleAPIError as e:
            logging.error(f"Error while listing files: {e}")
            return []

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
        path = f"{self.config.source_msg_blob}/{destination_blob_name}"
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
        try:
            os.remove(file_path)
            logging.info(f"Local file {file_path} deleted successfully.")
            return True
        except OSError as e:
            logging.error(f"Error deleting file {file_path}: {e}")
            return False

    def check_channel_stats(self):
        tmp_path = os.path.join(
            volume_folder_path, "tmp_gsc_channels_metadata_for_stats.json"
        )
        self.download_channels_metadata(path=tmp_path)
        channels = read_json(tmp_path)
        for group in channels.values():
            if (
                group["status"] == "ok"
                and group["type"] == "ChatType.CHANNEL"
                and "last_posted_message_id" in group
                and "target_id" in group
            ):
                downloaded = (group.get("right_saved_id") or 0) - (
                    group.get("left_saved_id") or 0
                )
                difference = (
                    group["last_posted_message_id"] - group["target_id"] - downloaded
                )
                self.statistics["total_downloaded"] += downloaded
                self.statistics["total_difference"] += difference
                self.statistics["channels_total"] += 1

                if difference == 0:
                    self.statistics["channels_done"] += 1
                elif difference > 0:
                    self.statistics["channels_to_update"] += 1
                    self.statistics["to_upd_channels_ids"].append(group.get("id"))
                else:
                    self.statistics["bad_channels"] += 1
                    self.statistics["bad_channels_ids"].append(group.get("id"))
        self.log_statistics()
        self.delete_local_file(tmp_path)

    def log_statistics(self):
        logging.info(f'msg downloaded: {self.statistics["total_downloaded"]}')
        logging.info(f'need to download total: {self.statistics["total_difference"]}')
        logging.info(f'channels_total: {self.statistics["channels_total"]}')
        logging.info(f'channels_done: {self.statistics["channels_done"]}')
        logging.info(f'channels_to_update: {self.statistics["channels_to_update"]}')
        logging.info(
            f'channels_to_update_ids: {self.statistics["to_upd_channels_ids"]}'
        )
        logging.info(f'bad_channels: {self.statistics["bad_channels"]}')
        logging.info(f'bad_channels_ids: {self.statistics["bad_channels_ids"]}')
