from google.cloud import storage
from tg_jobs_parser.configs import GoogleCloudConfig, volume_folder_path
from tg_jobs_parser.utils.json_helper import read_json

import logging
import os

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


# example metadata
# {
#     "id": -1001165767969,
#     "title": "Биржа IT I Удаленка/Офис",
#     "username": "Itjobonline",
#     "members": 5905,
#     "type": "ChatType.SUPERGROUP",
#     "url": "https://t.me/Itjobonline",
#     "last_posted_message_id": 186024,
#     "last_posted_message_data": "2025-01-26 23:48:50",
#     "updated_at": "2025-01-26 23:50:26.929816",
#     "target_date": "2024-03-31 19:34:12",
#     "left_saved_id": null,
#     "right_saved_id": null,
#     "target_id": 58105,
#     "missed_msgs": 0,
#     "status": "ok"
#   }



def delete_local_file(file_path):
    try:
        os.remove(file_path)
        logging.info(f"Local file {file_path} deleted successfully.")
        return True
    except OSError as e:
        logging.error(f"Error deleting file {file_path}: {e}")
        return False


class StorageManager:
    STATS_TEMPLATE = {
            "channels_total": 0,
            "channels_done": 0,
            "total_downloaded": 0,
            "download_scope": 0,
            "total_missed": 0,
            "channels_to_update": 0,
            "bad_channels": 0,
            "bad_channels_ids": [],
            "to_upd_channels_ids": [],
        }

    def __init__(self, json_config=None):
        self.config = GoogleCloudConfig(json_config=json_config)
        self.client = storage.Client()
        self.statistics = self.STATS_TEMPLATE.copy()

    def list_msgs_with_metadata(self, prefix=None, bucket_name=None):
        bucket_name = bucket_name or self.config.bucket_name
        prefix = prefix or self.config.source_msg_blob
        try:
            bucket = storage.Bucket(
                self.client, bucket_name, user_project=self.config.project
            )
            all_blobs = list(
                self.client.list_blobs(bucket, prefix=prefix)
            )
            files_metadata = []
            for blob in all_blobs:
                metadata = {
                    "name": blob.name.split("/")[-1],
                    "channel": blob.name.split("/")[-2],
                    "full_path": blob.name,
                    "size": blob.size,
                    "content_type": blob.content_type,
                    "updated": blob.updated,
                    "time_created": blob.time_created,
                }
                files_metadata.append(metadata)
                logging.info(f"metadata appended: {metadata},")

            return files_metadata

        except Exception as e:
            logging.error(f"Error while listing files: {e}")
            return []

    def get_folders(self, prefix=None, bucket_name=None):
        bucket_name = bucket_name or self.config.bucket_name
        prefix = prefix or self.config.source_msg_blob
        try:
            all_blobs = self.list_msgs_with_metadata(prefix, bucket_name)
            top_level_folders = {item["channel"] for item in all_blobs if "channel" in item}
            return list(top_level_folders)
        except Exception as e:
            logging.error(f"Error while listing folders: {e}")
            return []

    def upload_file(self, source_file_name, destination_blob_name, bucket_name=None):
        bucket_name = bucket_name or self.config.bucket_name
        bucket = self.client.bucket(bucket_name)
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

    def upload_message(self, source_file_name, destination_blob_name, bucket_name=None):
        bucket_name = bucket_name or self.config.bucket_name
        bucket = self.client.bucket(bucket_name)
        path = f"{self.config.source_msg_blob}/{destination_blob_name}"
        blob = bucket.blob(path)
        blob.upload_from_filename(source_file_name)
        if blob.exists():
            logging.info(f"Upload msg  to GCS  successful: {path}")
            return True
        else:
            logging.error(f"Upload msg to GCS failed: {path}")
            return False

    def delete_blob(self, blob_name, bucket_name=None):
        bucket_name = bucket_name or self.config.bucket_name
        bucket = self.client.bucket(bucket_name)  # Получаем бакет
        blob = bucket.blob(blob_name)  # Указываем путь к файлу (blob)

        if blob.exists():  # Проверяем, существует ли файл
            blob.delete()  # Удаляем файл
            logging.info(f"File {blob_name} successfully deleted from GCS.")
            return True
        else:
            logging.error(f"File {blob_name} does not exist in GCS.")
            return False

    def update_channels_metadata(self, source_file_name, bucket_name=None):
        bucket_name = bucket_name or self.config.bucket_name
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(self.config.source_channels_blob)
        try:
            blob.upload_from_filename(source_file_name)
            logging.info(f"File {source_file_name} uploded successfully to {blob}")
            return True
        except Exception as e:
            logging.error(f"Error: {e}")
            return None

    def download_blob(self, blob_name, path, bucket_name=None):
        bucket_name = bucket_name or self.config.bucket_name
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        try:
            blob.download_to_filename(path)
            logging.info(f"Blob {blob_name} downloaded successfully to {path}")
            return True
        except Exception as e:
            logging.error(f"Error: {e}")
            return None

    def move_blob(self, source_blob_name, destination_blob_name, bucket_name=None):
        """
        Перемещает блоб внутри одного бакета в Google Cloud Storage.

        :param bucket_name:
        :param source_blob_name: Текущее имя блоба (его путь внутри бакета).
        :param destination_blob_name: Новое имя блоба (куда он будет перемещен).
        :return: True, если успешно, иначе None.
        """
        bucket_name = bucket_name or self.config.bucket_name
        bucket = self.client.bucket(bucket_name)
        source_blob = bucket.blob(source_blob_name)

        try:
            # Копируем блоб на новое место
            new_blob = bucket.copy_blob(source_blob, bucket, destination_blob_name)

            # Проверяем, что новый блоб существует
            if new_blob.exists():
                source_blob.delete()
                logging.info(f"Blob {source_blob_name} успешно перемещен в {destination_blob_name}")
                return True
            else:
                logging.error(f"Ошибка: Новый блоб {destination_blob_name} не был создан")
                return None
        except Exception as e:
            logging.error(f"Ошибка при перемещении блоба: {e}")
            return None

    def check_channel_stats(self, bucket_name=None, type_filter=None):
        CHAT_TYPES = {"ChatType.CHANNEL", "ChatType.PRIVATE", "ChatType.BOT", "ChatType.SUPERGROUP",
                      "ChatType.GROUP"}

        if isinstance(type_filter, str):
            type_filter = {type_filter}
        elif type_filter is not None:
            if not isinstance(type_filter, (list, set)):
                raise TypeError("type_filter must be a string, list, or set")
            type_filter = set(type_filter)
            invalid_filters = type_filter - CHAT_TYPES
            if invalid_filters:
                logging.error(f"Incorrect type_filter: {invalid_filters}. Must be from {CHAT_TYPES}")
            type_filter &= CHAT_TYPES
        else:
            type_filter = CHAT_TYPES

        bucket_name = bucket_name or self.config.bucket_name
        self.statistics = self.STATS_TEMPLATE.copy()
        tmp_path = os.path.join(
            volume_folder_path, "tmp_gsc_channels_metadata_for_stats.json"
        )
        self.download_blob(blob_name=self.config.source_channels_blob, path=tmp_path, bucket_name=bucket_name)
        channels = read_json(tmp_path)
        if  channels is None:
            logging.error(f"no stats file to download at {self.config.source_channels_blob}, bucket name: {bucket_name}")
            return None
        for group in channels.values():
            if (
                    "status" in group
                    and group["status"] == "ok"
                    and group["type"] in type_filter
                    and "last_posted_message_id" in group
                    and "target_id" in group
            ):
                missed = group.get("missed_msgs") or 0
                if group.get("right_saved_id") and group.get("left_saved_id"):
                    downloaded = (
                                    group.get("right_saved_id") - group.get("left_saved_id") + 1
                                  )
                else:
                    downloaded = 0
                valid_max_values = [val for val in [group.get("last_posted_message_id"), group.get("right_saved_id")] if
                                    val is not None]
                valid_min_values = [val for val in [group.get("target_id"), group.get("left_saved_id")] if
                                    val is not None]
                if valid_max_values and valid_min_values:
                    download_scope = max(valid_max_values) - min(valid_min_values) + 1
                else:
                    download_scope = 0
                difference = download_scope - downloaded
                self.statistics["total_downloaded"] += downloaded
                self.statistics["total_missed"] += missed
                self.statistics["download_scope"] += download_scope
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
        delete_local_file(tmp_path)

    def download_blobs(self, folder_blobs, bucket_name=None):
        # folder_blobs - metadata from blobs from one folder
        bucket_name = bucket_name or self.config.bucket_name

        if folder_blobs is None or len(folder_blobs) == 0:
            logging.error("no folder blobs to download")
            return None

        channel = folder_blobs[0]["channel"]
        local_folder_path = f"{volume_folder_path}/{channel}"
        if not os.path.exists(local_folder_path):
            os.makedirs(local_folder_path)
            logging.info(f"Created directory: {local_folder_path}")
        for blob_metadata in folder_blobs:
            blob_name = blob_metadata["full_path"]
            local_file_path = f"{local_folder_path}/{os.path.basename(blob_name)}"
            logging.info(f"Created file: {local_file_path}")

            try:
                self.download_blob(blob_name=blob_name, path=local_file_path, bucket_name=bucket_name)
            except Exception as e:
                logging.error(f"Error downloading blobs and processing {blob_name}: {e}")
        return local_folder_path

    def backup_blobs(self, folder_blobs, backup_path='backup', bucket_name=None):
        # folder_blobs - metadata from blobs from one folder
        bucket_name = bucket_name or self.config.bucket_name

        for blob_metadata in folder_blobs:
            blob_name = blob_metadata["full_path"]
            try:
                self.move_blob(source_blob_name=blob_name, destination_blob_name=f"{backup_path}/{blob_name}", bucket_name=bucket_name)
            except Exception as e:
                logging.error(f"Error backup blobs and processing {blob_name}: {e}")
                raise (str(e))
        return True

    def log_statistics(self):
        logging.info(f'total scope to download: {self.statistics["download_scope"]}')
        logging.info(f'msg downloaded: {self.statistics["total_downloaded"]}')
        logging.info(f'missed total: {self.statistics["total_missed"]}')
        logging.info(f'left to download: {self.statistics["download_scope"] - self.statistics["total_downloaded"]}')

        logging.info(f'channels_total: {self.statistics["channels_total"]}')
        logging.info(f'channels_done: {self.statistics["channels_done"]}')
        logging.info(f'channels_to_update: {self.statistics["channels_to_update"]}')
        logging.info(
            f'channels_to_update_ids: {self.statistics["to_upd_channels_ids"]}'
        )
        logging.info(f'bad_channels: {self.statistics["bad_channels"]}')
        logging.info(f'bad_channels_ids: {self.statistics["bad_channels_ids"]}')
