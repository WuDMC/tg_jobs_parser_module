import os
import json
import logging
from tg_jobs_parser.google_cloud_helper.storage_manager import StorageManager
from tg_jobs_parser.utils import json_helper
from tg_jobs_parser.configs import volume_folder_path



if __name__ == "__main__":
    try:
        sm = StorageManager()
        sm.check_channel_stats(bucket_name='wu-eu-west', type_filter="ChatType.CHANNEL")
    finally:
        json_helper.delete_files_recursively(volume_folder_path)
