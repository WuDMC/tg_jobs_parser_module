import os
import json
import logging
import uuid

from tg_jobs_parser.google_cloud_helper.storage_manager import StorageManager
from tg_jobs_parser.utils import json_helper
from tg_jobs_parser.configs import volume_folder_path


def merge_downloaded_blobs(path):
    merged_data = {}
    duplicates_removed = 0

    for file in os.listdir(path):
        if file.startswith("msgs") and file.endswith(".json"):
            file_path = os.path.join(path, file)
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    for line in f:
                        try:
                            record = json.loads(line.strip())
                            record_id = record.get("id")
                            if record_id is None:
                                logging.warning(f"Record without 'id' in {file}: {record}")
                                continue

                            if record_id not in merged_data:
                                merged_data[record_id] = record
                            elif record_id in merged_data and not record.get("empty", True):
                                duplicates_removed += 1
                                merged_data[record_id] = record
                            else:
                                duplicates_removed += 1
                        except json.JSONDecodeError as e:
                            logging.error(f"Error parsing line in {file}: {e} | Line: {line.strip()}")

                # Safely delete the file after processing
                os.remove(file_path)
            except Exception as e:
                logging.error(f"Error processing file {file}: {e}")

    logging.info(f"Merged data with {duplicates_removed} duplicates removed.")
    return list(merged_data.values())


def process_data(data):
    sorted_data = data.copy()
    sorted_data.sort(key=lambda x: x["id"])
    min_id = sorted_data[0]["id"] if sorted_data else None
    max_id = sorted_data[-1]["id"] if sorted_data else None
    total_messages = len(data)
    skipped_msgs = (max_id - min_id + 1) - total_messages if min_id is not None and max_id is not None else 0

    logging.info(f"Statistics:")
    logging.info(f"  Total messages: {total_messages}")
    logging.info(f"  Min ID: {min_id}")
    logging.info(f"  Max ID: {max_id}")
    logging.info(f"  Missed msgs: {skipped_msgs}")

    return {
        "total_messages": total_messages,
        "min_id": min_id,
        "max_id": max_id,
        "skipped_msgs": skipped_msgs,
    }, sorted_data

def optimize(source_bucket_name, destination_bucket_name = None):
    try:
        if destination_bucket_name is None:
            destination_bucket_name = source_bucket_name
        sm = StorageManager()
        json_helper.delete_files_recursively(volume_folder_path)
        cl_channels_metadata_path = f"{volume_folder_path}/new_cloud_metadata.json"
        sm.check_channel_stats(bucket_name=source_bucket_name, type_filter="ChatType.CHANNEL")
        sm.download_blob(blob_name=sm.config.source_channels_blob, path=cl_channels_metadata_path, bucket_name=source_bucket_name)
        cloud_metadata = json_helper.read_json(cl_channels_metadata_path)
        folders = sm.get_folders(bucket_name=source_bucket_name)
        processed = 0
        processed_msgs = 0
        for folder in folders:
            prefix = f"{sm.config.source_msg_blob}/{folder}"
            channel_metadata = cloud_metadata[folder].copy() if cloud_metadata and folder in cloud_metadata else {}
            folder_blobs = sm.list_msgs_with_metadata(prefix, bucket_name=source_bucket_name)
            local_folder_path = sm.download_blobs(folder_blobs, bucket_name=source_bucket_name)
            merged_data = merge_downloaded_blobs(local_folder_path)
            stats, processed_data = process_data(merged_data)
            file_name = f"msgs{folder}_left_{stats['min_id']}_right_{stats['max_id']}.json"
            local_file_path = f"{volume_folder_path}/{file_name}"
            json_helper.save_to_line_delimited_json(processed_data, local_file_path )
            if destination_bucket_name == source_bucket_name:
                sm.backup_blobs(folder_blobs)
            sm.upload_file(source_file_name=local_file_path, destination_blob_name=f'{prefix}/{file_name}', bucket_name=destination_bucket_name)
            channel_metadata['left_saved_id'] = stats['min_id']
            channel_metadata['target_id'] = stats['min_id']
            channel_metadata['right_saved_id'] = stats['max_id']
            channel_metadata['last_posted_message_id'] = stats['max_id']
            channel_metadata['skipped_msgs'] = stats['skipped_msgs']
            # logging.info(f"channel stats: {stats}")
            cloud_metadata[folder] = channel_metadata
            processed_msgs = processed_msgs + stats['total_messages']
            processed = processed + 1
            logging.info(f"--- Processed {processed} files from {len(folders)} folder \n---------------- Total messages: {processed_msgs}")
        json_helper.save_to_json(data=cloud_metadata, path=cl_channels_metadata_path)
        sm.update_channels_metadata(source_file_name=cl_channels_metadata_path, bucket_name=destination_bucket_name)
        sm.check_channel_stats(bucket_name=destination_bucket_name, type_filter="ChatType.CHANNEL")
    finally:
        json_helper.delete_files_recursively(volume_folder_path)

if __name__ == "__main__":
    optimize('wu-eu-west', 'tg_msgs')
