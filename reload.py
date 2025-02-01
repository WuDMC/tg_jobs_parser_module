import os
from time import sleep

from tg_jobs_parser.google_cloud_helper.storage_manager import StorageManager
from tg_jobs_parser.google_cloud_helper.bigquery_manager import BigQueryManager
from tg_jobs_parser.utils import json_helper
from tg_jobs_parser.configs import volume_folder_path
import logging

# Инициализация
sm = StorageManager()
bq = BigQueryManager()
# Настройка логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def reload():
    try:
        json_helper.delete_files_recursively(volume_folder_path)

        sm.check_channel_stats()
        # Получение всех файлов с метаданными

        all_blobs = sm.list_msgs_with_metadata()
        total_files = len(all_blobs)
        logging.info(f"Начало загрузки: найдено {total_files} файлов.")
        error_blobs = []
        # Счётчик успешно загруженных файлов
        loaded_files = 0
        table_id = "tg_jobs.messages_raw_v4"
        for blob in all_blobs:
            sleep(2)
            try:
                # Попытка загрузить файл в BigQuery
                if bq.load_json_uri_to_bigquery(blob["full_path"], table_id):
                    loaded_files += 1
                    logging.info(f"Файл загружен успешно: {blob['name']}")
                    logging.info(f"успешно загружено {loaded_files} / всего файлов {total_files}")
                else:
                    tmp_path = "tmpnld.json"
                    logging.warning(f"Файл НЕ загружен: {blob['name']}, пробуем оптимизировать его")
                    sleep(1)
                    sm.download_blob(blob_name=blob["full_path"], path=tmp_path)
                    data = json_helper.read_line_delimeted_json(tmp_path)
                    data = [
                        {key: value for key, value in item.items() if key != 'ts'}
                        for item in data
                    ]
                    json_helper.save_to_line_delimited_json(data=data, path=tmp_path)
                    sm.delete_blob(blob["full_path"])
                    sm.upload_file(source_file_name=tmp_path, destination_blob_name=blob["full_path"])
                    os.remove(tmp_path)
                    if bq.load_json_uri_to_bigquery(blob["full_path"], table_id):
                        loaded_files += 1
                        logging.info(f"Файл загружен успешно: {blob['name']}")
                        logging.info(f"успешно загружено {loaded_files} / всего файлов {total_files}")
                    else:
                        error_blobs.append(blob)
                        logging.error(f"Файл НЕ загружен: {blob['name']},даже после оптимизации оптимизировать его")
            except Exception as e:
                # Логирование ошибки при загрузке файла
                error_blobs.append(blob["full_path"])
                logging.error(f"Ошибка при загрузке файла {blob['name']}: {str(e)}")

        if len(error_blobs) > 0:
            logging.error(f"Ошибка при загрузке файлов {error_blobs}")
        logging.info(f"Загрузка завершена: всего файлов {total_files}, успешно загружено {loaded_files}.")
    except Exception as e:
        # Логирование критической ошибки
        logging.critical(f"Ошибка при выполнении загрузки: {str(e)}")
    finally:
        json_helper.delete_files_recursively(volume_folder_path)


reload()
