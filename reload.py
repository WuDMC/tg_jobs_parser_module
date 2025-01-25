from tg_jobs_parser.google_cloud_helper.storage_manager import StorageManager
from tg_jobs_parser.google_cloud_helper.bigquery_manager import BigQueryManager
from datetime import datetime, timedelta
import logging
from tg_jobs_parser.utils import json_helper
import os
from tg_jobs_parser.configs import GoogleCloudConfig, vars

# Инициализация
sm = StorageManager()
bq = BigQueryManager("geo-roulette")
# Настройка логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def reload():
    try:
        # Получение всех файлов с метаданными
        all_blobs = sm.list_msgs_with_metadata()
        total_files = len(all_blobs)
        logging.info(f"Начало загрузки: найдено {total_files} файлов.")

        # Счётчик успешно загруженных файлов
        loaded_files = 0
        table_id = f"{vars.BIGQUERY_DATASET}.{vars.BIGQUERY_RAW_MESSAGES_TABLE}"
        for blob in all_blobs:
            try:
                # Попытка загрузить файл в BigQuery
                bq.load_json_uri_to_bigquery(blob["full_path"], table_id)
                loaded_files += 1
                logging.info(f"Файл загружен успешно: {blob['name']}")
                logging.info(f"успешно загружено {loaded_files} / всего файлов {total_files}")

            except Exception as e:
                # Логирование ошибки при загрузке файла
                logging.error(f"Ошибка при загрузке файла {blob['name']}: {str(e)}")

        # Финальный лог
        logging.info(f"Загрузка завершена: всего файлов {total_files}, успешно загружено {loaded_files}.")
    except Exception as e:
        # Логирование критической ошибки
        logging.critical(f"Ошибка при выполнении загрузки: {str(e)}")


reload()

