import json
import ndjson
import logging

import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def read_json(file_path):
    if not os.path.exists(file_path):
        logging.info(f'cant find {file_path}')
        return None
    with open(file_path, 'r', encoding='utf-8') as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            logging.info(f'json error {file_path}')
            return None
        except KeyError:
            logging.info(f'key error {file_path}')
            return None
        except IndexError:
            logging.info(f'index error {file_path}')
            return None


def save_to_json(data, path):
    try:
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

        logging.info(f"File saved successfully: {path}")
        return True

    except Exception as e:
        logging.error(f"Failed to save file: {path}, Error: {e}")
        return False

def save_to_line_delimeted_json(file, path):
    with open(path, 'w', encoding='utf-8') as f:
        ndjson.dump(file, f, ensure_ascii=False, indent=4)

def read_line_delimeted_json(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        ndjson.load(f, ensure_ascii=False, indent=4)

def merge_json_files(file1_path, file2_path, output_path):
    """
    Объединяет два JSON файла и сохраняет результат в новый файл.

    :param file1_path: Путь к первому JSON файлу
    :param file2_path: Путь ко второму JSON файлу
    :param output_path: Путь к выходному JSON файлу
    """

    json1 = read_json(file1_path)
    json2 = read_json(file2_path)

    if json1 is None or json2 is None:
        print(' one of file (or both) is empty')
        return

    print('test')
    # Рекурсивная функция для объединения словарей
    def recursive_merge(dict1, dict2):
        for key in dict2:
            if key in dict1 and isinstance(dict1[key], dict) and isinstance(dict2[key], dict):
                recursive_merge(dict1[key], dict2[key])
            else:
                dict1[key] = dict2[key]
    merged_json = json1.copy()
    recursive_merge(merged_json, json2)
    # Записываем объединенный JSON в выходной файл
    with open(output_path, 'w', encoding='utf-8') as output_file:
        json.dump(merged_json, output_file, ensure_ascii=False, indent=2)
# Out[26]: checking data before merge targets 4045 <<=>> 4191
# checking data before merge current stats 4046 <<=>>  4192
# checking data before merge right 4152 =>> 4192
# {'new_left_saved_id': 4152,
#  'new_right_saved_id': 4192,
#  'uploaded_path': ['/home/wudmc/PycharmProjects/jobs_parser/volume/msgs-1001151522902_left_4152_right_4192.json',
#   '-1001151522902/msgs-1001151522902_left_4152_right_4192.json']}

def update_uploaded_borders(file_path_1, file_path_2, output_path):
    # file_path_1 - путь к клауд метадата чанелс
    # file_path_2 - путь к метадате об uplodaed to storage файлах

    try:
        data1 = read_json(file_path_1)
        data2 = read_json(file_path_2)
        for key, values in data2.items():
            if key in data1:
                # Обновление left_saved_id
                logging.info(f"checking1 data before merge targets {data1[key].get('target_id')} <<=>> {data1[key]['last_posted_message_id']}")
                logging.info(f"checking2 data before merge current stats {data1[key].get('left_saved_id')} <<=>>  {data1[key].get('right_saved_id')}")
                logging.info(f"checking3 data before merge right {values['new_left_saved_id']} <<=>> {values['new_right_saved_id']}")

                if data1[key].get('left_saved_id') is None and data1[key].get('right_saved_id') is None:
                    logging.info(
                        f"Updating left_saved_id for {key} from {data1[key].get('left_saved_id')} to {values['new_left_saved_id']}")
                    data1[key]['left_saved_id'] = values['new_left_saved_id']
                    logging.info(
                        f"Updating right_saved_id for {key} from {data1[key].get('right_saved_id')} to {values['new_right_saved_id']}")
                    data1[key]['right_saved_id'] = values['new_right_saved_id']
                elif values['new_left_saved_id'] < data1[key]['left_saved_id'] and values['new_right_saved_id'] == data1[key]['left_saved_id'] + 1:
                    logging.info(
                        f"Updating left_saved_id for {key} from {data1[key].get('left_saved_id')} to {values['new_left_saved_id']}")
                    data1[key]['left_saved_id'] = values['new_left_saved_id']
                elif values['new_right_saved_id'] > data1[key]['right_saved_id'] and values['new_left_saved_id'] == data1[key]['right_saved_id'] + 1:
                    logging.info(
                        f"Updating right_saved_id for {key} from {data1[key].get('right_saved_id')} to {values['new_right_saved_id']}")
                    data1[key]['right_saved_id'] = values['new_right_saved_id']
                elif values['new_right_saved_id'] <= data1[key]['right_saved_id'] and values['new_left_saved_id'] >= data1[key]['left_saved_id']:
                    logging.error(f'File was already uploaded. cloud data {data1[key]} with file data {values}')
                else:
                    logging.error(f'uncovered case {data1[key]} with {values}')
        return save_to_json(data1, output_path)
    except Exception as e:
        logging.error(f"Failed to update borders with file1: {file_path_1} << {file_path_2} Error: {e}")
        return False


# Пример использования:
# file1_path = '/home/wudmc/PycharmProjects/tg_parser/gsc_channels_metadata.json'
# file2_path = '/home/wudmc/PycharmProjects/tg_parser/tg_channels_metadata.json'
# output_path = '/home/wudmc/PycharmProjects/tg_parser/merged.json'

# merge_json_files(file1_path, file2_path, output_path)




