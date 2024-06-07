import json
import ndjson

import os


def read_json(file_path):
    if not os.path.exists(file_path):
        print(f'cant find {file_path}')
        return None
    with open(file_path, 'r', encoding='utf-8') as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            print(f'json error {file_path}')
            return None
        except KeyError:
            print(f'key error {file_path}')
            return None
        except IndexError:
            print(f'index error {file_path}')
            return None


def save_to_json(file, path):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(file, f, ensure_ascii=False, indent=4)

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



# Пример использования:
# file1_path = '/home/wudmc/PycharmProjects/tg_parser/gsc_channels_metadata.json'
# file2_path = '/home/wudmc/PycharmProjects/tg_parser/tg_channels_metadata.json'
# output_path = '/home/wudmc/PycharmProjects/tg_parser/merged.json'

# merge_json_files(file1_path, file2_path, output_path)




