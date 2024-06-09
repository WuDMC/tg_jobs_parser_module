# from airflow.decorators import dag, task
# from datetime import datetime, timedelta
# import os
#
# from tg_parser.telegram_helper.channel_parser import ChannelParser
# from tg_parser.google_cloud_helper.storage_manager import StorageManager
# from tg_parser.utils import json_helper
# from tg_parser.configs import vars
# from tg_parser.telegram_helper.message_parser import MessageParser
#
# CL_CHANNELS_LOCAL_PATH = os.path.join(vars.VOLUME_DIR, 'gsc_channels_metadata.json')
# TG_CHANNELS_LOCAL_PATH = os.path.join(vars.VOLUME_DIR, 'tg_channels_metadata.json')
# MG_CHANNELS_LOCAL_PATH = os.path.join(vars.VOLUME_DIR,
#                                       'merged_channels_metadata.json')
#
#
#
# # Определяем DAG
# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
#     'start_date': datetime(2024, 7, 1),
# }
#
#
# @dag(
#     default_args=default_args,
#     schedule_interval=timedelta(days=1),
#     catchup=False,  # Отключить выполнение DAG'а за пропущенные интервалы
#     description='DAG для обновления метаданных каналов',
# )
# def update_channels_metadata_dag():
#     @task
#     def init_channel_parser_and_parse_tg_dialog():
#         print('init channel_parser and parse tg dialog')
#         channel_parser = ChannelParser()
#         dialogs = channel_parser.get_channels()
#         channel_parser.client.stop()
#         json_helper.save_to_json(file=dialogs, path=TG_CHANNELS_LOCAL_PATH)
#         return TG_CHANNELS_LOCAL_PATH
#
#     @task
#     def get_metadata_from_cloud():
#         print('get metadata from cloud')
#         storage_manager = StorageManager()
#         storage_manager.download_channels_metadata(path=CL_CHANNELS_LOCAL_PATH)
#         return CL_CHANNELS_LOCAL_PATH
#
#     @task
#     def update_metadata_locally(date, force):
#         msg_parser = MessageParser()
#         cloud_channels = json_helper.read_json(CL_CHANNELS_LOCAL_PATH) or {}
#         tg_channels = json_helper.read_json(TG_CHANNELS_LOCAL_PATH)
#         try:
#             for ch_id in tg_channels:
#                 cloud_channels.setdefault(ch_id, {'target_date': None})
#                 if cloud_channels[ch_id]['target_date'] and force is False:
#                     print(f'for {ch_id} target date already setted')
#                     continue
#                 print(f'getting target date for {ch_id} ')
#                 msgs_data = msg_parser.run_msg_parser(ch_id, date)
#                 if msgs_data:
#                     cloud_channels[ch_id]['left_saved_id'] = None
#                     cloud_channels[ch_id]['right_saved_id'] = None
#                     cloud_channels[ch_id]['target_id'] = msgs_data[0]
#                     cloud_channels[ch_id]['target_date'] = msgs_data[1]
#                     cloud_channels[ch_id]['status'] = 'ok'
#                 else:
#                     cloud_channels[ch_id]['status'] = 'bad'
#                 print(cloud_channels[ch_id])
#         finally:
#             msg_parser.client.stop()
#             json_helper.save_to_json(cloud_channels, CL_CHANNELS_LOCAL_PATH)
#
#     @task
#     def merge_metadata():
#         print('merge_metadata')
#         json_helper.merge_json_files(file1_path=CL_CHANNELS_LOCAL_PATH, file2_path=TG_CHANNELS_LOCAL_PATH,
#                                      output_path=MG_CHANNELS_LOCAL_PATH)
#         return MG_CHANNELS_LOCAL_PATH
#
#     @task
#     def update_metadata_in_cloud():
#         print('update metadata in cloud')
#         storage_manager = StorageManager()
#         storage_manager.update_channels_metadata(source_file_name=MG_CHANNELS_LOCAL_PATH)
#
#     # Порядок выполнения задач
#     init_channel_parser_and_parse_tg_dialog()
#     get_metadata_from_cloud()
#     update_metadata_locally(vars.START_DATE, False)
#     merge_metadata()
#     update_metadata_in_cloud()
#
#
# # Инстанцируем DAG
# dag_instance = update_channels_metadata_dag()
