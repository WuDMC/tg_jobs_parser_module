#### JUST SCRATCHES for AIRFLOW JOBS


from tg_jobs_parser.telegram_helper.channel_parser import ChannelParser
from tg_jobs_parser.google_cloud_helper.storage_manager import StorageManager
from tg_jobs_parser.telegram_helper.message_parser import MessageParser
from tg_jobs_parser.utils import json_helper
from tg_jobs_parser.configs import vars, volume_folder_path
import os

# metadata downloaded from cloud
CL_CHANNELS_LOCAL_PATH = os.path.join(volume_folder_path, 'gsc_channels_metadata.json')
# metadata parser from telegram
TG_CHANNELS_LOCAL_PATH = os.path.join(volume_folder_path, 'tg_channels_metadata2.json')
# metadata merged locally
MG_CHANNELS_LOCAL_PATH = os.path.join(volume_folder_path, 'merged_channels_metadata2.json')
# metadata uploadede to cloud (messages)
UP_CHANNELS_LOCAL_PATH = os.path.join(volume_folder_path, 'uploaded_msgs_metadata2.json')


def parse_msgs():
    """
    job two - read messages from channel
    # save in to json & avro files locally
    :param save_to:
    :param channel:
    :return: just save parsed msges to volume path
    """
    parser = MessageParser()
    # flow right: target_id or right border =>>>  last_posted_message_id.
    # flow left:  target_id <<<= left border or last_posted_message_id
    channels = json_helper.read_json(CL_CHANNELS_LOCAL_PATH)
    channel = channels['-1001239850479']
    msgs, left, right = parser.run_chat_parser(channel)
    print(f'{channel} will be saved to')

parse_msgs()

