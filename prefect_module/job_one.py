#### JUST SCRATCHES for AIRFLOW JOBS

from prefect import flow

from telegram_helper.channel_parser import ChannelParser
from google_cloud_helper.storage_manager import StorageManager
from telegram_helper.message_parser import MessageParser
from utils import json_helper
from configs import vars
import os

CL_CHANNELS_LOCAL_PATH = os.path.join(vars.VOLUME_DIR, 'gsc_channels_metadata.json')
TG_CHANNELS_LOCAL_PATH = os.path.join(vars.VOLUME_DIR, 'tg_channels_metadata.json')
MG_CHANNELS_LOCAL_PATH = os.path.join(vars.VOLUME_DIR, 'merged_channels_metadata.json')

@flow(log_prints=True)
def job_one(force=False, date=vars.START_DATE):
    """
        job ONE - update channels metadata
        get channels_metadata from GCS (if None => parse target id and target_dat)
        parse channels from TG and update last posted Id and Date
        merge left_saved_id right_saved_id target_id  target_dat status
        update channels metadata in GCS
        :param force: force update target_id and target_date
        :param date: target_date
        :return: update channels metadata in cloud
    """
    print('job one starts')

    print('init channel_parser and parse tg dialog')
    channel_parser = ChannelParser()
    dialogs = channel_parser.get_channels()
    # channel_parser.client.stop()
    json_helper.save_to_json(file=dialogs, path=TG_CHANNELS_LOCAL_PATH)

    print('get metadata from cloud')
    storage_manager = StorageManager()
    storage_manager.download_channels_metadata(path=CL_CHANNELS_LOCAL_PATH)

    print('update metadata locally')
    update_channels(date, force)

    print('merge_metadata')
    json_helper.merge_json_files(file1_path=CL_CHANNELS_LOCAL_PATH, file2_path=TG_CHANNELS_LOCAL_PATH,
                                 output_path=MG_CHANNELS_LOCAL_PATH)

    print('update metadata in cloud')
    storage_manager.update_channels_metadata(source_file_name=MG_CHANNELS_LOCAL_PATH)

    print('job one finished')


def update_channels(date, force=False):
    msg_parser = MessageParser()
    cloud_channels = json_helper.read_json(CL_CHANNELS_LOCAL_PATH) or {}
    tg_channels = json_helper.read_json(TG_CHANNELS_LOCAL_PATH)
    try:
        for ch_id in tg_channels:
            cloud_channels.setdefault(ch_id, {'target_date': None})
            if cloud_channels[ch_id]['target_date'] and force is False:
                print(f'for {ch_id} target date already setted')
                continue
            print(f'getting target date for {ch_id} ')
            msgs_data = msg_parser.run_msg_parser(ch_id, date)
            if msgs_data:
                cloud_channels[ch_id]['left_saved_id'] = None
                cloud_channels[ch_id]['right_saved_id'] = None
                cloud_channels[ch_id]['target_id'] = msgs_data[0]
                cloud_channels[ch_id]['target_date'] = msgs_data[1]
                cloud_channels[ch_id]['status'] = 'ok'
            else:
                cloud_channels[ch_id]['status'] = 'bad'
            print(cloud_channels[ch_id])
    finally:
        json_helper.save_to_json(cloud_channels, CL_CHANNELS_LOCAL_PATH)

if __name__ == "__main__":
    job_one.serve(name="job-one-new",
                      tags=["test"],
                  interval=100
                      )
# job_one()