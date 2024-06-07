#### JUST SCRATCHES for AIRFLOW JOBS


from telegram.channel_parser import ChannelParser
from google_cloud.storage_manager import StorageManager
from telegram.message_parser import MessageParser
from utils import json_helper
from configs import vars
import os

CL_CHANNELS_LOCAL_PATH = os.path.join(vars.VOLUME_DIR, 'gsc_channels_metadata.json')
TG_CHANNELS_LOCAL_PATH = os.path.join(vars.VOLUME_DIR, 'tg_channels_metadata.json')
MG_CHANNELS_LOCAL_PATH = os.path.join(vars.VOLUME_DIR, 'merged_channels_metadata.json')


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
    channel_parser.client.stop()
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
        msg_parser.client.stop()
        json_helper.save_to_json(cloud_channels, CL_CHANNELS_LOCAL_PATH)


def job_two(channel):
    """
    job two - read messages from channel
    # save in to json & avro files locally
    :param channel:
    :return: just save parsed msges to volume path
    """
    parser = MessageParser()
    # flow right: target_id or right border =>>>  last_posted_message_id.
    # flow left:  target_id <<<= left border or last_posted_message_id
    from_msg_id, limit, flow = chat_parser_args(channel)
    print(channel)
    msgs = parser.run_chat_parser(channel['id'], from_msg_id, limit, flow)
    if flow == 'right':
        left = from_msg_id + 1
        right = msgs[-1]['id']
    else:
        right = from_msg_id - 1
        left = msgs[0]['id']
    json_helper.save_to_line_delimeted_json(
        msgs,
        os.path.join(vars.VOLUME_DIR, f"msgs{channel['id']}_left_{left}_right_{right}.json")
    )
    parser.client.stop()


def chat_parser_args(channel):
    if channel['right_saved_id'] is None and channel['left_saved_id'] is None:
        flow = 'right'
        from_msg_id = channel['target_id']
        limit = int(channel['last_posted_message_id']) - int(from_msg_id)
    elif channel['right_saved_id'] is None or channel['left_saved_id'] is None:
        print(f"INCORRECT CASE => nothing to do at {channel['id']} CHECK THE DATA")
        raise
    elif channel['right_saved_id'] >= channel['last_posted_message_id'] \
            and channel['left_saved_id'] <= channel['target_id']:
        print(f"nothing to do at {channel['id']}")
        return
    elif channel['right_saved_id'] < channel['last_posted_message_id']:
        flow = 'right'
        from_msg_id = channel['right_saved_id']
        limit = int(channel['last_posted_message_id']) - int(from_msg_id)
    elif channel['left_saved_id'] > channel['target_id']:
        flow = 'left'
        from_msg_id = channel['left_saved_id']
        limit = int(from_msg_id) - int(channel['target_id'])
    else:
        print(f"UNCOVERED CASE => nothing to do at {channel['id']}")
        raise
    return from_msg_id, limit, flow


def job_three():
    """
    get channels_metadata from GCS
    each channel run job  two 'get messages' (saved to_json)
    :return:
    """
    storage_manager = StorageManager()
    storage_manager.download_channels_metadata(CL_CHANNELS_LOCAL_PATH)
    channels = json_helper.read_json(CL_CHANNELS_LOCAL_PATH)
    for ch_id in channels:
        if channels[ch_id]['type'] != 'ChatType.CHANNEL':
            continue
        job_two(channels[ch_id])


def job_four():
    """
    after all messages downloaded  load AVRO to GCS
    update cloud metadata with left and right ids if loaded OK
    :return:
    """


def job_five():
    """
    just check how much messages we have
    :return:
    """
    channels = json_helper.read_json(CL_CHANNELS_LOCAL_PATH)
    total_difference = sum(
        group["last_posted_message_id"] - group["target_id"]
        for group in channels.values()
        if group["status"] == "ok" and group[
            'type'] == 'ChatType.CHANNEL' and "last_posted_message_id" in group and "target_id" in group
    )
    print(total_difference)


job_one()
job_five()
