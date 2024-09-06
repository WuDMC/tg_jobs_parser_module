from tg_jobs_parser.telegram_helper.telegram_parser import TelegramParser
from tg_jobs_parser.google_cloud_helper.bigquery_manager import BigQueryManager
from datetime import datetime, timedelta
import logging
from tg_jobs_parser.utils import json_helper
import os

msg_parser = TelegramParser()
bigquery_manager = BigQueryManager("geo-roulette")
max_messages_limit = 2
day_limit = 7
def send_messages():
    try:
        logging.info("Downloading channels metadata")
        channels = msg_parser.get_channels()
        channels_statuses = bigquery_manager.select_all(dataset_id='tg_jobs', table_id='group_msg_statuses')
        msgs_sent = 0
        process_status = 0
        # process_len = len(channels.items())
        results = []
        new_datetime = datetime.now() + timedelta(days=day_limit)

        for ch_id, channel in channels.items():
            process_status += 1
            # logging.info(f"processed {process_status} from {process_len} channels")
            if channel["type"] != "ChatType.SUPERGROUP" and channel["type"] != "ChatType.GROUP":
                continue
            last_result = [item for item in channels_statuses if item['chat_id'] == str(ch_id)]
            if last_result and last_result[-1]['datetime'] < new_datetime:
                logging.info(f"Message was sent less than 4 days ago to channel {ch_id}: {channel['title']}, {channel['url']}, at {last_result[0]['datetime'] }")
                continue


            logging.info(f"Sending message to channel {ch_id}: {channel['title']}, {channel['url']}")
            try:
                # response = msg_parser.send_cv(ch_id)
                # result = {'result': True, 'chat_id': ch_id,'msg_id': response.__dict__['id'], 'datetime': response.str(response.__dict__['date']), 'error': None }
                result = {'result': True, 'chat_id': ch_id,'msg_id': 1, 'datetime': str(datetime.now()), 'error': None }
            except Exception as e:
                result = {'result': False, 'chat_id': ch_id,'msg_id': None, 'datetime': str(datetime.now()), 'error': str(e) }
            msgs_sent += 1
            results.append(result)
            if msgs_sent >= max_messages_limit:
                break
        if results:
            TMP_FILE = os.path.join('/home/wudmc/PycharmProjects/jobs_parser/volume', "group_msg_statuses.json")
            json_helper.save_to_line_delimited_json(results, TMP_FILE)
            bigquery_manager.load_json_to_bigquery(TMP_FILE, 'tg_jobs.group_msg_statuses')
        else:
            logging.info('nothing to sent to bq')
        return True
    except Exception as e:
        raise Exception(f"Error parsing messages: {e}")


send_messages()