import re

VOLUME_DIR = 'volume'

PROJECT_ID = 'geo-roulette'
GCS_BUCKET_NAME = 'wu-eu-west'
CHANNEL_METADATA_PATH = 'job_parser/channels.json'
MSGS_DATA_PATH = 'job_parser/msgs'
BIGQUERY_DATASET = 'tg_jobs'
BIGQUERY_UPLOAD_STATUS_TABLE = 'upload_msg_status'
BIGQUERY_RAW_MESSAGES_TABLE = 'messages_raw'

MSGS_FILE_PATTERN = re.compile(
    r'^msgs(?P<chat_id>-?\d+)_left_(?P<left>\d+)_right_(?P<right>\d+)\.json$'
)

START_DATE = '2024-04-01'
MAX_LIMIT = 500
ACCOUNT_NAME = 'airflow_account'
BANNED_CHATS = ['-1001709625616', '-1001093436171']

# banned - tg with only links on hh or habr - spam/no traget
# "id": -1001709625616,
# "title": "IT jobs aggregator",
# "username": "it_jobs_agregator",

# banned - every day re-post the same messages - need custom logic
# "id": -1001093436171,
# "title": "Jobs | DevKG"




