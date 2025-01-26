from tg_jobs_parser.configs import vars
from dotenv import load_dotenv
import os
import yaml
import json

load_dotenv()
volume_folder_path = os.path.join(os.getenv("TG_PARSER_HOME"), vars.VOLUME_DIR)


class Config:
    def __init__(self, config_file=None, debug=False):
        current_dir = os.path.dirname(__file__)
        if debug:
            default_config_file = os.path.join(current_dir, "config_debug.yaml")
        else:
            default_config_file = os.path.join(current_dir, "config.yaml")
        self.config_file = config_file if config_file else default_config_file
        self.config = self.load_config()
        self.interpolate_config()

    def load_config(self):
        with open(self.config_file, "r") as file:
            return yaml.safe_load(file)

    def interpolate_config(self):
        for section, settings in self.config.items():
            for key, value in settings.items():
                if (
                        isinstance(value, str)
                        and value.startswith("${")
                        and value.endswith("}")
                ):
                    env_var = value[2:-1]
                    self.config[section][key] = os.getenv(env_var)

    def get(self, section, key):
        return self.config.get(section, {}).get(key)


DEFAULT_CHANNELS_BLOB = "job_parser/channels.json"
DEFAULT_MSG_BLOB = "job_parser/msgs"


class GoogleCloudConfig(Config):
    def __init__(self, config_file=None, debug=False):
        super().__init__(config_file, debug)
        self.bucket_name = self.get("google_cloud", "bucket_name")
        self.source_channels_blob = (
                                        self.get("google_cloud", "source_channels_blob")
                                    ) or DEFAULT_CHANNELS_BLOB
        self.source_msg_blob = (
                                   self.get("google_cloud", "source_msg_blob")
                               ) or DEFAULT_MSG_BLOB
        self.project = self.get("google_cloud", "project")
        self.dataset = self.get("google_cloud", "bq_dataset")
        self.table_msg = self.get("google_cloud", "bq_table_msgs")
        self.table_status = self.get("google_cloud", "bq_table_status")


DEFAULT_ACCOUNT_NAME = "default_tg_client"


class TelegramConfig(Config):
    def __init__(self, config_file=None, debug=False):
        super().__init__(config_file, debug)
        self.session_string = self.get("telegram", "session_string")
        self.api_id = self.get("telegram", "api_id")
        self.api_hash = self.get("telegram", "api_hash")
        self.max_limit = self.get("telegram", "max_limit") or 500
        self.account_name = self.get("telegram", "account_name") or DEFAULT_ACCOUNT_NAME
        self.black_list = json.loads(self.get("telegram", "black_list")) or []
        self.white_list = json.loads(self.get("telegram", "white_list")) or []
