import json
from datetime import datetime
import logging
import os

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def read_json(file_path):
    if not os.path.exists(file_path):
        logging.info(f"cant find {file_path}")
        return None
    with open(file_path, "r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            logging.info(f"json error {file_path}")
            return None
        except KeyError:
            logging.info(f"key error {file_path}")
            return None
        except IndexError:
            logging.info(f"index error {file_path}")
            return None


def save_to_json(data, path):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

        logging.info(f"File saved successfully: {path}")
        return True

    except Exception as e:
        logging.error(f"Failed to save file: {path}, Error: {e}")
        return False


def save_to_line_delimited_json(data, path):
    try:
        with open(path, "w", encoding="utf-8") as f:
            for item in data:
                json.dump(item, f, ensure_ascii=False)
                f.write("\n")

        logging.info(f"File saved successfully: {path}")
        return True

    except Exception as e:
        logging.error(f"Failed to save file: {path}, Error: {e}")
        return False


def make_row_msg_status(file, match, status):
    chat_id = match.group("chat_id")
    left = match.group("left")
    right = match.group("right")
    time_created = file.get("time_created", None)

    row = {
        "filename": file["name"],
        "path": file["full_path"],
        "status": status,
        "chat_id": str(chat_id),
        "from": int(left),
        "to": int(right),
        "size": file.get("size", None),
        "created_date": (
            time_created.date().isoformat() if time_created else None
        ),  # STRING in format YYYY-MM-DD
        "created_at": time_created.isoformat() if time_created else None,  # TIMESTAMP
        "updated_at": datetime.now().isoformat(),  # TIMESTAMP
    }
    return row


def read_line_delimeted_json(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        return [json.loads(line) for line in f]


def recursive_merge(dict1, dict2):
    for key in dict2:
        if (
            key in dict1
            and isinstance(dict1[key], dict)
            and isinstance(dict2[key], dict)
        ):
            recursive_merge(dict1[key], dict2[key])
        else:
            dict1[key] = dict2[key]


def merge_json_files(file1_path, file2_path, output_path):
    json1 = read_json(file1_path)
    json2 = read_json(file2_path)
    if json1 is None or json2 is None:
        logging.info(" one of file (or both) is empty")
        return

    merged_json = json1.copy()
    recursive_merge(merged_json, json2)
    with open(output_path, "w", encoding="utf-8") as output_file:
        json.dump(merged_json, output_file, ensure_ascii=False, indent=2)


def update_uploaded_borders(file_path_1, file_path_2, output_path):
    # file_path_1 << file_path_2
    try:
        data1 = read_json(file_path_1)
        data2 = read_json(file_path_2)
        for key, values in data2.items():
            if key in data1:
                logging.info(
                    f"checking1 data before merge targets {data1[key].get('target_id')} <<=>> {data1[key]['last_posted_message_id']}"
                )
                logging.info(
                    f"checking2 data before merge current stats {data1[key].get('left_saved_id')} <<=>>  {data1[key].get('right_saved_id')}"
                )
                logging.info(
                    f"checking3 data before merge right {values['new_left_saved_id']} <<=>> {values['new_right_saved_id']}"
                )

                if (
                    data1[key].get("left_saved_id") is None
                    and data1[key].get("right_saved_id") is None
                ):
                    logging.info(
                        f"Updating left_saved_id for {key} from {data1[key].get('left_saved_id')} to {values['new_left_saved_id']}"
                    )
                    data1[key]["left_saved_id"] = values["new_left_saved_id"]
                    logging.info(
                        f"Updating right_saved_id for {key} from {data1[key].get('right_saved_id')} to {values['new_right_saved_id']}"
                    )
                    data1[key]["right_saved_id"] = values["new_right_saved_id"]
                elif (
                    values["new_left_saved_id"] < data1[key]["left_saved_id"]
                    and values["new_right_saved_id"] == data1[key]["left_saved_id"] + 1
                ):
                    logging.info(
                        f"Updating left_saved_id for {key} from {data1[key].get('left_saved_id')} to {values['new_left_saved_id']}"
                    )
                    data1[key]["left_saved_id"] = values["new_left_saved_id"]
                elif (
                    values["new_right_saved_id"] > data1[key]["right_saved_id"]
                    and values["new_left_saved_id"] == data1[key]["right_saved_id"] + 1
                ):
                    logging.info(
                        f"Updating right_saved_id for {key} from {data1[key].get('right_saved_id')} to {values['new_right_saved_id']}"
                    )
                    data1[key]["right_saved_id"] = values["new_right_saved_id"]
                elif (
                    values["new_right_saved_id"] <= data1[key]["right_saved_id"]
                    and values["new_left_saved_id"] >= data1[key]["left_saved_id"]
                ):
                    logging.error(
                        f"File was already uploaded. cloud data {data1[key]} with file data {values}"
                    )
                else:
                    logging.error(f"uncovered case {data1[key]} with {values}")
        return save_to_json(data1, output_path)
    except Exception as e:
        logging.error(
            f"Failed to update borders with file1: {file_path_1} << {file_path_2} Error: {e}"
        )
        return False


# def set_target_ids(tg_channels, cloud_channels, msg_parser, black_list, date, force):
#     for ch_id in tg_channels:
#         try:
#             tg_channel = tg_channels.get(ch_id, {})
#             cloud_channel = cloud_channels.get(ch_id, {})
#             if ch_id in black_list:
#                 cloud_channel["status"] = "bad"
#                 logging.info(f"{ch_id} is BANNED - skip")
#                 continue
#             logging.info(f"getting target date for {ch_id} ")
#             if tg_channel["type"] == "ChatType.BOT":
#                 cloud_channel["status"] = "bad"
#                 logging.info(f"{ch_id} is bot - skip")
#                 continue
#             if tg_channel["type"] == "ChatType.PRIVATE":
#                 cloud_channel["status"] = "bad"
#                 logging.info(f"{ch_id} is PRIVATE - skip")
#                 continue
#
#             if "target_date" in cloud_channel and force is False:
#                 logging.info(f"for {ch_id} target date already setted")
#                 continue
#
#             if ("last_posted_message_id" in tg_channel and
#                     "right_saved_id" in cloud_channel and
#                     tg_channel["last_posted_message_id"] and
#                     cloud_channel["right_saved_id"] and
#                     tg_channel["last_posted_message_id"] < cloud_channel["right_saved_id"]):
#                 logging.info(f"{ch_id} was cleared - update last id")
#                 tg_channel["last_posted_message_id"] = cloud_channel["right_saved_id"]
#
#             cloud_channel.setdefault("target_date", None)
#             msgs_data = msg_parser.run_msg_parser(ch_id, date)
#             if msgs_data:
#                 cloud_channel["left_saved_id"] = None
#                 cloud_channel["right_saved_id"] = None
#                 cloud_channel["target_id"] = msgs_data[0]
#                 cloud_channel["target_date"] = msgs_data[1]
#                 cloud_channel["status"] = "ok"
#             else:
#                 cloud_channel["status"] = "bad"
#         except Exception as e:
#             raise Exception(f"Error json helper: {e}")

