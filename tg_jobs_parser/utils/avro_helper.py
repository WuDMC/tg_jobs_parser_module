import os
import fastavro
import json_helper
# CREATE TABLE `geo-roulette.test_avro.manual_table2`
# (
#   id INT64,
#   text STRING,
#   datetime DATETIME,
#   date DATE,
#   link STRING,
#   user STRING,
#   user_name STRING,
#   chat_id INT64
# )
# PARTITION BY date
# CLUSTER BY chat_id, id;

MSG_SCHEMA = {
  "type": "record",
  "name": "ManualTable2Record",
  "namespace": "geo_roulette.test_avro",
  "fields": [
    {
      "name": "id",
      "type": "long"
    },
    {
      "name": "text",
      "type": "string"
    },
    {
      "name": "datetime",
      "type": "string"
    },
    {
      "name": "date",
      "type": "string"
    },
    {
      "name": "link",
      "type": "string"
    },
    {
      "name": "user",
      "type": ["null", "string"],
      "default": None
    },
    {
      "name": "user_name",
      "type": ["null", "string"],
      "default": None
    },
    {
      "name": "chat_id",
      "type": "long"
    }
  ]
}




def save_to_avro(json_filename):
    messages = json_helper.read_json(json_filename)
    base_name, current_extension = os.path.splitext(json_filename)
    avro_filename = base_name + '.' + 'avro'
    with open(avro_filename, 'wb') as out:
        parsed_schema = fastavro.parse_schema(MSG_SCHEMA)
        fastavro.writer(out, parsed_schema, messages)


save_to_avro('/home/wudmc/PycharmProjects/tg_parser/volume/msgs-1001017371671_left_9400_right_9411.json')
