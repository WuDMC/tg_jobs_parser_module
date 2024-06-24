from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from tg_jobs_parser.configs import GoogleCloudConfig, vars
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class BigQueryManager:
    def __init__(self, project_id):
        self.config = GoogleCloudConfig()
        self.client = bigquery.Client()

    def load_json_to_bigquery(self, json_file_path, table_id):
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=True
        )
        with open(json_file_path, "rb") as source_file:
            job = self.client.load_table_from_file(source_file, table_id, job_config=job_config)
        job.result()  # Waits for the job to complete.
        table = self.client.get_table(table_id)  # Make an API request.
        logging.info(
            "Tabgle got {} rows and {} columns. Table name is {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )

    def load_json_uri_to_bigquery(self, path, table_id):
        uri = f'gs://{vars.GCS_BUCKET_NAME}/{path}'
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=True
        )
        try:
            job = self.client.load_table_from_uri(
                            uri, table_id, job_config=job_config
                        )
            job.result()  # Waits for the job to complete.
            table = self.client.get_table(table_id)  # Make an API request.
            logging.info(
                "Tabgle got {} rows and {} columns. Table name is {}".format(
                    table.num_rows, len(table.schema), table_id
                )
            )
            return True
        except Exception as e:
            logging.error(f'Error: {e}')
            return False

    def check_connection(self):
        try:
            QUERY = (
                'SELECT name FROM `bigquery-public-data.usa_names.usa_1910_2013` '
                'WHERE state = "TX" '
                'LIMIT 100')
            query_job = self.client.query(QUERY)  # API request
            rows = query_job.result()  # Waits for query to finish
            for row in rows:
                logging.info(row.name)
            return True
        except NotFound:
            return False

    def fetch_data(self, dataset_id, table_id, selected_fields='*',
                   date=None, status=None, chat_id=None):
        query = f"SELECT distinct({selected_fields}) FROM `{self.client.project}.{dataset_id}.{table_id}`"
        conditions = []
        if date:
            conditions.append(f"created_date = '{date}'")
        if status:
            conditions.append(f"status = '{status}'")
        if chat_id:
            conditions.append(f"chat_id = '{chat_id}'")
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query_job = self.client.query(query)
        return [dict(row) for row in query_job]

    def query_msg_files_statuses(self, dataset_id, table_id):
        query = f"""
        WITH LatestStatus AS (
            SELECT filename, status, updated_at,
                   ROW_NUMBER() OVER (PARTITION BY filename ORDER BY updated_at DESC) AS rn
            FROM `{self.client.project}.{dataset_id}.{table_id}`
        )
        SELECT filename, status
        FROM LatestStatus
        WHERE rn = 1
        """
        query_job = self.client.query(query)
        return [dict(row) for row in query_job]
