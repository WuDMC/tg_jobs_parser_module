import pytest
from tg_jobs_parser.google_cloud_helper.bigquery_manager import BigQueryManager


@pytest.fixture
def bigquery_manager():
    return BigQueryManager()


def test_load_data_from_gcs(bigquery_manager):
    # Add test logic here
    pass
