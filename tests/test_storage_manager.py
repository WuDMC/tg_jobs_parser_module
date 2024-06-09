import pytest
from google_cloud_helper.storage_manager import StorageManager

@pytest.fixture
def storage_manager():
    return StorageManager()

def test_upload_file(storage_manager):
    # Add test logic here
    pass

def test_download_file(storage_manager):
    # Add test logic here
    pass
