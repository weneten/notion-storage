import io
import os
import sys
import requests

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import types

s3_dummy = types.ModuleType("s3_downloader")
def _noop(*args, **kwargs):
    return None
s3_dummy.download_file = _noop
s3_dummy.download_file_from_url = _noop
s3_dummy.stream_file_from_url = _noop
s3_dummy.stream_file_range_from_url = _noop
sys.modules["uploader.s3_downloader"] = s3_dummy
sys.modules["s3_downloader"] = s3_dummy

from uploader.notion_uploader import NotionFileUploader


class DummyUploader(NotionFileUploader):
    """Test double that avoids network calls."""
    def __init__(self):
        self.headers = {}
        self.base_url = ""
        self.session = requests.Session()
        self.socketio = None
        self.files = {}
        self.uploads = {}

    def get_user_database_id(self, user_id):
        return "db1"

    def ensure_txt_filename(self, filename):
        return filename

    def get_mime_type(self, filename):
        return "text/plain"

    def create_file_upload(self, content_type, filename, mode="single_part", number_of_parts=None):
        upload_id = "upload1"
        url = f"http://example.com/{upload_id}"
        self.uploads[upload_id] = b""
        return {"id": upload_id, "file": {"url": url}, "upload_url": url}

    def send_file_content(self, file_upload_id, stream_buffer, content_type, filename, file_size):
        data = stream_buffer.read()
        self.uploads[file_upload_id] = data
        return {"file": {"url": f"http://example.com/{file_upload_id}"}}

    def add_file_to_user_database(
        self,
        database_id,
        filename,
        file_size,
        file_hash,
        file_upload_id,
        is_public=False,
        salt="",
        original_filename=None,
        file_url=None,
        is_manifest=False,
        folder_path="/",
        password_hash=None,
        expires_at=None,
    ):
        page_id = "page1"
        # Simulate Notion storing size as 0 initially
        self.files[page_id] = {
            "id": page_id,
            "properties": {
                "filesize": {"number": 0},
                "file_data": {"files": [{"name": "file.txt", "file": {"url": file_url}}]},
            },
        }
        # Re-fetch and update as production code would
        page_info = self.get_user_by_id(page_id)
        if page_info.get("properties", {}).get("filesize", {}).get("number", 0) == 0:
            computed = file_size or self.get_file_size_from_url(file_url)
            self.update_file_entry(page_id, {"filesize": {"number": computed}})
        return self.files[page_id]

    def get_user_by_id(self, page_id):
        return self.files.get(page_id)

    def update_file_entry(self, page_id, properties):
        self.files[page_id]["properties"].update(properties)
        return self.files[page_id]

    def get_file_size_from_url(self, url):
        upload_id = url.rsplit("/", 1)[-1]
        return len(self.uploads.get(upload_id, b""))

    def get_content_type_from_filename(self, filename):
        return "text/plain"


def test_get_file_download_metadata_returns_size():
    uploader = DummyUploader()
    data = b"hello world"
    stream = [data]
    result = uploader.upload_file_stream(stream, "sample.txt", "user1", len(data))
    page_id = result["page_id"]
    metadata = uploader.get_file_download_metadata(page_id, "sample.txt")
    assert metadata["file_size"] == len(data)
