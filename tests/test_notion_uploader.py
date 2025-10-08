import os
import sys
import types

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

s3_dummy = types.ModuleType("s3_downloader")


def _noop(*args, **kwargs):
    return None


s3_dummy.download_file = _noop
s3_dummy.download_file_from_url = _noop
s3_dummy.stream_file_from_url = _noop
s3_dummy.stream_file_range_from_url = _noop
s3_dummy.cleanup_stale_streams = _noop
sys.modules["uploader.s3_downloader"] = s3_dummy
sys.modules["s3_downloader"] = s3_dummy

from uploader.notion_uploader import NotionFileUploader


class _FakeResponse:
    def __init__(self, status_code=200, json_data=None, text="", headers=None):
        self.status_code = status_code
        self._json = json_data or {}
        self.text = text
        self.headers = headers or {}

    def json(self):
        if self._json is None:
            raise ValueError("No JSON")
        return self._json


def test_send_file_part_retries_invalid_multipart(monkeypatch):
    uploader = NotionFileUploader(api_token="token")

    calls = []

    responses = [
        _FakeResponse(
            status_code=400,
            text="{\"object\":\"error\",\"status\":400,\"code\":\"validation_error\",\"message\":\"Invalid `multipart/form-data` request\"}"
        ),
        _FakeResponse(
            status_code=200,
            json_data={"object": "file_upload", "id": "file-part"}
        ),
    ]

    def fake_post(url, headers, files, timeout):
        file_entry = files["file"]
        assert isinstance(file_entry, tuple)
        file_obj = file_entry[1]
        body = file_obj.read()
        calls.append(body)
        response = responses.pop(0)
        if response.status_code != 200:
            response._json = None
        return response

    monkeypatch.setattr("requests.post", fake_post)

    result = uploader._send_file_part_with_retry(
        file_upload_id="upload-id",
        part_number=1,
        chunk_data=b"a" * 5,
        filename="file.txt",
        content_type="text/plain",
        bytes_uploaded_so_far=0,
        total_bytes=5,
        total_parts=1,
        session_id="session",
        max_retries=3,
    )

    assert result["id"] == "file-part"
    # Ensure two attempts were made and each attempt received the full payload
    assert len(calls) == 2
    assert calls[0] == b"a" * 5
    assert calls[1] == b"a" * 5


def test_send_file_part_respects_retry_after_on_rate_limit(monkeypatch):
    uploader = NotionFileUploader(api_token="token")

    calls = []
    sleep_calls = []

    responses = [
        _FakeResponse(
            status_code=429,
            text="{\"object\":\"error\",\"code\":\"rate_limited\"}",
            headers={"Retry-After": "2"},
        ),
        _FakeResponse(
            status_code=200,
            json_data={"object": "file_upload", "id": "file-part"}
        ),
    ]

    def fake_post(url, headers, files, timeout):
        file_entry = files["file"]
        file_obj = file_entry[1]
        calls.append(file_obj.read())
        response = responses.pop(0)
        if response.status_code != 200:
            response._json = None
        return response

    def fake_sleep(delay):
        sleep_calls.append(delay)

    monkeypatch.setattr("requests.post", fake_post)
    monkeypatch.setattr("time.sleep", fake_sleep)

    result = uploader._send_file_part_with_retry(
        file_upload_id="upload-id",
        part_number=1,
        chunk_data=b"b" * 5,
        filename="file.txt",
        content_type="text/plain",
        bytes_uploaded_so_far=0,
        total_bytes=5,
        total_parts=1,
        session_id="session",
        max_retries=3,
    )

    assert result["id"] == "file-part"
    assert len(calls) == 2
    assert calls[0] == b"b" * 5
    assert calls[1] == b"b" * 5
    assert sleep_calls == [2.0]
