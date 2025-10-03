import os
import sys
import threading
import types
from typing import Any, Dict, Optional

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

s3_dummy = types.ModuleType("s3_downloader")


def _noop(*args, **kwargs):
    return None


s3_dummy.download_file = _noop
s3_dummy.download_file_from_url = _noop
s3_dummy.stream_file_from_url = _noop
s3_dummy.stream_file_range_from_url = _noop
sys.modules["uploader.s3_downloader"] = s3_dummy
sys.modules["s3_downloader"] = s3_dummy

import pytest

from uploader.streaming_uploader import NotionStreamingUploader


class FakeNotionUploader:
    def __init__(self, fail_plan: Optional[Dict[str, int]] = None):
        self.fail_plan = fail_plan or {}
        self.pages: Dict[str, Dict[str, Any]] = {}
        self.global_file_index_db_id = None
        self._lock = threading.Lock()

    def add_file_to_user_database(
        self,
        database_id: str,
        filename: str,
        file_size: int,
        file_hash: str,
        file_upload_id: str,
        is_public: bool = False,
        salt: str | None = None,
        original_filename: str | None = None,
        file_url: str | None = None,
        folder_path: str | None = None,
        is_manifest: bool = False,
    ) -> Dict[str, Any]:
        remaining = self.fail_plan.get(filename, 0)
        if remaining:
            if remaining == -1:
                raise RuntimeError("forced failure")
            self.fail_plan[filename] = remaining - 1
            raise RuntimeError("transient failure")

        page_id = f"page-{len(self.pages) + 1}-{filename}"
        entry = {
            "id": page_id,
            "properties": {
                "file_data": {
                    "files": [
                        {
                            "type": "file_upload",
                            "file_upload": {
                                "id": file_upload_id,
                            },
                        }
                    ]
                }
            },
        }
        with self._lock:
            self.pages[page_id] = entry
        return entry

    def get_user_by_id(self, page_id: str) -> Dict[str, Any] | None:
        return self.pages.get(page_id)

    def update_user_properties(self, page_id: str, properties: Dict[str, Any]) -> None:
        with self._lock:
            page = self.pages.get(page_id)
            if not page:
                return
            page.setdefault("properties", {}).update(properties)

    def add_file_to_index(self, *args, **kwargs):
        return None

    def ensure_database_property(self, *args, **kwargs):
        return None


@pytest.fixture
def uploader(monkeypatch):
    fake = FakeNotionUploader()
    uploader = NotionStreamingUploader(api_token="token", notion_uploader=fake)
    monkeypatch.setattr(NotionStreamingUploader, "SPLIT_THRESHOLD", 4)
    monkeypatch.setattr(NotionStreamingUploader, "SINGLE_PART_THRESHOLD", 4)

    def fake_worker(self, part_session, chunk_queue, part_size):
        data = b""
        while True:
            chunk = chunk_queue.get()
            if chunk is None:
                break
            data += chunk
        assert len(data) == part_size
        return {
            "file_upload_id": f"upload-{part_session['filename']}",
            "file_url": f"https://example.com/{part_session['filename']}",
        }

    def fake_upload_single(self, user_database_id, metadata_filename, stream, size):
        return {
            "file_upload_id": "meta-upload",
            "result": {"file": {"url": "https://example.com/meta"}},
        }

    monkeypatch.setattr(NotionStreamingUploader, "_upload_part_worker", fake_worker, raising=False)
    monkeypatch.setattr(NotionStreamingUploader, "_upload_to_notion_single_part", fake_upload_single, raising=False)
    return uploader, fake


def test_process_stream_retries_transient_database_errors(uploader):
    uploader_instance, fake = uploader
    fake.fail_plan["bigfile.part1"] = 2

    session = uploader_instance.create_upload_session("bigfile", 8, "db1")

    def stream_gen():
        yield b"aaaa"
        yield b"bbbb"

    result = uploader_instance.process_stream(session, stream_gen())

    assert result["status"] == "finalizing"
    assert result["split"] is True
    assert len(result["parts"]) == 2
    assert fake.fail_plan["bigfile.part1"] == 0
    stored_filenames = {page["id"] for page in fake.pages.values()}
    assert any("bigfile.part1" in page_id for page_id in stored_filenames)
    assert any("bigfile.part2" in page_id for page_id in stored_filenames)


def test_process_stream_aborts_on_persistent_database_errors(uploader):
    uploader_instance, fake = uploader
    fake.fail_plan["bigfile.part1"] = -1

    session = uploader_instance.create_upload_session("bigfile", 8, "db1")

    def stream_gen():
        yield b"aaaa"
        yield b"bbbb"

    with pytest.raises(RuntimeError):
        uploader_instance.process_stream(session, stream_gen())

    # Upload should abort without creating a manifest entry
    ids = list(fake.pages.keys())
    assert all(".file.json" not in page_id for page_id in ids)
    assert all("bigfile.part1" not in page_id for page_id in ids)
