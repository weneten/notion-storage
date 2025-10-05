import base64
import importlib
import os
import sys
import threading
import time
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
s3_dummy.cleanup_stale_streams = _noop
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

    def fake_worker(self, part_session, chunk_queue, part_size, start_event=None):
        if start_event is not None:
            start_event.set()
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


def test_streaming_uploader_waits_for_available_workers(monkeypatch):
    fake = FakeNotionUploader()
    uploader = NotionStreamingUploader(api_token="token", notion_uploader=fake)
    monkeypatch.setattr(NotionStreamingUploader, "SPLIT_THRESHOLD", 4)
    monkeypatch.setattr(NotionStreamingUploader, "SINGLE_PART_THRESHOLD", 4)

    active = 0
    max_active = 0
    active_lock = threading.Lock()
    processed_parts: list[str] = []

    def slow_worker(self, part_session, chunk_queue, part_size, start_event=None):
        nonlocal active, max_active
        if start_event is not None:
            start_event.set()
        local_total = 0
        with active_lock:
            active += 1
            max_active = max(max_active, active)
        try:
            while True:
                chunk = chunk_queue.get()
                if chunk is None:
                    break
                local_total += len(chunk)
                time.sleep(0.05)
            assert local_total == part_size
            processed_parts.append(part_session["filename"])
            return {
                "file_upload_id": f"upload-{part_session['filename']}",
                "file_url": f"https://example.com/{part_session['filename']}",
            }
        finally:
            with active_lock:
                active -= 1

    monkeypatch.setattr(NotionStreamingUploader, "_upload_part_worker", slow_worker, raising=False)
    monkeypatch.setattr(
        NotionStreamingUploader,
        "_upload_to_notion_single_part",
        lambda self, user_database_id, metadata_filename, stream, size: {
            "file_upload_id": "meta-upload",
            "result": {"file": {"url": "https://example.com/meta"}},
        },
        raising=False,
    )

    total_parts = 5
    part_size = 4
    session = uploader.create_upload_session("slowfile", total_parts * part_size, "db1")

    def stream_gen():
        for idx in range(total_parts):
            yield bytes([97 + (idx % 26)]) * part_size

    result = uploader.process_stream(session, stream_gen())

    assert result["status"] == "finalizing"
    assert result["split"] is True
    assert len(result["parts"]) == total_parts
    assert len(processed_parts) == total_parts
    assert max_active <= 3
    assert active == 0


def test_load_user_returns_cached_user_on_notion_error(monkeypatch):
    if "app" in sys.modules:
        app_module = sys.modules["app"]
    else:
        class _NoopTimer:
            def __init__(self, *args, **kwargs):
                pass

            def start(self):
                return None

        monkeypatch.setattr(threading, "Timer", lambda *args, **kwargs: _NoopTimer(*args, **kwargs))
        app_module = importlib.import_module("app")

    cache_user_credentials = app_module.cache_user_credentials
    clear_user_credentials = app_module.clear_user_credentials

    user_id = "user-123"
    username = "alice"
    password_hash = base64.b64encode(b"secret").decode("utf-8")

    cache_user_credentials(user_id, username, password_hash)

    def raiser(*args, **kwargs):
        raise RuntimeError("transient error")

    monkeypatch.setattr(app_module.uploader, "get_user_by_id", raiser)

    try:
        user = app_module.load_user(user_id)
        assert user is not None
        assert user.id == user_id
        assert user.username == username
        assert user.password_hash == password_hash
    finally:
        clear_user_credentials(user_id)


def test_load_user_refreshes_cached_credentials(monkeypatch):
    if "app" in sys.modules:
        app_module = sys.modules["app"]
    else:
        class _NoopTimer:
            def __init__(self, *args, **kwargs):
                pass

            def start(self):
                return None

        monkeypatch.setattr(threading, "Timer", lambda *args, **kwargs: _NoopTimer(*args, **kwargs))
        app_module = importlib.import_module("app")

    cache_user_credentials = app_module.cache_user_credentials
    clear_user_credentials = app_module.clear_user_credentials
    get_cached_user_credentials = app_module.get_cached_user_credentials

    user_id = "user-456"
    cached_username = "old-alice"
    cached_hash = base64.b64encode(b"old-secret").decode("utf-8")
    cache_user_credentials(user_id, cached_username, cached_hash)

    refreshed_username = "new-alice"
    refreshed_hash = base64.b64encode(b"new-secret").decode("utf-8")
    user_data = {
        "properties": {
            "Name": {
                "title": [
                    {
                        "text": {
                            "content": refreshed_username,
                        }
                    }
                ]
            },
            "Password-Hash": {
                "rich_text": [
                    {
                        "text": {
                            "content": refreshed_hash,
                        }
                    }
                ]
            },
        }
    }

    calls = []

    def fake_get_user_by_id(requested_user_id):
        calls.append(requested_user_id)
        return user_data

    monkeypatch.setattr(app_module.uploader, "get_user_by_id", fake_get_user_by_id)

    try:
        user = app_module.load_user(user_id)
        assert calls == [user_id]
        assert user is not None
        assert user.username == refreshed_username
        assert user.password_hash == refreshed_hash
        cached = get_cached_user_credentials(user_id)
        assert cached["username"] == refreshed_username
        assert cached["password_hash"] == refreshed_hash
    finally:
        clear_user_credentials(user_id)


def test_load_user_respects_deletion(monkeypatch):
    if "app" in sys.modules:
        app_module = sys.modules["app"]
    else:
        class _NoopTimer:
            def __init__(self, *args, **kwargs):
                pass

            def start(self):
                return None

        monkeypatch.setattr(threading, "Timer", lambda *args, **kwargs: _NoopTimer(*args, **kwargs))
        app_module = importlib.import_module("app")

    cache_user_credentials = app_module.cache_user_credentials
    clear_user_credentials = app_module.clear_user_credentials
    get_cached_user_credentials = app_module.get_cached_user_credentials

    user_id = "user-789"
    username = "bob"
    password_hash = base64.b64encode(b"password").decode("utf-8")
    cache_user_credentials(user_id, username, password_hash)

    monkeypatch.setattr(app_module.uploader, "get_user_by_id", lambda uid: None)

    try:
        user = app_module.load_user(user_id)
        assert user is None
        assert get_cached_user_credentials(user_id) is None
    finally:
        clear_user_credentials(user_id)
