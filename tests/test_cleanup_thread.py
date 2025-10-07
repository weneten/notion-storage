import threading
import time
from types import ModuleType, SimpleNamespace

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


def test_cleanup_does_not_block_active_uploads(monkeypatch):
    import sys

    # Ensure clean imports for the heavily patched app module
    for name in [
        "app",
        "uploader",
        "uploader.streaming_uploader",
        "uploader.s3_downloader",
    ]:
        sys.modules.pop(name, None)

    fake_uploader = ModuleType("uploader")
    fake_uploader.__path__ = []  # Mark as package for submodule imports

    class DummyNotionFileUploader:
        def __init__(self, *args, **kwargs):
            self.notion_uploader = SimpleNamespace(
                delete_file_from_user_database=lambda part_id: None,
                delete_file_from_index=lambda part_id: None,
                global_file_index_db_id=None,
            )

    class DummyChunkProcessor:
        pass

    class DummyStreamingUploadManager:
        def __init__(self, *args, **kwargs):
            self.upload_lock = threading.Lock()
            self.active_uploads = {}
            self.session_locks = {}
            notion_uploader = kwargs.get("notion_uploader")
            if notion_uploader is None:
                notion_uploader = SimpleNamespace(
                    notion_uploader=SimpleNamespace(
                        delete_file_from_user_database=lambda part_id: None,
                        delete_file_from_index=lambda part_id: None,
                        global_file_index_db_id=None,
                    )
                )
            self.uploader = notion_uploader

    fake_streaming_module = ModuleType("uploader.streaming_uploader")
    fake_streaming_module.StreamingUploadManager = DummyStreamingUploadManager
    fake_streaming_module.NotionStreamingUploader = object

    fake_s3_module = ModuleType("uploader.s3_downloader")
    fake_s3_module.cleanup_stale_streams = lambda: None
    fake_s3_module.download_file_from_url = lambda *args, **kwargs: None

    fake_uploader.NotionFileUploader = DummyNotionFileUploader
    fake_uploader.ChunkProcessor = DummyChunkProcessor
    fake_uploader.download_s3_file_from_url = lambda *args, **kwargs: None
    fake_uploader.StreamingUploadManager = DummyStreamingUploadManager

    monkeypatch.setitem(sys.modules, "uploader", fake_uploader)
    monkeypatch.setitem(sys.modules, "uploader.streaming_uploader", fake_streaming_module)
    monkeypatch.setitem(sys.modules, "uploader.s3_downloader", fake_s3_module)

    import app

    class DummyTimer:
        def __init__(self, interval, function):
            self.interval = interval
            self.function = function

        def start(self):
            return self

    monkeypatch.setattr(app.threading, "Timer", lambda interval, func: DummyTimer(interval, func))
    monkeypatch.setattr(app, "cleanup_stale_streams", lambda: None)

    slow_delete_started = threading.Event()
    allow_delete_to_finish = threading.Event()

    class SlowNotionCleaner:
        global_file_index_db_id = None

        def delete_file_from_user_database(self, part_id):
            slow_delete_started.set()
            # Block until the test signals that cleanup work can finish
            allow_delete_to_finish.wait(timeout=2)

        def delete_file_from_index(self, part_id):
            pass

    fake_manager = SimpleNamespace(
        upload_lock=threading.Lock(),
        active_uploads={},
        session_locks={}
    )

    expired_session = {
        "status": "completed",
        "uploaded_parts": ["part-1"],
        "completed_at": time.time() - 120,
        "created_at": time.time() - 120,
    }

    fake_manager.active_uploads["expired"] = expired_session
    fake_manager.uploader = SimpleNamespace(notion_uploader=SlowNotionCleaner())

    monkeypatch.setattr(app, "streaming_upload_manager", fake_manager, raising=False)

    cleanup_thread = threading.Thread(target=app.cleanup_old_sessions)
    cleanup_thread.start()

    upload_lock_acquired = threading.Event()

    def simulate_upload():
        slow_delete_started.wait()
        with fake_manager.upload_lock:
            upload_lock_acquired.set()

    uploader_thread = threading.Thread(target=simulate_upload)
    uploader_thread.start()

    try:
        assert slow_delete_started.wait(timeout=1), "Cleanup did not reach deletion step"
        assert upload_lock_acquired.wait(timeout=0.2), "Upload thread blocked waiting for cleanup"
    finally:
        allow_delete_to_finish.set()
        cleanup_thread.join(timeout=1)
        uploader_thread.join(timeout=1)

    assert "expired" not in fake_manager.active_uploads
