import threading
import time
from types import SimpleNamespace
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import app as flask_app


def test_cleanup_preserves_finalizing_sessions(monkeypatch):
    class DummyTimer:
        def __init__(self, interval, function):
            self.interval = interval
            self.function = function

        def start(self):
            return self

        def cancel(self):
            return None

    monkeypatch.setattr(flask_app.threading, "Timer", lambda interval, func: DummyTimer(interval, func))
    monkeypatch.setattr(flask_app, "cleanup_stale_streams", lambda: None)

    delete_calls: list[str] = []

    class TrackingCleaner:
        global_file_index_db_id = None

        def delete_file_from_user_database(self, part_id):
            delete_calls.append(part_id)

        def delete_file_from_index(self, part_id):
            delete_calls.append(f"index:{part_id}")

    now = time.time()
    session = {
        "upload_id": "long-upload",
        "status": "finalizing",
        "uploaded_parts": ["part-1", "part-2"],
        "created_at": now - 7200,
        "last_activity": now - 1800,
    }

    fake_manager = SimpleNamespace(
        upload_lock=threading.Lock(),
        active_uploads={"long-upload": session},
        session_locks={},
        uploader=SimpleNamespace(notion_uploader=TrackingCleaner()),
    )

    monkeypatch.setattr(flask_app, "streaming_upload_manager", fake_manager, raising=False)

    flask_app.cleanup_old_sessions()

    assert "long-upload" in fake_manager.active_uploads
    assert not delete_calls
