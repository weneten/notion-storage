import time
from types import SimpleNamespace

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from uploader.streaming_uploader import StreamingUploadManager


class ControlledTimer:
    instances = []

    def __init__(self, interval, function):
        self.interval = interval
        self.function = function
        self.cancelled = False
        self.daemon = True
        ControlledTimer.instances.append(self)

    def start(self):
        return self

    def fire(self):
        if not self.cancelled:
            self.function()

    def cancel(self):
        self.cancelled = True


class DummyNotionClient:
    def __init__(self, record_index_deletes=True):
        self.deleted_from_user = []
        self.deleted_from_index = []
        self.global_file_index_db_id = "index-db" if record_index_deletes else None

    def delete_file_from_user_database(self, part_id):
        self.deleted_from_user.append(part_id)

    def delete_file_from_index(self, part_id):
        self.deleted_from_index.append(part_id)


def _build_manager(dummy_client: DummyNotionClient) -> StreamingUploadManager:
    manager = StreamingUploadManager("token", notion_uploader=dummy_client)
    manager._timer_factory = lambda interval, func: ControlledTimer(interval, func)
    return manager


def test_orphan_cleanup_deletes_parts_after_delay():
    client = DummyNotionClient()
    manager = _build_manager(client)
    ControlledTimer.instances.clear()

    upload_id = "upload-1"
    manager.active_uploads[upload_id] = {
        "upload_id": upload_id,
        "status": "failed",
        "uploaded_parts": ["part-a", "part-b"],
        "failed_at": time.time(),
    }

    manager._schedule_orphan_cleanup(upload_id)

    assert ControlledTimer.instances, "Timer was not scheduled"
    timer = ControlledTimer.instances[-1]
    assert timer.interval == manager._orphan_cleanup_delay_seconds

    # Simulate timer firing
    timer.fire()

    assert client.deleted_from_user == ["part-a", "part-b"]
    assert client.deleted_from_index == ["part-a", "part-b"]
    assert manager.active_uploads[upload_id]["uploaded_parts"] == []


def test_orphan_cleanup_skipped_when_status_changes():
    client = DummyNotionClient()
    manager = _build_manager(client)
    ControlledTimer.instances.clear()

    upload_id = "upload-2"
    manager.active_uploads[upload_id] = {
        "upload_id": upload_id,
        "status": "failed",
        "uploaded_parts": ["part-a"],
        "failed_at": time.time(),
    }

    manager._schedule_orphan_cleanup(upload_id)
    timer = ControlledTimer.instances[-1]

    # Status flips to processing before the timer fires (e.g., resumed upload)
    manager.active_uploads[upload_id]["status"] = "processing"
    timer.fire()

    assert client.deleted_from_user == []
    assert manager.active_uploads[upload_id]["uploaded_parts"] == ["part-a"]


def test_rescheduling_cancels_previous_timer():
    client = DummyNotionClient(record_index_deletes=False)
    manager = _build_manager(client)

    ControlledTimer.instances.clear()

    upload_id = "upload-3"
    manager.active_uploads[upload_id] = {
        "upload_id": upload_id,
        "status": "failed",
        "uploaded_parts": ["part-a"],
        "failed_at": time.time(),
    }

    ControlledTimer.instances.clear()

    manager._schedule_orphan_cleanup(upload_id)
    first_timer = ControlledTimer.instances[-1]

    manager._schedule_orphan_cleanup(upload_id)
    second_timer = ControlledTimer.instances[-1]

    assert first_timer.cancelled is True
    assert second_timer is not first_timer

    second_timer.fire()

    # Only user DB deletes should be recorded when no global index is configured
    assert client.deleted_from_user == ["part-a"]
    assert client.deleted_from_index == []
    assert manager.active_uploads[upload_id]["uploaded_parts"] == []
