import os
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest
import app as flask_app


@pytest.fixture(autouse=True)
def disable_login(monkeypatch):
    original = flask_app.app.config.get('LOGIN_DISABLED', False)
    flask_app.app.config['LOGIN_DISABLED'] = True
    try:
        yield
    finally:
        flask_app.app.config['LOGIN_DISABLED'] = original


@pytest.fixture
def heartbeat_session():
    upload_id = 'heartbeat-test'
    now = time.time() - 120
    session = {
        'upload_id': upload_id,
        'status': 'uploading',
        'filename': 'bigfile.bin',
        'file_size': 1024,
        'bytes_uploaded': 256,
        'is_multipart': False,
        'created_at': now,
        'last_activity': now,
    }
    with flask_app.streaming_upload_manager.upload_lock:
        flask_app.streaming_upload_manager.active_uploads[upload_id] = session
    try:
        yield upload_id, session
    finally:
        with flask_app.streaming_upload_manager.upload_lock:
            flask_app.streaming_upload_manager.active_uploads.pop(upload_id, None)
            flask_app.streaming_upload_manager.session_locks.pop(upload_id, None)


def test_upload_heartbeat_updates_last_activity(heartbeat_session):
    upload_id, session = heartbeat_session
    client = flask_app.app.test_client()

    initial_activity = session['last_activity']

    resp = client.post(f'/api/upload/heartbeat/{upload_id}')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data['status'] == 'ok'
    assert data['heartbeat_count'] == 1
    assert session['last_activity'] >= initial_activity

    second_activity = session['last_activity']
    resp = client.post(f'/api/upload/heartbeat/{upload_id}')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data['heartbeat_count'] == 2
    assert session['last_activity'] >= second_activity


def test_upload_heartbeat_missing_session_returns_404():
    client = flask_app.app.test_client()
    resp = client.post('/api/upload/heartbeat/unknown-upload')
    assert resp.status_code == 404
