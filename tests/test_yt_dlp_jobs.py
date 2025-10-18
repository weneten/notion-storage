import concurrent.futures
import io
import os
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import app as flask_app


@pytest.fixture(autouse=True)
def disable_login(monkeypatch):
    original = flask_app.app.config.get('LOGIN_DISABLED', False)
    flask_app.app.config['LOGIN_DISABLED'] = True
    try:
        yield
    finally:
        flask_app.app.config['LOGIN_DISABLED'] = original


@pytest.fixture(autouse=True)
def clear_job_registry():
    with flask_app.yt_dlp_job_registry._lock:  # pylint: disable=protected-access
        flask_app.yt_dlp_job_registry._jobs.clear()  # pylint: disable=protected-access
    yield
    with flask_app.yt_dlp_job_registry._lock:  # pylint: disable=protected-access
        flask_app.yt_dlp_job_registry._jobs.clear()  # pylint: disable=protected-access


@pytest.fixture
def run_jobs_immediately(monkeypatch):
    def submit(func, job_id):
        future = concurrent.futures.Future()
        try:
            func(job_id)
        except Exception as exc:  # pragma: no cover - defensive, ensures Future has an exception set
            future.set_exception(exc)
        else:
            future.set_result(None)
        return future

    monkeypatch.setattr(flask_app, 'yt_dlp_executor', SimpleNamespace(submit=submit))


def test_create_job_requires_url():
    client = flask_app.app.test_client()
    resp = client.post('/api/yt-dlp/jobs', json={})
    assert resp.status_code == 400
    assert 'error' in resp.get_json()


def test_create_job_runs_and_tracks_progress(monkeypatch, run_jobs_immediately):
    client = flask_app.app.test_client()

    class DummyProcess:
        def __init__(self):
            self.stdout = io.StringIO(
                "[download]  50.0% of 10.0MiB at 5.0MiB/s ETA 00:01\n"
                "[download] 100% of 10.0MiB in 00:02\n"
            )

        def wait(self):
            return 0

        def poll(self):
            return None

    monkeypatch.setattr(flask_app.shutil, 'which', lambda exe: '/usr/bin/yt-dlp')
    monkeypatch.setattr(flask_app.subprocess, 'Popen', lambda *args, **kwargs: DummyProcess())

    resp = client.post('/api/yt-dlp/jobs', json={'url': 'https://example.com/video'})
    assert resp.status_code == 201
    job_data = resp.get_json()['job']
    job_id = job_data['id']

    assert job_data['status'] == 'completed'
    assert job_data['progress']['percentage'] == 100.0

    status_resp = client.get(f'/api/yt-dlp/jobs/{job_id}')
    assert status_resp.status_code == 200
    status_job = status_resp.get_json()['job']
    assert status_job['status'] == 'completed'
    assert status_job['progress']['percentage'] == 100.0
    assert status_job['progress']['downloaded_bytes'] >= 10 * 1024 * 1024

    list_resp = client.get('/api/yt-dlp/jobs')
    assert list_resp.status_code == 200
    jobs = list_resp.get_json()['jobs']
    assert any(job['id'] == job_id for job in jobs)


def test_cancel_job_marks_cancelled(monkeypatch):
    client = flask_app.app.test_client()

    pending_future = concurrent.futures.Future()

    def submit(func, job_id):
        return pending_future

    monkeypatch.setattr(flask_app, 'yt_dlp_executor', SimpleNamespace(submit=submit))

    resp = client.post('/api/yt-dlp/jobs', json={'url': 'https://example.com/video'})
    assert resp.status_code == 201
    job_id = resp.get_json()['job']['id']

    cancel_resp = client.delete(f'/api/yt-dlp/jobs/{job_id}')
    assert cancel_resp.status_code == 200
    cancelled_job = cancel_resp.get_json()['job']
    assert cancelled_job['status'] == 'cancelled'
    assert cancelled_job['error']
