import concurrent.futures
import io
import os
import sys
import threading
import time
import types
from types import SimpleNamespace
from pathlib import Path

import pytest


class _DummyClient:
    def download_file(self, *args, **kwargs):
        return None


dummy_boto3 = types.ModuleType('boto3')
dummy_boto3.client = lambda *args, **kwargs: _DummyClient()

dummy_transfer_module = types.ModuleType('boto3.s3.transfer')


class TransferConfig:  # noqa: D401 - minimal stub
    def __init__(self, *args, **kwargs):
        pass


class S3Transfer:
    def __init__(self, client=None, config=None):
        self.client = client or _DummyClient()

    def download_file(self, *args, **kwargs):
        return self.client.download_file(*args, **kwargs)


dummy_transfer_module.TransferConfig = TransferConfig
dummy_transfer_module.S3Transfer = S3Transfer
sys.modules['boto3'] = dummy_boto3
sys.modules['boto3.s3'] = types.ModuleType('boto3.s3')
sys.modules['boto3.s3.transfer'] = dummy_transfer_module

dummy_botocore = types.ModuleType('botocore')
dummy_botocore.UNSIGNED = object()

botocore_config = types.ModuleType('botocore.config')


class Config:  # noqa: D401 - minimal stub
    def __init__(self, *args, **kwargs):
        pass


botocore_config.Config = Config

botocore_exceptions = types.ModuleType('botocore.exceptions')


class NoCredentialsError(Exception):
    pass


botocore_exceptions.NoCredentialsError = NoCredentialsError

sys.modules['botocore'] = dummy_botocore
sys.modules['botocore.config'] = botocore_config
sys.modules['botocore.exceptions'] = botocore_exceptions

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import app as flask_app
import uploader.yt_dlp_importer as importer_module


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


@pytest.fixture(autouse=True)
def stub_importer_dependencies(monkeypatch):
    class DummyUploadManager:
        def __init__(self):
            self.created = []
            self.processed_streams = []

        def create_upload_session(self, **kwargs):
            self.created.append(kwargs)
            return f"upload-{len(self.created)}"

        def process_upload_stream(self, upload_id, stream):  # noqa: D401 - simple stub
            collected = io.BytesIO()
            for chunk in stream:
                collected.write(chunk)
            self.processed_streams.append(
                {
                    'upload_id': upload_id,
                    'size': collected.tell(),
                }
            )
            return {'upload_id': upload_id, 'status': 'completed'}

    dummy_manager = DummyUploadManager()
    monkeypatch.setattr(flask_app, 'ensure_folder_structure', lambda *args, **kwargs: None)
    monkeypatch.setattr(
        flask_app.yt_dlp_importer,
        'ensure_folder_structure',
        lambda *args, **kwargs: None,
        raising=False,
    )
    monkeypatch.setattr(
        flask_app.yt_dlp_importer,
        'upload_manager',
        dummy_manager,
        raising=False,
    )
    monkeypatch.setattr(flask_app.uploader, 'get_user_database_id', lambda user_id: 'test-db')
    yield dummy_manager


@pytest.fixture(autouse=True)
def stub_yt_dlp_binary(monkeypatch):
    monkeypatch.setattr(flask_app.shutil, 'which', lambda exe: '/usr/bin/yt-dlp')
    yield


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


def test_create_job_accepts_url_without_scheme():
    client = flask_app.app.test_client()

    resp = client.post('/api/yt-dlp/jobs', json={'url': 'example.com/video'})

    assert resp.status_code == 201
    payload = resp.get_json()['job']
    assert payload['url'] == 'https://example.com/video'
    assert payload['normalized_command'].endswith('https://example.com/video')


def test_build_command_places_output_before_url(tmp_path):
    importer = importer_module.YtDlpImporter(upload_manager=None, job_registry=None)
    normalized_command = (
        "yt-dlp --newline --output %(title)s.%(ext)s --format best https://example.com/video"
    )

    built = list(importer._build_command(normalized_command, tmp_path.as_posix()))

    assert built[-1] == 'https://example.com/video'
    assert '--output' in built
    assert built.index('--output') < len(built) - 1


def test_create_job_requires_yt_dlp_binary(monkeypatch):
    client = flask_app.app.test_client()
    monkeypatch.setattr(flask_app.shutil, 'which', lambda exe: None)

    resp = client.post(
        '/api/yt-dlp/jobs',
        json={'url': 'https://example.com/video', 'user_database_id': 'test-db'},
    )

    assert resp.status_code == 503
    payload = resp.get_json()
    assert payload['error'].startswith("Executable 'yt-dlp'")
    assert flask_app.yt_dlp_job_registry.list() == []


def test_remote_import_job_executes_with_fake_binary(tmp_path, monkeypatch, stub_importer_dependencies, run_jobs_immediately):
    fake_executable = Path(__file__).parent / 'remote_import' / 'fake_yt_dlp.py'
    bin_dir = tmp_path / 'bin'
    bin_dir.mkdir()
    executable_path = bin_dir / 'yt-dlp'
    executable_path.write_bytes(fake_executable.read_bytes())
    executable_path.chmod(0o755)

    original_path = os.environ.get('PATH', '')
    monkeypatch.setenv('PATH', f"{bin_dir}{os.pathsep}{original_path}")
    monkeypatch.setattr(
        flask_app.shutil,
        'which',
        lambda exe: executable_path.as_posix() if exe == 'yt-dlp' else None,
    )

    client = flask_app.app.test_client()
    response = client.post(
        '/api/yt-dlp/jobs',
        json={'url': 'https://example.com/video', 'user_database_id': 'test-db'},
    )

    assert response.status_code == 201
    job_id = response.get_json()['job']['id']

    snapshot = None
    deadline = time.time() + 10
    while time.time() < deadline:
        snapshot = flask_app.yt_dlp_job_registry.get(job_id)
        if snapshot and snapshot.get('status') == 'completed':
            break
        time.sleep(0.1)
    else:  # pragma: no cover - defensive guard for flakes
        pytest.fail('yt-dlp job did not reach completed state')

    assert snapshot is not None
    progress = snapshot['progress']
    assert progress['stage'] == 'done'
    assert progress['percentage'] == 100.0
    assert progress['upload_percentage'] == 100.0

    log_messages = [entry['message'] for entry in snapshot['logs']]
    assert any(line.startswith('[download]') for line in log_messages)
    assert any('[upload] preparing files' in line for line in log_messages)
    assert any('Discovered 001_fake_video.mp4' in line for line in log_messages)
    assert any('Completed successfully with 2 files' in line for line in log_messages)

    upload_manager = stub_importer_dependencies
    assert flask_app.yt_dlp_importer.upload_manager is upload_manager
    assert [entry['filename'] for entry in upload_manager.created] == [
        '001_fake_video.mp4',
        '002_fake_video.mp4',
    ]
    assert [entry['size'] for entry in upload_manager.processed_streams] == [
        len(b'fake video payload #1'),
        len(b'fake video payload #2'),
    ]


def test_create_job_runs_and_tracks_progress(monkeypatch, run_jobs_immediately, tmp_path):
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

    dispatched = {'value': False}

    class FakeObserver:
        def __init__(self, context):
            self.context = context
            self._thread = threading.Thread(target=self._run, daemon=True)

        def start(self):
            self._thread.start()

        def join(self):
            self._thread.join()

        def _run(self):
            if not dispatched['value']:
                target = self.context.output_dir / 'example.bin'
                target.write_bytes(b'payload')
                self.context.files_queue.put(target)
                self.context.metrics.record_discovery()
                dispatched['value'] = True
            self.context.process_completed.wait()
            self.context.files_queue.put(None)

    def stub_create_observer(self, context):
        return FakeObserver(context)

    monkeypatch.setattr(flask_app.shutil, 'which', lambda exe: '/usr/bin/yt-dlp')
    monkeypatch.setattr(flask_app.subprocess, 'Popen', lambda *args, **kwargs: DummyProcess())
    monkeypatch.setattr(
        flask_app.yt_dlp_importer,
        '_create_directory_observer',
        types.MethodType(stub_create_observer, flask_app.yt_dlp_importer),
        raising=False,
    )

    resp = client.post('/api/yt-dlp/jobs', json={'url': 'https://example.com/video', 'user_database_id': 'test-db'})
    assert resp.status_code == 201
    job_data = resp.get_json()['job']
    job_id = job_data['id']

    assert job_data['status'] == 'completed'
    assert job_data['terminal_state'] == 'success'
    assert job_data['progress']['percentage'] == 100.0
    assert job_data['progress']['stage'] == 'done'

    status_resp = client.get(f'/api/yt-dlp/jobs/{job_id}')
    assert status_resp.status_code == 200
    status_job = status_resp.get_json()['job']
    assert status_job['status'] == 'completed'
    assert status_job['terminal_state'] == 'success'
    assert status_job['progress']['percentage'] == 100.0
    assert status_job['progress']['stage'] == 'done'
    assert status_job['progress']['downloaded_bytes'] >= 10 * 1024 * 1024

    list_resp = client.get('/api/yt-dlp/jobs')
    assert list_resp.status_code == 200
    jobs = list_resp.get_json()['jobs']
    assert any(job['id'] == job_id for job in jobs)


def test_job_resolves_missing_user_database_id(monkeypatch, run_jobs_immediately):
    client = flask_app.app.test_client()

    class DummyProcess:
        def __init__(self):
            self.stdout = io.StringIO('[download] 100% of 1.0MiB in 00:01\n')

        def wait(self):
            return 0

        def poll(self):
            return None

    dispatched = {'value': False}

    class FakeObserver:
        def __init__(self, context):
            self.context = context
            self._thread = threading.Thread(target=self._run, daemon=True)

        def start(self):
            self._thread.start()

        def join(self):
            self._thread.join()

        def _run(self):
            if not dispatched['value']:
                target = self.context.output_dir / 'resolved.bin'
                target.write_bytes(b'content')
                self.context.files_queue.put(target)
                self.context.metrics.record_discovery()
                dispatched['value'] = True
            self.context.process_completed.wait()
            self.context.files_queue.put(None)

    def stub_create_observer(self, context):
        return FakeObserver(context)

    class ResolvingUploadManager:
        def __init__(self):
            self.created = []
            self.processed_streams = []
            self.resolution_calls = []

            def _resolve(user_id):
                self.resolution_calls.append(user_id)
                return 'resolved-db'

            self.notion_uploader = SimpleNamespace(get_user_database_id=_resolve)

        def create_upload_session(self, **kwargs):
            self.created.append(kwargs)
            return f"upload-{len(self.created)}"

        def process_upload_stream(self, upload_id, stream):
            data = io.BytesIO()
            for chunk in stream:
                data.write(chunk)
            self.processed_streams.append({'upload_id': upload_id, 'size': data.tell()})
            return {'upload_id': upload_id, 'status': 'completed'}

    resolving_manager = ResolvingUploadManager()
    monkeypatch.setattr(flask_app.yt_dlp_importer, 'upload_manager', resolving_manager, raising=False)
    monkeypatch.setattr(flask_app.uploader, 'get_user_database_id', lambda user_id: None)
    monkeypatch.setattr(flask_app, 'current_user', SimpleNamespace(id='user-123'))
    monkeypatch.setattr(flask_app.shutil, 'which', lambda exe: '/usr/bin/yt-dlp')
    monkeypatch.setattr(flask_app.subprocess, 'Popen', lambda *args, **kwargs: DummyProcess())
    monkeypatch.setattr(
        flask_app.yt_dlp_importer,
        '_create_directory_observer',
        types.MethodType(stub_create_observer, flask_app.yt_dlp_importer),
        raising=False,
    )

    resp = client.post('/api/yt-dlp/jobs', json={'url': 'https://example.com/video'})
    assert resp.status_code == 201

    job_payload = resp.get_json()['job']
    job_id = job_payload['id']

    status_resp = client.get(f'/api/yt-dlp/jobs/{job_id}')
    assert status_resp.status_code == 200
    status_job = status_resp.get_json()['job']

    assert status_job['status'] == 'completed'
    assert status_job['user_database_id'] == 'resolved-db'
    assert status_job['terminal_state'] == 'success'
    assert resolving_manager.resolution_calls == ['user-123']

def test_cancel_job_marks_cancelled(monkeypatch):
    client = flask_app.app.test_client()

    pending_future = concurrent.futures.Future()

    def submit(func, job_id):
        return pending_future

    monkeypatch.setattr(flask_app, 'yt_dlp_executor', SimpleNamespace(submit=submit))

    resp = client.post('/api/yt-dlp/jobs', json={'url': 'https://example.com/video', 'user_database_id': 'test-db'})
    assert resp.status_code == 201
    job_id = resp.get_json()['job']['id']

    cancel_resp = client.delete(f'/api/yt-dlp/jobs/{job_id}')
    assert cancel_resp.status_code == 200
    cancelled_job = cancel_resp.get_json()['job']
    assert cancelled_job['status'] == 'cancelled'
    assert cancelled_job['terminal_state'] == 'cancelled'
    assert cancelled_job['error']
    assert cancelled_job['progress']['stage'] == 'cancelled'


def test_yt_dlp_sequential_file_processing(
    monkeypatch,
    tmp_path,
    run_jobs_immediately,
    stub_importer_dependencies,
):
    dummy_manager = stub_importer_dependencies

    download_dir = tmp_path / 'downloads'
    download_dir.mkdir()
    contents = [b'alpha', b'beta-data']
    file_paths = []
    for index, data in enumerate(contents, start=1):
        path = download_dir / f'{index:02d}_test.txt'
        path.write_bytes(data)
        file_paths.append(path)

    def fake_tempdir(*args, **kwargs):
        class _TempDir:
            def __enter__(self_inner):
                return str(download_dir)

            def __exit__(self_inner, exc_type, exc, tb):  # noqa: D401 - simple passthrough
                return False

        return _TempDir()

    monkeypatch.setattr(importer_module.tempfile, 'TemporaryDirectory', fake_tempdir)

    deleted_files = []
    original_unlink = importer_module.Path.unlink

    def tracking_unlink(self, *args, **kwargs):
        deleted_files.append(self.name)
        return original_unlink(self, *args, **kwargs)

    monkeypatch.setattr(importer_module.Path, 'unlink', tracking_unlink)

    class FakeObserver:
        def __init__(self, context):
            self.context = context
            self._thread = threading.Thread(target=self._run, daemon=True)

        def start(self):
            self._thread.start()

        def join(self):
            self._thread.join()

        def _run(self):
            for path in file_paths:
                self.context.files_queue.put(path)
                self.context.metrics.record_discovery()
            self.context.process_completed.wait()
            self.context.files_queue.put(None)

    def stub_create_observer(self, context):
        return FakeObserver(context)

    monkeypatch.setattr(
        flask_app.yt_dlp_importer,
        '_create_directory_observer',
        types.MethodType(stub_create_observer, flask_app.yt_dlp_importer),
        raising=False,
    )

    class DummyProcess:
        def __init__(self):
            self.stdout = io.StringIO(
                "[download]  10.0% of 1.0MiB at 1.0MiB/s ETA 00:09\n"
                "[download] 100% of 1.0MiB in 00:01\n"
            )

        def wait(self):
            return 0

        def poll(self):
            return None

    monkeypatch.setattr(flask_app.shutil, 'which', lambda exe: '/usr/bin/yt-dlp')
    monkeypatch.setattr(flask_app.subprocess, 'Popen', lambda *args, **kwargs: DummyProcess())

    client = flask_app.app.test_client()
    response = client.post(
        '/api/yt-dlp/jobs',
        json={'url': 'https://example.com/video', 'user_database_id': 'test-db'},
    )
    assert response.status_code == 201
    job_payload = response.get_json()['job']

    assert job_payload['status'] == 'completed'
    assert job_payload['progress']['stage'] == 'done'
    assert job_payload['progress'].get('files_completed') == len(file_paths)
    assert [entry['filename'] for entry in dummy_manager.created] == [p.name for p in file_paths]
    assert [entry['size'] for entry in dummy_manager.processed_streams] == [len(data) for data in contents]
    assert deleted_files == [p.name for p in file_paths]

    status_response = client.get(f"/api/yt-dlp/jobs/{job_payload['id']}")
    assert status_response.status_code == 200
    fetched_job = status_response.get_json()['job']
    assert fetched_job['status'] == 'completed'
    assert fetched_job['progress']['stage'] == 'done'
    assert fetched_job['progress'].get('files_completed') == len(file_paths)


def test_job_fails_when_no_files_downloaded(monkeypatch, run_jobs_immediately, stub_importer_dependencies):
    client = flask_app.app.test_client()

    class DummyProcess:
        def __init__(self):
            self.stdout = io.StringIO('[download] 100% of 1.0MiB in 00:01\n')

        def wait(self):
            return 0

        def poll(self):
            return None

    class IdleObserver:
        def __init__(self, context):
            self.context = context
            self._thread = threading.Thread(target=self._run, daemon=True)

        def start(self):
            self._thread.start()

        def join(self):
            self._thread.join()

        def _run(self):
            self.context.process_completed.wait()
            self.context.files_queue.put(None)

    monkeypatch.setattr(flask_app.shutil, 'which', lambda exe: '/usr/bin/yt-dlp')
    monkeypatch.setattr(flask_app.subprocess, 'Popen', lambda *args, **kwargs: DummyProcess())
    monkeypatch.setattr(
        flask_app.yt_dlp_importer,
        '_create_directory_observer',
        types.MethodType(lambda self, context: IdleObserver(context), flask_app.yt_dlp_importer),
        raising=False,
    )

    response = client.post(
        '/api/yt-dlp/jobs',
        json={'url': 'https://example.com/video', 'user_database_id': 'test-db'},
    )

    assert response.status_code == 201
    job_payload = response.get_json()['job']

    assert job_payload['status'] == 'failed'
    assert job_payload['terminal_state'] == 'failure'
    assert job_payload['progress']['stage'] == 'failed'
    assert 'without downloading any files' in (job_payload.get('error') or '').lower()
