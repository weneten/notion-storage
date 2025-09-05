import os
import base64
import hashlib
import importlib.util
from pathlib import Path
import types
import io
import zipfile
import json
import sys
import threading

import pytest

repo_root = Path(__file__).resolve().parents[1]
cu_spec = importlib.util.spec_from_file_location('crypto_utils', repo_root / 'uploader' / 'crypto_utils.py')
cu = importlib.util.module_from_spec(cu_spec)
cu_spec.loader.exec_module(cu)
wrap_file_key = cu.wrap_file_key
encrypt_stream = cu.encrypt_stream

def load_app(monkeypatch):
    uploader_stub = types.ModuleType('uploader')
    class DummyUploader:
        def __init__(self, *a, **k):
            self.pages = {}
            self.files = {}
        def get_user_database_id(self, user_id):
            return 'db'
        def get_user_by_id(self, fid):
            return self.pages[fid]
        def stream_file_from_notion(self, fid, filename, download_url=None):
            yield self.files[fid]
    uploader_stub.NotionFileUploader = DummyUploader
    uploader_stub.ChunkProcessor = object
    uploader_stub.download_s3_file_from_url = lambda url, path: None
    uploader_stub.streaming_uploader = types.ModuleType('streaming_uploader')
    class _SM:
        def __init__(self, *a, **k):
            self.upload_lock = threading.Lock()
            self.active_uploads = {}
            self.session_locks = {}
    uploader_stub.streaming_uploader.StreamingUploadManager = _SM
    uploader_stub.s3_downloader = types.ModuleType('s3_downloader')
    uploader_stub.s3_downloader.cleanup_stale_streams = lambda: None
    uploader_stub.crypto_utils = types.ModuleType('crypto_utils')
    uploader_stub.crypto_utils.decrypt_stream = cu.decrypt_stream
    uploader_stub.crypto_utils.unwrap_file_key = cu.unwrap_file_key
    sys_modules = {
        'uploader': uploader_stub,
        'uploader.streaming_uploader': uploader_stub.streaming_uploader,
        'uploader.s3_downloader': uploader_stub.s3_downloader,
        'uploader.crypto_utils': uploader_stub.crypto_utils,
    }
    monkeypatch.setitem(sys.modules, 'uploader', uploader_stub)
    monkeypatch.setitem(sys.modules, 'uploader.streaming_uploader', uploader_stub.streaming_uploader)
    monkeypatch.setitem(sys.modules, 'uploader.s3_downloader', uploader_stub.s3_downloader)
    monkeypatch.setitem(sys.modules, 'uploader.crypto_utils', uploader_stub.crypto_utils)

    app_spec = importlib.util.spec_from_file_location('app', repo_root / 'app.py')
    app_mod = importlib.util.module_from_spec(app_spec)
    app_spec.loader.exec_module(app_mod)
    return app_mod


def test_download_folder_multiple_link_keys(monkeypatch):
    app_mod = load_app(monkeypatch)
    uploader = app_mod.uploader

    fk1 = os.urandom(32)
    fk2 = os.urandom(32)
    lk1 = os.urandom(32)
    lk2 = os.urandom(32)
    wrapped1 = wrap_file_key(fk1, lk1)
    wrapped2 = wrap_file_key(fk2, lk2)
    nonce1 = os.urandom(12)
    nonce2 = os.urandom(12)
    enc1, tag1 = encrypt_stream(fk1, nonce1, [b'file1'])
    enc2, tag2 = encrypt_stream(fk2, nonce2, [b'file2'])
    ciphertext1 = b''.join(list(enc1))
    ciphertext2 = b''.join(list(enc2))

    props1 = {
        'filename': {'title':[{'text':{'content':'file1'}}]},
        'is_folder': {'checkbox': False},
        'is_visible': {'checkbox': True},
        'is_manifest': {'checkbox': False},
        'folder_path': {'rich_text':[{'text':{'content':'/'}}]},
        'Original Filename': {'title':[{'text':{'content':'file1'}}]},
        'wrapped_file_key': {'rich_text':[{'text':{'content': base64.b64encode(wrapped1).decode()}}]},
        'key_fingerprint': {'rich_text':[{'text':{'content': hashlib.sha256(fk1).hexdigest()}}]},
        'nonce': {'rich_text':[{'text':{'content': base64.b64encode(nonce1).decode()}}]},
        'tag': {'rich_text':[{'text':{'content': base64.b64encode(tag1).decode()}}]},
    }
    props2 = {
        'filename': {'title':[{'text':{'content':'file2'}}]},
        'is_folder': {'checkbox': False},
        'is_visible': {'checkbox': True},
        'is_manifest': {'checkbox': False},
        'folder_path': {'rich_text':[{'text':{'content':'/'}}]},
        'Original Filename': {'title':[{'text':{'content':'file2'}}]},
        'wrapped_file_key': {'rich_text':[{'text':{'content': base64.b64encode(wrapped2).decode()}}]},
        'key_fingerprint': {'rich_text':[{'text':{'content': hashlib.sha256(fk2).hexdigest()}}]},
        'nonce': {'rich_text':[{'text':{'content': base64.b64encode(nonce2).decode()}}]},
        'tag': {'rich_text':[{'text':{'content': base64.b64encode(tag2).decode()}}]},
    }

    uploader.pages = {
        'file1': {'properties': props1},
        'file2': {'properties': props2},
    }
    uploader.files = {'file1': ciphertext1, 'file2': ciphertext2}

    files_data = {'results':[{'id':'file1','properties':props1},{'id':'file2','properties':props2}]}
    monkeypatch.setattr(app_mod, 'get_cached_files', lambda db_id: (files_data, 0))
    monkeypatch.setattr(app_mod, 'fetch_download_metadata', lambda fid, name: {'url':'x','content_type':'','file_size':0})
    monkeypatch.setattr(app_mod, 'current_user', types.SimpleNamespace(id='user'))

    lk_map = {'file1': base64.b64encode(lk1).decode(), 'file2': base64.b64encode(lk2).decode()}
    lk_map_json = json.dumps(lk_map)

    with app_mod.app.test_request_context('/download_folder', query_string={'folder': '/', 'lk_map': lk_map_json}):
        resp = app_mod.download_folder.__wrapped__()
        data = b''.join(resp.response)
    zf = zipfile.ZipFile(io.BytesIO(data))
    assert zf.read('file1') == b'file1'
    assert zf.read('file2') == b'file2'


def test_download_folder_no_link_keys_needed(monkeypatch):
    app_mod = load_app(monkeypatch)
    uploader = app_mod.uploader

    fk = os.urandom(32)
    nonce = os.urandom(12)
    enc, tag = encrypt_stream(fk, nonce, [b'plain'])
    ciphertext = b''.join(list(enc))

    props = {
        'filename': {'title': [{'text': {'content': 'plain'}}]},
        'is_folder': {'checkbox': False},
        'is_visible': {'checkbox': True},
        'is_manifest': {'checkbox': False},
        'folder_path': {'rich_text': [{'text': {'content': '/'}}]},
        'Original Filename': {'title': [{'text': {'content': 'plain'}}]},
        'encryption_key': {'rich_text': [{'text': {'content': base64.b64encode(fk).decode()}}]},
        'nonce': {'rich_text': [{'text': {'content': base64.b64encode(nonce).decode()}}]},
        'tag': {'rich_text': [{'text': {'content': base64.b64encode(tag).decode()}}]},
    }

    uploader.pages = {'plain': {'properties': props}}
    uploader.files = {'plain': ciphertext}

    files_data = {'results': [{'id': 'plain', 'properties': props}]}
    monkeypatch.setattr(app_mod, 'get_cached_files', lambda db_id: (files_data, 0))
    monkeypatch.setattr(app_mod, 'fetch_download_metadata', lambda fid, name: {'url': 'x', 'content_type': '', 'file_size': 0})
    monkeypatch.setattr(app_mod, 'current_user', types.SimpleNamespace(id='user'))

    with app_mod.app.test_request_context('/download_folder?folder=/'):
        resp = app_mod.download_folder.__wrapped__()
        data = b''.join(resp.response)
    zf = zipfile.ZipFile(io.BytesIO(data))
    assert zf.read('plain') == b'plain'


def test_download_folder_missing_link_key(monkeypatch):
    app_mod = load_app(monkeypatch)
    uploader = app_mod.uploader

    fk = os.urandom(32)
    lk = os.urandom(32)
    wrapped = wrap_file_key(fk, lk)
    nonce = os.urandom(12)
    enc, tag = encrypt_stream(fk, nonce, [b'secret'])
    ciphertext = b''.join(list(enc))

    props = {
        'filename': {'title': [{'text': {'content': 'secret'}}]},
        'is_folder': {'checkbox': False},
        'is_visible': {'checkbox': True},
        'is_manifest': {'checkbox': False},
        'folder_path': {'rich_text': [{'text': {'content': '/'}}]},
        'Original Filename': {'title': [{'text': {'content': 'secret'}}]},
        'wrapped_file_key': {'rich_text': [{'text': {'content': base64.b64encode(wrapped).decode()}}]},
        'key_fingerprint': {'rich_text': [{'text': {'content': hashlib.sha256(fk).hexdigest()}}]},
        'nonce': {'rich_text': [{'text': {'content': base64.b64encode(nonce).decode()}}]},
        'tag': {'rich_text': [{'text': {'content': base64.b64encode(tag).decode()}}]},
    }

    uploader.pages = {'secret': {'properties': props}}
    uploader.files = {'secret': ciphertext}

    files_data = {'results': [{'id': 'secret', 'properties': props}]}
    monkeypatch.setattr(app_mod, 'get_cached_files', lambda db_id: (files_data, 0))
    monkeypatch.setattr(app_mod, 'fetch_download_metadata', lambda fid, name: {'url': 'x', 'content_type': '', 'file_size': 0})
    monkeypatch.setattr(app_mod, 'current_user', types.SimpleNamespace(id='user'))

    with app_mod.app.test_request_context('/download_folder?folder=/'):
        resp = app_mod.download_folder.__wrapped__()
    assert resp[1] == 403
