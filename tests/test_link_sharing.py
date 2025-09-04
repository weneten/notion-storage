import os
import base64
import hashlib
import importlib.util
from pathlib import Path
import types
import pytest
import sys
import urllib.parse

repo_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(repo_root))

# Load crypto utils for wrapping/unwrapping
cu_spec = importlib.util.spec_from_file_location('crypto_utils', repo_root / 'uploader' / 'crypto_utils.py')
cu = importlib.util.module_from_spec(cu_spec)
cu_spec.loader.exec_module(cu)
wrap_file_key = cu.wrap_file_key
encrypt_stream = cu.encrypt_stream
decrypt_stream = cu.decrypt_stream
unwrap_file_key = cu.unwrap_file_key

# Minimal NotionFileUploader replacement
class DummyNotionFileUploader:
    last_properties = None
    def __init__(self, *a, **k):
        pass
    def add_file_to_user_database(self, database_id, filename, file_size, file_hash, file_upload_id, encryption_meta=None, **kwargs):
        props = {}
        if encryption_meta:
            props['wrapped_file_key'] = {'rich_text':[{'text':{'content': encryption_meta.get('wrapped_fk_b64','none')}}]}
            props['key_fingerprint'] = {'rich_text':[{'text':{'content': encryption_meta.get('key_fingerprint','none')}}]}
            props['nonce'] = {'rich_text':[{'text':{'content': encryption_meta.get('nonce_b64','none')}}]}
            props['tag'] = {'rich_text':[{'text':{'content': encryption_meta.get('tag_b64','none')}}]}
        DummyNotionFileUploader.last_properties = props
        return {'id':'1','properties':props}

# Stub uploader package for importing app
uploader_stub = types.ModuleType('uploader')
uploader_stub.NotionFileUploader = DummyNotionFileUploader
uploader_stub.ChunkProcessor = object
def download_s3_file_from_url(url, path):
    pass
uploader_stub.download_s3_file_from_url = download_s3_file_from_url
uploader_stub.streaming_uploader = types.ModuleType('streaming_uploader')
uploader_stub.streaming_uploader.StreamingUploadManager = object
uploader_stub.s3_downloader = types.ModuleType('s3_downloader')
uploader_stub.s3_downloader.cleanup_stale_streams = lambda: None
uploader_stub.crypto_utils = types.ModuleType('crypto_utils')
uploader_stub.crypto_utils.decrypt_stream = cu.decrypt_stream
uploader_stub.crypto_utils.unwrap_file_key = cu.unwrap_file_key
sys.modules.update({
    'uploader': uploader_stub,
    'uploader.streaming_uploader': uploader_stub.streaming_uploader,
    'uploader.s3_downloader': uploader_stub.s3_downloader,
    'uploader.crypto_utils': uploader_stub.crypto_utils,
})

import requests

def test_wrapped_key_saved(monkeypatch):
    captured = {}
    def fake_post(url, json=None, headers=None):
        captured['json'] = json
        class Resp:
            status_code = 200
            def json(self):
                return {'id': '1'}
        return Resp()
    monkeypatch.setattr(requests, 'post', fake_post)
    uploader = DummyNotionFileUploader()
    enc_meta = {
        'alg': 'AES-GCM',
        'nonce_b64': 'AA==',
        'tag_b64': 'AA==',
        'wrapped_fk_b64': 'Zm9v',
        'key_fingerprint': 'deadbeef'
    }
    uploader.add_file_to_user_database('db','f.txt',1,'hash','fid',encryption_meta=enc_meta)
    props = DummyNotionFileUploader.last_properties
    assert 'wrapped_file_key' in props
    assert 'encryption_key' not in props

def test_extract_requires_link_key():
    uploader_stub = types.ModuleType('uploader')
    uploader_stub.NotionFileUploader = DummyNotionFileUploader
    uploader_stub.ChunkProcessor = object
    def download_s3_file_from_url(url, path):
        pass
    uploader_stub.download_s3_file_from_url = download_s3_file_from_url
    uploader_stub.streaming_uploader = types.ModuleType('streaming_uploader')
    class _SM:
        def __init__(self, *a, **k):
            pass
    uploader_stub.streaming_uploader.StreamingUploadManager = _SM
    uploader_stub.s3_downloader = types.ModuleType('s3_downloader')
    uploader_stub.s3_downloader.cleanup_stale_streams = lambda: None
    uploader_stub.crypto_utils = types.ModuleType('crypto_utils')
    uploader_stub.crypto_utils.decrypt_stream = decrypt_stream
    uploader_stub.crypto_utils.unwrap_file_key = unwrap_file_key
    sys.modules.update({
        'uploader': uploader_stub,
        'uploader.streaming_uploader': uploader_stub.streaming_uploader,
        'uploader.s3_downloader': uploader_stub.s3_downloader,
        'uploader.crypto_utils': uploader_stub.crypto_utils,
    })

    app_spec = importlib.util.spec_from_file_location('app', repo_root / 'app.py')
    app_mod = importlib.util.module_from_spec(app_spec)
    app_spec.loader.exec_module(app_mod)
    app = app_mod.app
    extract_encryption_params = app_mod.extract_encryption_params

    fk = os.urandom(32)
    lk = os.urandom(32)
    wrapped = wrap_file_key(fk, lk)
    nonce = os.urandom(12)
    plaintext = b'hello'
    enc_iter, tag = encrypt_stream(fk, nonce, [plaintext])
    ciphertext = b''.join(list(enc_iter))
    props = {
        'wrapped_file_key': {'rich_text':[{'text':{'content': base64.b64encode(wrapped).decode()}}]},
        'key_fingerprint': {'rich_text':[{'text':{'content': hashlib.sha256(fk).hexdigest()}}]},
        'nonce': {'rich_text':[{'text':{'content': base64.b64encode(nonce).decode()}}]},
        'tag': {'rich_text':[{'text':{'content': base64.b64encode(tag).decode()}}]},
    }
    with app.test_request_context('/download'):
        with pytest.raises(PermissionError):
            extract_encryption_params(props)
    lk_b64 = base64.b64encode(lk).decode()
    lk_q = urllib.parse.quote(lk_b64)
    with app.test_request_context(f'/download?lk={lk_q}'):
        key, n, t = extract_encryption_params(props)
        decrypted = b''.join(decrypt_stream(key, n, t, [ciphertext]))
        assert decrypted == plaintext
