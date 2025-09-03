import os
import sys
from pathlib import Path
import importlib.util
import types

repo_root = Path(__file__).resolve().parents[1]

# Create a dummy 'uploader' package to satisfy relative imports without executing __init__
# Stub package and dependencies to avoid heavy imports
uploader_pkg = types.ModuleType('uploader')
uploader_pkg.__path__ = [str(repo_root / 'uploader')]
sys.modules['uploader'] = uploader_pkg

dummy_notion = types.ModuleType('uploader.notion_uploader')
class NotionFileUploader:
    pass
dummy_notion.NotionFileUploader = NotionFileUploader
sys.modules['uploader.notion_uploader'] = dummy_notion

dummy_s3 = types.ModuleType('uploader.s3_downloader')
def download_file_from_url(url, path):
    raise NotImplementedError
dummy_s3.download_file_from_url = download_file_from_url
sys.modules['uploader.s3_downloader'] = dummy_s3

su_path = repo_root / 'uploader' / 'streaming_uploader.py'
spec = importlib.util.spec_from_file_location('uploader.streaming_uploader', su_path)
su = importlib.util.module_from_spec(spec)
spec.loader.exec_module(su)
NotionStreamingUploader = su.NotionStreamingUploader

cu_path = repo_root / 'uploader' / 'crypto_utils.py'
cu_spec = importlib.util.spec_from_file_location('uploader.crypto_utils', cu_path)
cu = importlib.util.module_from_spec(cu_spec)
cu_spec.loader.exec_module(cu)
encrypt_stream = cu.encrypt_stream
decrypt_stream = cu.decrypt_stream


class DummyNotionUploader:
    def __init__(self):
        self.uploaded_bytes = None
        self.encryption_meta = None

    def upload_single_file_stream(self, file_stream, filename, database_id, content_type,
                                   file_size, original_filename, encryption_meta=None):
        chunks = list(file_stream)
        if encryption_meta:
            fk = encryption_meta['file_key']
            nonce = os.urandom(12)
            chunks_iter, tag = encrypt_stream(fk, nonce, iter(chunks))
            chunks = list(chunks_iter)
            encryption_meta.setdefault('parts', []).append({'nonce': nonce, 'tag': tag})
        self.uploaded_bytes = b"".join(chunks)
        self.encryption_meta = encryption_meta
        return {'file_upload_id': '1', 'result': {}}


def test_single_part_stream_encrypted():
    dummy = DummyNotionUploader()
    uploader = NotionStreamingUploader(api_token='token', notion_uploader=dummy)
    data = b'hello world' * 1000
    session = uploader.create_upload_session('test.txt', len(data), 'db')
    uploader._process_single_part_stream(session, iter([data]), db_integration=False)

    encrypted = dummy.uploaded_bytes
    assert encrypted != data
    enc = session['encryption_meta']
    part = enc['parts'][0]
    decrypted = b"".join(decrypt_stream(enc['file_key'], part['nonce'], part['tag'], [encrypted]))
    assert decrypted == data


def test_multipart_stream_encrypted(monkeypatch):
    capture = {}

    class DummyParallelProcessor:
        def __init__(self, max_workers=10, notion_uploader=None, upload_session=None, socketio=None):
            capture['data'] = b''

        def process_stream(self, stream_generator, resume_from=0):
            for chunk in stream_generator:
                capture['data'] += chunk
            return {'file_upload_id': 'id', 'status': 'completed', 'result': {}}

    monkeypatch.setattr(su, 'ParallelChunkProcessor', DummyParallelProcessor)

    uploader = NotionStreamingUploader(api_token='token', notion_uploader=None)
    data = b'A' * (1024 * 1024)
    session = uploader.create_upload_session('big.bin', len(data), 'db')
    uploader._process_multipart_stream(session, iter([data]), db_integration=False)

    encrypted = capture['data']
    assert encrypted != data
    enc = session['encryption_meta']
    part = enc['parts'][0]
    decrypted = b"".join(decrypt_stream(enc['file_key'], part['nonce'], part['tag'], [encrypted]))
    assert decrypted == data
