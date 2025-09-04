import os
import base64
import types
from pathlib import Path
import importlib.util
import sys

repo_root = Path(__file__).resolve().parents[1]
for name in [
    'uploader',
    'uploader.notion_uploader',
    'uploader.s3_downloader',
    'uploader.crypto_utils',
    's3_downloader',
    'crypto_utils',
]:
    sys.modules.pop(name, None)

uploader_pkg = types.ModuleType('uploader')
uploader_pkg.__path__ = [str(repo_root / 'uploader')]
sys.modules['uploader'] = uploader_pkg

nu_spec = importlib.util.spec_from_file_location('uploader.notion_uploader', repo_root / 'uploader' / 'notion_uploader.py')
nu = importlib.util.module_from_spec(nu_spec)
nu_spec.loader.exec_module(nu)
sys.modules['uploader.notion_uploader'] = nu
sys.modules['uploader'].notion_uploader = nu
NotionFileUploader = nu.NotionFileUploader

cu_spec = importlib.util.spec_from_file_location('uploader.crypto_utils', repo_root / 'uploader' / 'crypto_utils.py')
cu = importlib.util.module_from_spec(cu_spec)
cu_spec.loader.exec_module(cu)
sys.modules['uploader.crypto_utils'] = cu
sys.modules['uploader'].crypto_utils = cu
encrypt_stream = cu.encrypt_stream


def test_stream_multi_part_file_decrypts_without_link_key(monkeypatch):
    file_key = os.urandom(32)
    data = b'hello world' * 100
    nonce = os.urandom(12)
    enc_iter, tag = encrypt_stream(file_key, nonce, [data])
    ciphertext = b"".join(enc_iter)

    manifest = {
        "parts": [
            {
                "part_number": 1,
                "filename": "part1",
                "file_hash": "hash1",
                "size": len(ciphertext),
                "nonce_b64": base64.b64encode(nonce).decode(),
                "tag_b64": base64.b64encode(tag).decode(),
            }
        ]
    }

    # Patch _fetch_json to return our manifest
    monkeypatch.setattr(nu, '_fetch_json', lambda url, key=None, nonce=None, tag=None: manifest)

    uploader = NotionFileUploader(api_token='token')

    # Stub out methods used by stream_multi_part_file
    def fake_get_user_by_id(self, page_id):
        return {
            'properties': {
                'file_data': {'files': [{'file': {'url': 'unused'}}]},
                'nonce': {'rich_text': [{'text': {'content': 'none'}}]},
                'tag': {'rich_text': [{'text': {'content': 'none'}}]},
            }
        }

    def fake_get_file_by_hash(self, file_hash):
        return {
            'properties': {
                'File Page ID': {'rich_text': [{'text': {'content': 'part1'}}]}
            }
        }

    def fake_stream_file_from_notion(self, page_id, filename, download_url=None, chunk_size=None):
        return [ciphertext]

    def fake_stream_range(self, page_id, filename, start, end, download_url=None, chunk_size=None):
        return [ciphertext[start:end+1]]

    uploader.get_user_by_id = types.MethodType(fake_get_user_by_id, uploader)
    uploader.get_file_by_salted_sha512_hash = types.MethodType(fake_get_file_by_hash, uploader)
    uploader.stream_file_from_notion = types.MethodType(fake_stream_file_from_notion, uploader)
    uploader.stream_file_from_notion_range = types.MethodType(fake_stream_range, uploader)

    output = b"".join(uploader.stream_multi_part_file('manifest', file_key=file_key))
    assert output == data


def test_stream_multi_part_file_partial_range(monkeypatch):
    file_key = os.urandom(32)
    data = b'hello world' * 100
    nonce = os.urandom(12)
    enc_iter, tag = encrypt_stream(file_key, nonce, [data])
    ciphertext = b"".join(enc_iter)

    manifest = {
        "parts": [
            {
                "part_number": 1,
                "filename": "part1",
                "file_hash": "hash1",
                "size": len(ciphertext),
                "nonce_b64": base64.b64encode(nonce).decode(),
                "tag_b64": base64.b64encode(tag).decode(),
            }
        ]
    }

    monkeypatch.setattr(nu, '_fetch_json', lambda url, key=None, nonce=None, tag=None: manifest)

    uploader = NotionFileUploader(api_token='token')

    def fake_get_user_by_id(self, page_id):
        return {
            'properties': {
                'file_data': {'files': [{'file': {'url': 'unused'}}]},
                'nonce': {'rich_text': [{'text': {'content': 'none'}}]},
                'tag': {'rich_text': [{'text': {'content': 'none'}}]},
            }
        }

    def fake_get_file_by_hash(self, file_hash):
        return {
            'properties': {
                'File Page ID': {'rich_text': [{'text': {'content': 'part1'}}]}
            }
        }

    def fake_stream_file_from_notion(self, page_id, filename, download_url=None, chunk_size=None):
        return [ciphertext]

    def fake_stream_range(*args, **kwargs):
        raise AssertionError('range streaming not expected for encrypted parts')

    uploader.get_user_by_id = types.MethodType(fake_get_user_by_id, uploader)
    uploader.get_file_by_salted_sha512_hash = types.MethodType(fake_get_file_by_hash, uploader)
    uploader.stream_file_from_notion = types.MethodType(fake_stream_file_from_notion, uploader)
    uploader.stream_file_from_notion_range = types.MethodType(fake_stream_range, uploader)

    start = 10
    end = 150
    output = b"".join(
        uploader.stream_multi_part_file('manifest', start=start, end=end, file_key=file_key)
    )
    assert output == data[start:end + 1]
