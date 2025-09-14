import os
import sys
import types

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest

s3_dummy = types.ModuleType("s3_downloader")
def _noop(*args, **kwargs):
    return None
s3_dummy.download_file = _noop
s3_dummy.download_file_from_url = _noop
s3_dummy.stream_file_from_url = _noop
s3_dummy.stream_file_range_from_url = _noop
s3_dummy.cleanup_stale_streams = _noop
sys.modules["uploader.s3_downloader"] = s3_dummy
sys.modules["s3_downloader"] = s3_dummy

import app as flask_app


class DummyUploader:
    def get_file_by_salted_sha512_hash(self, hash_value, force_refresh=True):
        return {
            'properties': {
                'Is Public': {'checkbox': True},
                'File Page ID': {'rich_text': [{'text': {'content': 'page1'}}]},
                'User Database ID': {'rich_text': [{'text': {'content': 'db1'}}]},
                'Original Filename': {'title': [{'text': {'content': 'video.mp4'}}]},
            }
        }

    def get_user_by_id(self, page_id):
        return {'properties': {'filename': {'title': [{'text': {'content': 'video.mp4'}}]}}}


def fake_fetch_download_metadata(page_id, filename):
    return {
        'url': 'http://example.com/video.mp4',
        'file_size': 1000,
        'content_type': 'video/mp4',
    }


def test_head_range_returns_partial_headers(monkeypatch):
    monkeypatch.setattr(flask_app, 'uploader', DummyUploader())
    monkeypatch.setattr(flask_app, 'fetch_download_metadata', fake_fetch_download_metadata)
    client = flask_app.app.test_client()
    resp = client.head('/v/testhash', headers={'Range': 'bytes=100-199'})
    assert resp.status_code == 206
    assert resp.headers['Content-Range'] == 'bytes 100-199/1000'
    assert resp.headers['Content-Length'] == '100'
    assert resp.headers['Accept-Ranges'] == 'bytes'
