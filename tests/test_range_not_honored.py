import io
import os
import sys
import types
import importlib.util
import pytest
import requests

# Create minimal stubs for boto3/botocore dependencies used during import
class _DummyBoto3(types.ModuleType):
    def client(self, *args, **kwargs):
        class _C:
            pass
        return _C()

sys.modules.setdefault("boto3", _DummyBoto3("boto3"))
_transfer_mod = types.ModuleType("boto3.s3.transfer")
class _DummyTransfer:
    def __init__(self, *a, **k):
        pass
_transfer_mod.S3Transfer = _DummyTransfer
_transfer_mod.TransferConfig = lambda *a, **k: None
sys.modules.setdefault("boto3.s3.transfer", _transfer_mod)

botocore_mod = types.ModuleType("botocore")
botocore_mod.UNSIGNED = None
sys.modules.setdefault("botocore", botocore_mod)

botocore_ex = types.ModuleType("botocore.exceptions")
botocore_ex.NoCredentialsError = Exception
sys.modules.setdefault("botocore.exceptions", botocore_ex)

sys.modules.setdefault("botocore.config", types.ModuleType("botocore.config"))
sys.modules["botocore.config"].Config = lambda *a, **k: None

spec = importlib.util.spec_from_file_location(
    "s3_downloader", os.path.join(os.path.dirname(__file__), "..", "uploader", "s3_downloader.py")
)
s3 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(s3)

class DummyResp:
    def __init__(self):
        self.status_code = 200
        self.headers = {}
        self._data = io.BytesIO(b"0" * 1000)
    def iter_content(self, chunk_size):
        while True:
            data = self._data.read(chunk_size)
            if not data:
                break
            yield data
    def close(self):
        pass
    def raise_for_status(self):
        pass


def test_stream_file_range_requires_206(monkeypatch):
    def fake_get(url, headers=None, stream=False):
        return DummyResp()
    monkeypatch.setattr(s3, "_SESSION", types.SimpleNamespace(get=fake_get))
    stream = s3.stream_file_range_from_url("http://example.com/file", 100, 199)
    with pytest.raises(requests.HTTPError):
        next(iter(stream))
