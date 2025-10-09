import io
import os
import sys
import types
import importlib.util

import requests


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
    "s3_downloader",
    os.path.join(os.path.dirname(__file__), "..", "uploader", "s3_downloader.py"),
)
s3 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(s3)


class _FailingResponse:
    def __init__(self, payload: bytes, *, status_code: int = 200, fail_after_first: bool = False):
        self.status_code = status_code
        self._payload = payload
        self._fail_after_first = fail_after_first

    def iter_content(self, chunk_size: int):
        stream = io.BytesIO(self._payload)
        while True:
            chunk = stream.read(chunk_size)
            if not chunk:
                break
            yield chunk
            if self._fail_after_first:
                self._fail_after_first = False
                raise requests.exceptions.ConnectionError("socket closed")

    def close(self):
        pass

    def raise_for_status(self):
        pass


def test_stream_resumes_after_disconnect(monkeypatch):
    data = b"abcdefghij"
    responses = [
        _FailingResponse(data, fail_after_first=True),
        _FailingResponse(data[4:], status_code=206),
    ]
    seen_headers = []

    def fake_get(url, headers=None, stream=False):
        assert responses, "unexpected additional request"
        seen_headers.append(headers)
        return responses.pop(0)

    monkeypatch.setattr(s3, "_SESSION", types.SimpleNamespace(get=fake_get))
    monkeypatch.setattr(s3, "time", types.SimpleNamespace(time=lambda: 0, sleep=lambda _=0: None))

    stream = s3.stream_file_from_url("http://example.com/file", chunk_size=4)
    collected = b"".join(stream)

    assert collected == data
    assert seen_headers[0] is None
    assert seen_headers[1]["Range"] == "bytes=4-"
