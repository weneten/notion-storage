import sys
import types


# Stub out s3_downloader dependencies required by app import
s3_dummy = types.ModuleType("s3_downloader")
def _noop(*args, **kwargs):
    return None
s3_dummy.cleanup_stale_streams = _noop
s3_dummy.download_file = _noop
s3_dummy.download_file_from_url = _noop
s3_dummy.stream_file_from_url = _noop
s3_dummy.stream_file_range_from_url = _noop
sys.modules["uploader.s3_downloader"] = s3_dummy
sys.modules["s3_downloader"] = s3_dummy

from app import calculate_manifest_total_size


def test_calculate_manifest_total_size_sums_parts():
    manifest = {
        'total_size': 9999,
        'parts': [
            {'size': 100},
            {'size': 200},
            {'size': 300},
        ]
    }
    assert calculate_manifest_total_size(manifest) == 600

def test_calculate_manifest_total_size_empty_parts():
    assert calculate_manifest_total_size({}) == 0
