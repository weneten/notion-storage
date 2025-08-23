"""Utility for downloading objects from S3 with concurrency and retries.

This module provides a ``download_file`` helper that wraps ``boto3``'s
:class:`~boto3.s3.transfer.S3Transfer` to perform multipart downloads with
configurable concurrency. Each part download is retried with exponential
backoff to help recover from transient throttling errors.

The maximum concurrency can be configured via the ``MAX_S3_CONCURRENCY``
environment variable (default: ``10``). This value also controls the size of
the shared S3 client's connection pool used for all transfers.
"""

import os
import time
import atexit
import gc
import threading
from typing import Optional
import weakref
from urllib.parse import urlparse

import boto3
import requests
from requests.adapters import HTTPAdapter
from boto3.s3.transfer import S3Transfer, TransferConfig
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import NoCredentialsError

# Number of attempts for each part download (includes exponential backoff)
_NUM_DOWNLOAD_ATTEMPTS = 5

# Default concurrency for S3 transfers and connection pools
_MAX_S3_CONCURRENCY = int(os.getenv("MAX_S3_CONCURRENCY", "10"))

# Shared S3 clients to avoid repeated initialization
_S3_CLIENT = boto3.client(
    "s3", config=Config(max_pool_connections=_MAX_S3_CONCURRENCY)
)
_ANON_S3_CLIENT = boto3.client(
    "s3",
    config=Config(
        signature_version=UNSIGNED, max_pool_connections=_MAX_S3_CONCURRENCY
    ),
)

# Reusable transfers for the default concurrency
_TRANSFER_CONFIG = TransferConfig(
    multipart_threshold=8 * 1024 * 1024,
    max_concurrency=_MAX_S3_CONCURRENCY,
    num_download_attempts=_NUM_DOWNLOAD_ATTEMPTS,
)
_S3_TRANSFER = S3Transfer(client=_S3_CLIENT, config=_TRANSFER_CONFIG)
_ANON_S3_TRANSFER = S3Transfer(client=_ANON_S3_CLIENT, config=_TRANSFER_CONFIG)

# Shared HTTP session for downloading pre-signed URLs
_SESSION = requests.Session()
_SESSION.mount(
    "https://",
    HTTPAdapter(
        pool_connections=_MAX_S3_CONCURRENCY, pool_maxsize=_MAX_S3_CONCURRENCY
    ),
)
atexit.register(_SESSION.close)

# Registry to track active presigned URL streams
_ACTIVE_STREAMS = weakref.WeakKeyDictionary()
_ACTIVE_STREAMS_LOCK = threading.Lock()


def _get_transfer(concurrency: int, *, anonymous: bool = False) -> S3Transfer:
    """Return an ``S3Transfer`` for the desired concurrency."""

    if concurrency == _MAX_S3_CONCURRENCY:
        return _ANON_S3_TRANSFER if anonymous else _S3_TRANSFER

    transfer_config = TransferConfig(
        multipart_threshold=8 * 1024 * 1024,
        max_concurrency=concurrency,
        num_download_attempts=_NUM_DOWNLOAD_ATTEMPTS,
    )
    client = _ANON_S3_CLIENT if anonymous else _S3_CLIENT
    return S3Transfer(client=client, config=transfer_config)


def download_file(bucket: str, key: str, dest: str, concurrency: Optional[int] = None) -> None:
    """Download an object from S3 to ``dest``.

    Parameters
    ----------
    bucket:
        Name of the S3 bucket.
    key:
        Key of the object within the bucket.
    dest:
        Local filesystem path where the object should be stored.
    concurrency:
        Optional override for maximum concurrent S3 requests. If ``None``,
        the value is read from the ``MAX_S3_CONCURRENCY`` environment variable
        (default: ``10``).
    """
    if concurrency is None:
        concurrency = _MAX_S3_CONCURRENCY

    transfer = _get_transfer(concurrency)
    try:
        transfer.download_file(bucket, key, dest)
    except NoCredentialsError:
        # Fallback to unsigned requests for publicly accessible objects
        anon_transfer = _get_transfer(concurrency, anonymous=True)
        anon_transfer.download_file(bucket, key, dest)


def _parse_s3_url(url: str) -> tuple[str, str]:
    """Extract bucket and key from an S3 URL."""
    parsed = urlparse(url)
    netloc_parts = parsed.netloc.split(".")
    path = parsed.path.lstrip("/")

    if netloc_parts[0] == "s3":
        # Path-style URL: s3.amazonaws.com/bucket/key
        parts = path.split("/", 1)
        bucket = parts[0]
        key = parts[1] if len(parts) > 1 else ""
    else:
        # Virtual-hosted-style URL: bucket.s3.amazonaws.com/key
        bucket = netloc_parts[0]
        key = path

    return bucket, key


def download_file_from_url(url: str, dest: str, concurrency: Optional[int] = None) -> None:
    """Download an S3 object specified by URL to ``dest``.

    If ``url`` contains pre-signed query parameters, the object is fetched
    directly via HTTP so the signature is preserved. Otherwise the helper
    extracts the bucket/key pair and delegates to :func:`download_file` for
    concurrent downloads via ``boto3``.
    """
    parsed = urlparse(url)
    if parsed.query:
        _download_presigned_url(url, dest)
    else:
        bucket, key = _parse_s3_url(url)
        download_file(bucket, key, dest, concurrency=concurrency)


def _download_presigned_url(url: str, dest: str) -> None:
    """Download an object using its full pre-signed URL.

    The download is streamed to ``dest`` and each attempt is retried with
    exponential backoff to tolerate transient errors.
    """
    for attempt in range(_NUM_DOWNLOAD_ATTEMPTS):
        try:
            with _SESSION.get(url, stream=True) as resp:
                resp.raise_for_status()
                with open(dest, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
            return
        except Exception:
            if attempt == _NUM_DOWNLOAD_ATTEMPTS - 1:
                raise
            time.sleep(2**attempt)


class _PresignedStream:
    """Iterable wrapper around a streaming ``requests`` response.

    Ensures the underlying HTTP connection is released back to the shared
    session's connection pool even if the consumer aborts iteration early.
    """

    def __init__(
        self,
        url: str,
        *,
        headers: Optional[dict[str, str]] = None,
        chunk_size: int = 8192,
    ):
        self.url = url
        self.headers = headers
        self.chunk_size = chunk_size
        self._resp: Optional[requests.Response] = None
        with _ACTIVE_STREAMS_LOCK:
            _ACTIVE_STREAMS[self] = time.time()
        weakref.finalize(self, self.close)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    def __iter__(self):
        for attempt in range(_NUM_DOWNLOAD_ATTEMPTS):
            self._resp = _SESSION.get(self.url, headers=self.headers, stream=True)
            try:
                if self._resp.status_code not in (200, 206):
                    self._resp.raise_for_status()

                skip = 0
                target_bytes = None
                if self.headers and "Range" in self.headers:
                    range_spec = self.headers["Range"].split("=", 1)[1]
                    start_str, end_str = range_spec.split("-")
                    start = int(start_str)
                    end = int(end_str)
                    target_bytes = end - start + 1
                    if self._resp.status_code == 200:
                        skip = start

                bytes_read = 0
                for chunk in self._resp.iter_content(chunk_size=self.chunk_size):
                    with _ACTIVE_STREAMS_LOCK:
                        _ACTIVE_STREAMS[self] = time.time()
                    if skip:
                        if len(chunk) <= skip:
                            skip -= len(chunk)
                            continue
                        chunk = chunk[skip:]
                        skip = 0

                    if target_bytes is not None:
                        to_yield = min(len(chunk), target_bytes - bytes_read)
                        if to_yield <= 0:
                            break
                        yield chunk[:to_yield]
                        bytes_read += to_yield
                        if bytes_read >= target_bytes:
                            break
                    else:
                        if chunk:
                            yield chunk
                return
            except Exception:
                if attempt == _NUM_DOWNLOAD_ATTEMPTS - 1:
                    raise
                time.sleep(2**attempt)
            finally:
                self.close()

    def close(self) -> None:
        if self._resp is not None:
            self._resp.close()
            self._resp = None
        with _ACTIVE_STREAMS_LOCK:
            _ACTIVE_STREAMS.pop(self, None)


def cleanup_stale_streams(max_age_seconds: int = 300) -> None:
    """Close presigned URL streams that have been inactive longer than ``max_age_seconds``."""
    current_time = time.time()
    with _ACTIVE_STREAMS_LOCK:
        stale = [s for s, ts in _ACTIVE_STREAMS.items() if current_time - ts > max_age_seconds]
    for stream in stale:
        try:
            stream.close()
        finally:
            with _ACTIVE_STREAMS_LOCK:
                _ACTIVE_STREAMS.pop(stream, None)
    gc.collect()


def _stream_presigned_url(
    url: str,
    *,
    headers: Optional[dict[str, str]] = None,
    chunk_size: int = 8192,
):
    """Return an iterable that streams a pre-signed S3 URL."""

    return _PresignedStream(url, headers=headers, chunk_size=chunk_size)


def stream_file_from_url(url: str, chunk_size: int = 8192):
    """Stream an object using its full pre-signed URL."""

    return _stream_presigned_url(url, chunk_size=chunk_size)


def stream_file_range_from_url(
    url: str, start: int, end: int, chunk_size: int = 8192
):
    """Stream a specific byte range from a pre-signed S3 URL."""

    headers = {"Range": f"bytes={start}-{end}"}
    return _stream_presigned_url(url, headers=headers, chunk_size=chunk_size)
