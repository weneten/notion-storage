"""Utility for downloading objects from S3 with concurrency and retries.

This module provides a ``download_file`` helper that wraps ``boto3``'s
:class:`~boto3.s3.transfer.S3Transfer` to perform multipart downloads with
configurable concurrency. Each part download is retried with exponential
backoff to help recover from transient throttling errors.

The maximum concurrency can be configured via the ``MAX_S3_CONCURRENCY``
environment variable (default: ``10``).
"""

import os
import time
from typing import Optional
from urllib.parse import urlparse

import boto3
import requests
from boto3.s3.transfer import S3Transfer, TransferConfig
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import NoCredentialsError

# Number of attempts for each part download (includes exponential backoff)
_NUM_DOWNLOAD_ATTEMPTS = 5


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
        concurrency = int(os.getenv("MAX_S3_CONCURRENCY", "10"))

    transfer_config = TransferConfig(
        multipart_threshold=8 * 1024 * 1024,
        max_concurrency=concurrency,
        num_download_attempts=_NUM_DOWNLOAD_ATTEMPTS,
    )

    client = boto3.client("s3")
    transfer = S3Transfer(client=client, config=transfer_config)
    try:
        transfer.download_file(bucket, key, dest)
    except NoCredentialsError:
        # Fallback to unsigned requests for publicly accessible objects
        anon_client = boto3.client("s3", config=Config(signature_version=UNSIGNED))
        anon_transfer = S3Transfer(client=anon_client, config=transfer_config)
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
            with requests.get(url, stream=True) as resp:
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
