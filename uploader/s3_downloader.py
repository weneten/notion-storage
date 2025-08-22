"""Utility for downloading objects from S3 with concurrency and retries.

This module provides a ``download_file`` helper that wraps ``boto3``'s
:class:`~boto3.s3.transfer.S3Transfer` to perform multipart downloads with
configurable concurrency. Each part download is retried with exponential
backoff to help recover from transient throttling errors.

The maximum concurrency can be configured via the ``MAX_S3_CONCURRENCY``
environment variable (default: ``10``).
"""

import os
from typing import Optional
from urllib.parse import urlparse

import boto3
from boto3.s3.transfer import S3Transfer, TransferConfig

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
    transfer.download_file(bucket, key, dest)


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
    """Download an S3 object specified by URL to ``dest``."""
    bucket, key = _parse_s3_url(url)
    download_file(bucket, key, dest, concurrency=concurrency)
