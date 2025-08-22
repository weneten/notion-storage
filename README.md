# Notion Storage

## Environment Variables

### `MAX_S3_CONCURRENCY`
Controls the maximum number of concurrent threads used for S3 downloads in
`uploader.s3_downloader.download_file`. The helper automatically scales the
number of worker threads to roughly one per 100 MiB of object size, capped by
this environment variable. Defaults to `10` if unset.

S3 downloads automatically fall back to unsigned requests when AWS credentials
are unavailable, allowing access to publicly readable or pre-signed URLs
without additional configuration.

