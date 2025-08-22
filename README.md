# Notion Storage

## Environment Variables

### `MAX_S3_CONCURRENCY`
Controls the maximum number of concurrent threads used for S3 downloads in
`uploader.s3_downloader.download_file`. Defaults to `10` if unset.

