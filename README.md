# Notion Storage

## Features

- Upload single files or entire folders through the web interface. Large files are
  streamed and split into multipart uploads automatically.
- Create, rename, delete, and move files or folders to organize content in a
  hierarchical structure.
- Share files with publicly accessible links that can be protected with a
  password and optional expiration time.

## Environment Variables

You can configure behavior via a `.env` file. See `.env.example` for a full set
of defaults and inline docs. Key settings:

- NOTION_API_TOKEN: Integration token for Notion.
- NOTION_USER_DB_ID: Notion Users DB ID.
- GLOBAL_FILE_INDEX_DB_ID: Optional Global File Index DB ID.
- NOTION_SINGLE_PART_THRESHOLD: Size at or below which a file is uploaded as single-part. Default: 20MiB.
- NOTION_MULTIPART_CHUNK_SIZE: Per-part size for multipart uploads. Default: 5MiB. Allowed: 5–20MiB.
- MAX_PARALLEL_UPLOAD_WORKERS: Max concurrent chunk uploads. Default: 10.
- MAX_S3_CONCURRENCY: Max concurrency for S3 downloads. Default: 10.
- DOWNLOAD_CHUNK_SIZE: Streaming download chunk size. Default: 1MiB.

Notes:
- Multipart uploads are used for files larger than 20MiB. The multipart part size must be set between 5MiB and 20MiB.
- Defaults are applied automatically if a variable is not present in `.env`.
