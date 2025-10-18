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
- YT_DLP_MAX_CONCURRENT_JOBS: Cap for simultaneous yt-dlp imports. Default: 2.
- YT_DLP_ALLOWED_FLAGS: Space/comma separated list of flag-style yt-dlp options users may request. Defaults to a curated safe list.
- YT_DLP_ALLOWED_VALUE_OPTIONS: Space/comma separated list of yt-dlp options that accept a value. Defaults to a curated safe list.

If you override the yt-dlp allow-lists, avoid enabling options that execute arbitrary commands or write outside of the temporary workspace.

Notes:
- Multipart uploads are used for files larger than 20MiB. The multipart part size must be set between 5MiB and 20MiB.
- Defaults are applied automatically if a variable is not present in `.env`.

## Notes

- The `/v/<hash>` download endpoint uses the stored `filesize` property to set
  an accurate `Content-Length` header. Uploads must record a correct `filesize`
  or the header may report `0`.

## yt-dlp import jobs

- The API exposes `/api/yt-dlp/jobs` for creating download-and-upload jobs. The server streams downloaded artifacts into Notion sequentially and removes each temporary file once uploaded.
- The backend now depends on the [`yt-dlp`](https://github.com/yt-dlp/yt-dlp) Python package. Install optional tools like `ffmpeg` if you need audio extraction or post-processing features.
- Jobs are sandboxed by default. Only whitelisted yt-dlp flags/arguments are accepted, and the allow-lists can be tightened via environment variables as noted above.
- Administrators should review any custom allow-list changes to avoid exposing flags that execute shell commands, change output paths, or download untrusted subtitles/postprocessors.
