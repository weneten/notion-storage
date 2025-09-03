# Notion Storage

## Environment Variables

### `MAX_S3_CONCURRENCY`
Controls the maximum number of concurrent threads used for S3 downloads in
`uploader.s3_downloader.download_file`. Defaults to `10` if unset.

S3 downloads automatically fall back to unsigned requests when AWS credentials
are unavailable, allowing access to publicly readable or pre-signed URLs
without additional configuration.

## End-to-End Encryption

Uploads are encrypted in the browser using AES‑256‑GCM. A random file key is
generated per upload; each chunk is encrypted with a unique IV derived from the
chunk index. A manifest is produced with the algorithm, IV list, per‑chunk
lengths, MACs and total size. The file key is encrypted with a user master key
derived via PBKDF2 from a passphrase and stored only in the browser.

The manifest and encrypted file key are sent to the server and persisted with
the Notion page. Download endpoints include the metadata in the
`X-Encryption-Metadata` header, allowing clients to decrypt without server
involvement. Share links append the encrypted file key in the URL fragment so
recipients can supply their own passphrase to decrypt.

### Migration

Existing files remain unencrypted and continue to work. Encrypted files appear
with a lock icon in the UI. To migrate older files, download and re‑upload them
with a passphrase set.

