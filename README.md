# Notion Storage

## Environment Variables

### `MAX_S3_CONCURRENCY`
Controls the maximum number of concurrent threads used for S3 downloads in
`uploader.s3_downloader.download_file`. Defaults to `10` if unset.

S3 downloads automatically fall back to unsigned requests when AWS credentials
are unavailable, allowing access to publicly readable or pre-signed URLs
without additional configuration.

## Encryption

All uploads use **AES-GCM** authenticated encryption.  Each file has a
unique 32-byte *file key (FK)* generated client-side.  File content is
split into parts and every part is encrypted with its own randomly
generated 96-bit nonce and produces a 16-byte authentication tag.  The
nonce and tag for each part are stored alongside the ciphertext in the
database.

For optional sharing, a user may generate a 32-byte *link key (LK)*.
The LK wraps the FK using AES-GCM and only the wrapped value (plus
nonce and tag) is persisted on the server.  The share link includes the
LK only in the URL fragment:

```
https://domain.com/d/<id>#k=<base64url(LK)>
```

Because the fragment is never sent to the server, the LK and unwrapped
FK remain private to the client.

new-docker-encrypt