# Notion Storage

## End-to-End Encryption

Notion Storage supports optional end-to-end encryption (E2EE) for file uploads. Files are encrypted on the client before transmission and remain encrypted at rest. The server provides streaming helpers in `uploader.crypto_utils` using AES-CTR for scenarios where on-the-fly encryption or decryption is required.

## Key Management

1. Generate a 256-bit key using `uploader.crypto_utils.generate_key()` or your platform's secure random source.
2. Create a unique 16-byte initialization vector (IV) for each file.
3. Compute a fingerprint of the key (for example, `sha256`) and send only the fingerprint, algorithm name, and IV as metadata with the upload.
4. Store the encryption key securely on the client. The server never stores or sees the raw key.

## Security Considerations

- Loss of the encryption key means loss of access to the encrypted file.
- Rotate keys periodically and discard old encrypted files when keys are rotated.
- Ensure IVs are never reused with the same key.
- Verify the integrity of client-side code delivering encryption routines.

## Environment Variables

### `MAX_S3_CONCURRENCY`
Controls the maximum number of concurrent threads used for S3 downloads in `uploader.s3_downloader.download_file`. Defaults to `10` if unset.

S3 downloads automatically fall back to unsigned requests when AWS credentials are unavailable, allowing access to publicly readable or pre-signed URLs without additional configuration.
