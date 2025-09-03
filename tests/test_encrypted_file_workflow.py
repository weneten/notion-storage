import os
from uploader.crypto_utils import generate_key, encrypt_stream, decrypt_stream


def test_encrypted_file_upload_download_stream(tmp_path):
    key = generate_key()
    iv = os.urandom(16)
    plaintext = b"The quick brown fox jumps over the lazy dog" * 1024

    # Simulate upload: encrypt and write to disk in chunks
    encrypted_path = tmp_path / "cipher.bin"
    with open(encrypted_path, "wb") as f:
        chunks = [plaintext[i:i + 512] for i in range(0, len(plaintext), 512)]
        for enc in encrypt_stream(key, iv, chunks):
            f.write(enc)

    # Simulate download and streaming decryption
    def encrypted_chunks():
        with open(encrypted_path, "rb") as f:
            while True:
                data = f.read(256)
                if not data:
                    break
                yield data

    decrypted = b"".join(decrypt_stream(key, iv, encrypted_chunks()))
    assert decrypted == plaintext
