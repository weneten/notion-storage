import os
import sys
from pathlib import Path
import importlib.util

# Load crypto_utils directly to avoid importing package with heavy dependencies
utils_path = Path(__file__).resolve().parents[1] / 'uploader' / 'crypto_utils.py'
spec = importlib.util.spec_from_file_location('crypto_utils', utils_path)
crypto_utils = importlib.util.module_from_spec(spec)
spec.loader.exec_module(crypto_utils)

generate_file_key = crypto_utils.generate_file_key
encrypt_stream = crypto_utils.encrypt_stream
decrypt_stream = crypto_utils.decrypt_stream
wrap_file_key = crypto_utils.wrap_file_key
unwrap_file_key = crypto_utils.unwrap_file_key


def test_round_trip_stream_encryption():
    key = generate_file_key()
    nonce = os.urandom(12)
    plaintext_chunks = [os.urandom(64), os.urandom(128)]

    encrypted_chunks, tag = encrypt_stream(key, nonce, iter(plaintext_chunks))
    encrypted_chunks = list(encrypted_chunks)
    # Ensure ciphertext differs from plaintext
    assert b"".join(encrypted_chunks) != b"".join(plaintext_chunks)

    decrypted_chunks = list(decrypt_stream(key, nonce, tag, iter(encrypted_chunks)))

    assert b"".join(decrypted_chunks) == b"".join(plaintext_chunks)


def test_file_key_wrap_unwrap():
    fk = generate_file_key()
    lk = generate_file_key()
    wrapped, nonce, tag = wrap_file_key(fk, lk)
    recovered = unwrap_file_key(wrapped, lk, nonce, tag)
    assert recovered == fk
