import os
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from uploader.crypto_utils import generate_key, encrypt_stream, decrypt_stream


def test_round_trip_stream_encryption():
    key = generate_key()
    iv = os.urandom(16)
    plaintext_chunks = [os.urandom(64), os.urandom(128)]

    encrypted_chunks = list(encrypt_stream(key, iv, iter(plaintext_chunks)))
    # Ensure ciphertext differs from plaintext
    assert b"".join(encrypted_chunks) != b"".join(plaintext_chunks)

    decrypted_chunks = list(decrypt_stream(key, iv, iter(encrypted_chunks)))

    assert b"".join(decrypted_chunks) == b"".join(plaintext_chunks)
