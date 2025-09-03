import io
import os
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from uploader.crypto_utils import generate_key, encrypt_stream, decrypt_stream


def test_round_trip_stream_encryption():
    key = generate_key()
    plaintext = os.urandom(64)
    plain_stream = io.BytesIO(plaintext)

    encrypted = encrypt_stream(key, plain_stream)
    assert encrypted != plaintext

    encrypted_stream = io.BytesIO(encrypted)
    decrypted = decrypt_stream(key, encrypted_stream)

    assert decrypted == plaintext
