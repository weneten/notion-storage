import os
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from base64 import b64encode, b64decode

def derive_master_key(passphrase: str, salt: bytes=b'notion-e2ee') -> bytes:
    kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=32, salt=salt, iterations=100000)
    return kdf.derive(passphrase.encode())

def test_encrypt_file_key_roundtrip():
    master = derive_master_key('test-pass')
    file_key = os.urandom(32)
    iv = os.urandom(12)
    aes = AESGCM(master)
    enc = aes.encrypt(iv, file_key, None)
    dec = aes.decrypt(iv, enc, None)
    assert dec == file_key
