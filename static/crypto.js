// Utility module for client-side encryption/decryption using WebCrypto
// Provides per-file key generation and storage in localStorage

class CryptoManager {
    constructor() {
        this.storagePrefix = 'file-key-';
    }

    async generateKey() {
        const key = await crypto.subtle.generateKey(
            { name: 'AES-GCM', length: 256 },
            true,
            ['encrypt', 'decrypt']
        );
        const raw = await crypto.subtle.exportKey('raw', key);
        const fingerprint = await this._fingerprint(raw);
        localStorage.setItem(this.storagePrefix + fingerprint, this._bufToBase64(raw));
        return { key, fingerprint };
    }

    async getKey(fingerprint) {
        const base64 = localStorage.getItem(this.storagePrefix + fingerprint);
        if (!base64) return null;
        const raw = this._base64ToBuf(base64);
        return crypto.subtle.importKey('raw', raw, { name: 'AES-GCM' }, false, ['encrypt', 'decrypt']);
    }

    async encryptFile(file) {
        const { key, fingerprint } = await this.generateKey();
        const iv = crypto.getRandomValues(new Uint8Array(12));
        const data = await file.arrayBuffer();
        const ciphertext = await crypto.subtle.encrypt({ name: 'AES-GCM', iv }, key, data);
        return {
            ciphertext: new Uint8Array(ciphertext),
            iv: this._bufToBase64(iv),
            keyFingerprint: fingerprint
        };
    }

    async decrypt(ciphertext, ivBase64, fingerprint) {
        const key = await this.getKey(fingerprint);
        if (!key) throw new Error('Missing decryption key');
        const iv = new Uint8Array(this._base64ToBuf(ivBase64));
        const plaintext = await crypto.subtle.decrypt({ name: 'AES-GCM', iv }, key, ciphertext);
        return new Uint8Array(plaintext);
    }

    async _fingerprint(rawKey) {
        const hash = await crypto.subtle.digest('SHA-256', rawKey);
        return Array.from(new Uint8Array(hash)).map(b => b.toString(16).padStart(2, '0')).join('');
    }

    _bufToBase64(buf) {
        const bytes = new Uint8Array(buf);
        let binary = '';
        for (let i = 0; i < bytes.length; i++) {
            binary += String.fromCharCode(bytes[i]);
        }
        return btoa(binary);
    }

    _base64ToBuf(b64) {
        const binary = atob(b64);
        const bytes = new Uint8Array(binary.length);
        for (let i = 0; i < binary.length; i++) {
            bytes[i] = binary.charCodeAt(i);
        }
        return bytes.buffer;
    }
}

window.CryptoManager = new CryptoManager();
