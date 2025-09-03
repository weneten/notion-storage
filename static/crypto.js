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

    hasKey(fingerprint) {
        return !!localStorage.getItem(this.storagePrefix + fingerprint);
    }

    listKeys() {
        const keys = [];
        for (let i = 0; i < localStorage.length; i++) {
            const k = localStorage.key(i);
            if (k && k.startsWith(this.storagePrefix)) {
                const fingerprint = k.substring(this.storagePrefix.length);
                keys.push({ fingerprint, key: localStorage.getItem(k) });
            }
        }
        return keys;
    }

    exportKeys() {
        return JSON.stringify(this.listKeys());
    }

    importKeys(json) {
        let data;
        if (typeof json === 'string') {
            data = JSON.parse(json);
        } else {
            data = json;
        }
        if (!Array.isArray(data)) throw new Error('Invalid key file');
        data.forEach(k => {
            if (k.fingerprint && k.key) {
                localStorage.setItem(this.storagePrefix + k.fingerprint, k.key);
            }
        });
    }

    async encryptFile(file, progressCallback = null) {
        const { key, fingerprint } = await this.generateKey();
        const iv = crypto.getRandomValues(new Uint8Array(12));

        // Read file in chunks to provide progress updates
        const reader = file.stream().getReader();
        const chunks = [];
        let received = 0;
        if (progressCallback) progressCallback(0, 0);
        while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            chunks.push(value);
            received += value.length;
            if (progressCallback) {
                const pct = (received / file.size) * 100;
                progressCallback(pct, received);
            }
        }
        const data = new Uint8Array(received);
        let offset = 0;
        for (const chunk of chunks) {
            data.set(chunk, offset);
            offset += chunk.length;
        }

        const ciphertext = await crypto.subtle.encrypt({ name: 'AES-GCM', iv }, key, data);
        if (progressCallback) progressCallback(100, file.size);
        return {
            ciphertext: new Uint8Array(ciphertext),
            iv: this._bufToBase64(iv),
            keyFingerprint: fingerprint
        };
    }

    async decrypt(ciphertext, ivBase64, fingerprint, progressCallback = null) {
        const key = await this.getKey(fingerprint);
        if (!key) throw new Error('Missing decryption key');
        const iv = new Uint8Array(this._base64ToBuf(ivBase64));
        if (progressCallback) progressCallback(0);
        const plaintext = await crypto.subtle.decrypt({ name: 'AES-GCM', iv }, key, ciphertext);
        if (progressCallback) progressCallback(100);
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
