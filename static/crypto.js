// Utility module for client-side encryption/decryption using WebCrypto
// Provides per-file key generation and storage in localStorage

class CryptoManager {
    constructor() {
        this.storagePrefix = 'file-key-';
    }

    async generateFileKey() {
        const key = await crypto.subtle.generateKey(
            { name: 'AES-GCM', length: 256 },
            true,
            ['encrypt', 'decrypt']
        );
        const raw = await crypto.subtle.exportKey('raw', key);
        const fingerprint = await this._fingerprint(raw);
        localStorage.setItem(this.storagePrefix + fingerprint, this._bufToBase64(raw));
        return { key, raw, fingerprint };
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

    async encryptPart(data, fileKey) {
        const nonce = crypto.getRandomValues(new Uint8Array(12));
        const ctWithTag = new Uint8Array(await crypto.subtle.encrypt({ name: 'AES-GCM', iv: nonce }, fileKey, data));
        const tag = ctWithTag.slice(ctWithTag.length - 16);
        const ciphertext = ctWithTag.slice(0, ctWithTag.length - 16);
        return {
            ciphertext,
            nonce: this._bufToBase64(nonce),
            tag: this._bufToBase64(tag)
        };
    }

    async decryptPart(ciphertext, nonceB64, tagB64, fileKey) {
        const nonce = new Uint8Array(this._base64ToBuf(nonceB64));
        const tag = new Uint8Array(this._base64ToBuf(tagB64));
        const combined = new Uint8Array(ciphertext.length + tag.length);
        combined.set(ciphertext);
        combined.set(tag, ciphertext.length);
        const plaintext = await crypto.subtle.decrypt({ name: 'AES-GCM', iv: nonce }, fileKey, combined);
        return new Uint8Array(plaintext);
    }

    async wrapFileKey(fileKeyRaw, linkKeyRaw) {
        const lk = await crypto.subtle.importKey('raw', linkKeyRaw, { name: 'AES-GCM' }, false, ['encrypt', 'decrypt']);
        const nonce = crypto.getRandomValues(new Uint8Array(12));
        const wrappedWithTag = new Uint8Array(await crypto.subtle.encrypt({ name: 'AES-GCM', iv: nonce }, lk, fileKeyRaw));
        const tag = wrappedWithTag.slice(wrappedWithTag.length - 16);
        const wrapped = wrappedWithTag.slice(0, wrappedWithTag.length - 16);
        return {
            wrapped: this._bufToBase64(wrapped),
            nonce: this._bufToBase64(nonce),
            tag: this._bufToBase64(tag)
        };
    }

    async unwrapFileKey(wrappedB64, linkKeyRaw, nonceB64, tagB64) {
        const lk = await crypto.subtle.importKey('raw', linkKeyRaw, { name: 'AES-GCM' }, false, ['encrypt', 'decrypt']);
        const nonce = new Uint8Array(this._base64ToBuf(nonceB64));
        const wrapped = new Uint8Array(this._base64ToBuf(wrappedB64));
        const tag = new Uint8Array(this._base64ToBuf(tagB64));
        const combined = new Uint8Array(wrapped.length + tag.length);
        combined.set(wrapped);
        combined.set(tag, wrapped.length);
        const fileKey = await crypto.subtle.decrypt({ name: 'AES-GCM', iv: nonce }, lk, combined);
        return new Uint8Array(fileKey);
    }

    generateShareLink(id, linkKeyRaw) {
        const lkB64 = this._bufToBase64url(linkKeyRaw);
        return `${window.location.origin}/d/${id}#k=${lkB64}`;
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

    _bufToBase64url(buf) {
        return this._bufToBase64(buf).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
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
