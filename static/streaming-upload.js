/**
 * Streaming File Upload Client
 * Implements continuous streaming upload without client-side chunking
 * Addresses performance issues by eliminating artificial delays and reducing overhead
 */

// Track uploads awaiting database integration before refreshing UI
window.pendingUploads = window.pendingUploads || new Set();

// Essential utility functions for UI updates
function showStatus(message, type) {
    const messageContainer = document.getElementById('messageContainer');
    if (!messageContainer) return;

    messageContainer.innerHTML = '';

    // Determine the correct alert class
    let alertClass = 'alert-info'; // Default to info
    if (type === 'error') {
        alertClass = 'alert-danger';
    } else if (type === 'success') {
        alertClass = 'alert-success';
    } else if (type === 'info') {
        alertClass = 'alert-info';
    }

    const alertDiv = document.createElement('div');
    alertDiv.className = `alert ${alertClass}`;

    // Add appropriate icon based on message type
    let icon = '';
    if (type === 'error') {
        icon = '<i class="fas fa-exclamation-circle mr-2"></i>';
    } else if (type === 'success') {
        icon = '<i class="fas fa-check-circle mr-2"></i>';
    } else {
        icon = '<i class="fas fa-info-circle mr-2"></i>';
    }

    alertDiv.innerHTML = icon + message;
    messageContainer.appendChild(alertDiv);

    // Apply inline styles to override any Bootstrap styles
    if (type === 'info' || type === 'initializing' || type === 'finalizing') {
        alertDiv.style.backgroundColor = '#1c332d';
        alertDiv.style.borderColor = '#03dac6';
        alertDiv.style.color = '#03dac6';
    }

    // Auto-remove after 5 seconds for success messages
    if (type === 'success') {
        setTimeout(() => {
            if (alertDiv.parentNode === messageContainer) {
                messageContainer.removeChild(alertDiv);
            }
        }, 5000);
    }
}

// Display a retry button when an upload fails
function showRetryButton() {
    const container = document.getElementById('messageContainer');
    if (!container) return;

    const retryBtn = document.createElement('button');
    retryBtn.textContent = 'Retry Upload';
    retryBtn.className = 'btn btn-warning btn-sm ml-2';
    retryBtn.addEventListener('click', () => {
        retryBtn.disabled = true;
        if (streamingUploader && streamingUploader.failedUpload) {
            resumeFailedUpload();
        } else {
            uploadFile();
        }
    });

    container.appendChild(retryBtn);
}

// Create a progress bar element for a specific file
function createProgressBarElements(filename, totalBytes) {
    const container = document.createElement('div');
    container.className = 'progress-item';

    const bar = document.createElement('div');
    bar.className = 'progress-bar';

    const text = document.createElement('span');
    text.className = 'progress-text';
    text.textContent = '0%';
    bar.appendChild(text);

    container.appendChild(bar);

    const subText = document.createElement('p');
    subText.className = 'progress-subtext';
    subText.textContent = `${filename}: 0/${streamingUploader ? streamingUploader.formatFileSize(totalBytes) : totalBytes}`;
    container.appendChild(subText);

    const progressContainer = document.getElementById('progressBars');
    if (progressContainer) {
        progressContainer.appendChild(container);
    }

    return { container, bar, text, subText, filename, totalBytes };
}

// Update a specific progress bar
function updateIndividualProgressBar(elements, percentage, uploadedBytes) {
    if (!elements) return;
    elements.bar.style.width = percentage + '%';
    elements.text.textContent = percentage + '%';
    if (uploadedBytes != null) {
        const totalFormatted = streamingUploader ? streamingUploader.formatFileSize(elements.totalBytes) : elements.totalBytes;
        const uploadedFormatted = streamingUploader ? streamingUploader.formatFileSize(uploadedBytes) : uploadedBytes;
        elements.subText.textContent = `${elements.filename}: ${uploadedFormatted}/${totalFormatted}`;
    }
}

// Legacy single progress bar updater (retained for backwards compatibility)
function updateProgressBar(percentage, statusText) {
    const progressBar = document.getElementById('progressBar');
    const progressText = document.getElementById('progressText');
    const progressSubText = document.getElementById('progressSubText');
    const progressBarContainer = document.getElementById('progressBarContainer');

    if (!progressBar || !progressText || !progressSubText || !progressBarContainer) return;

    // Show progress bar
    progressBarContainer.style.display = 'block';

    // Update progress
    progressBar.style.width = percentage + '%';
    progressText.textContent = percentage + '%';

    // Update status text
    if (statusText) {
        progressSubText.textContent = statusText;
    }
}

// Bulk selection helpers
const deleteSelectedBtn = document.getElementById('deleteSelectedButton');
const moveSelectedBtn = document.getElementById('moveSelectedButton');
const selectAllBtn = document.getElementById('selectAllButton');

function updateBulkActionButtons() {
    const checkboxes = document.querySelectorAll('.select-item');
    const checkedBoxes = document.querySelectorAll('.select-item:checked');
    const anyChecked = checkedBoxes.length > 0;
    if (deleteSelectedBtn) deleteSelectedBtn.style.display = anyChecked ? 'inline-block' : 'none';
    if (moveSelectedBtn) moveSelectedBtn.style.display = anyChecked ? 'inline-block' : 'none';
    if (selectAllBtn) {
        const allChecked = checkboxes.length > 0 && checkedBoxes.length === checkboxes.length;
        selectAllBtn.textContent = allChecked ? 'Deselect All' : 'Select All';
    }
}

function formatBytes(bytes) {
    if (bytes === 0) return '0 Bytes';
    const k = 1000;
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

function refreshServerCache() {
    fetch('/api/cache/refresh', { method: 'POST' }).catch(() => {});
}

async function fetchRemainingEntries() {
    const spinner = document.getElementById('loadingSpinner');
    while (window.nextCursor !== null && window.nextCursor !== undefined) {
        if (spinner) spinner.style.display = 'block';
        const params = new URLSearchParams({
            cursor: window.nextCursor,
            folder: window.currentFolder || '/',
            since: window.cacheTimestamp || 0
        });
        const resp = await fetch(`/api/files/sync?${params.toString()}`);
        if (!resp.ok) break;
        const data = await resp.json();
        window.cacheTimestamp = data.last_sync;
        if (Array.isArray(data.results) && data.results.length) {
            appendEntries(data.results);
        }
        if (data.pending) {
            await new Promise(r => setTimeout(r, 1000));
        }
        window.nextCursor = data.next_cursor;
        if ((window.nextCursor === null || window.nextCursor === undefined) && spinner) {
            spinner.style.display = 'none';
        }
    }
}

function appendEntries(entries) {
    if (!window.cachedEntries) window.cachedEntries = [];
    window.cachedEntries = window.cachedEntries.concat(entries);
    const tbody = document.querySelector('#files-container tbody');
    if (!tbody) return;
    entries.forEach(entry => {
        let row = document.createElement('tr');
        if (entry.type === 'folder') {
            row.className = 'folder-row';
            row.dataset.folderPath = entry.full_path;
            row.innerHTML = `
                <td><input type="checkbox" class="select-item" data-type="folder" data-id="${entry.id}"></td>
                <td><i class="fas fa-folder mr-1"></i><strong>${entry.name}</strong></td>
                <td class="filesize-cell">${formatBytes(entry.size)}</td>
                <td>${entry.full_path}</td>
                <td></td>
                <td></td>
                <td>
                    <a href="/?folder=${encodeURIComponent(entry.full_path)}" class="btn btn-primary btn-sm"><i class="fas fa-folder-open mr-1"></i>Open</a>
                    <a href="/download_folder?folder=${encodeURIComponent(entry.full_path)}" class="btn btn-primary btn-sm"><i class="fas fa-download mr-1"></i>Download</a>
                    <button class="btn btn-secondary btn-sm rename-folder-btn" data-folder-id="${entry.id}" data-folder-name="${entry.name}"><i class="fas fa-edit mr-1"></i>Rename</button>
                    <button class="btn btn-danger btn-sm delete-folder-btn" data-folder-id="${entry.id}" data-folder-path="${entry.full_path}"><i class="fas fa-trash-alt mr-1"></i>Delete</button>
                </td>`;
        } else {
            row.dataset.fileId = entry.id;
            row.dataset.fileHash = entry.file_hash || '';
            row.dataset.filename = entry.name;
            row.dataset.encryptionAlg = entry.encryption_alg || 'none';
            row.dataset.iv = entry.iv || 'none';
            row.dataset.keyFingerprint = entry.key_fingerprint || 'none';
            const link = entry.file_hash ? `<a href="/d/${entry.file_hash}" target="_blank" class="public-link"><i class="fas fa-external-link-alt mr-1"></i>${location.origin}/d/${entry.file_hash.slice(0,10)}...</a>` : '<span class="text-muted">N/A</span>';
            const viewContainer = entry.file_hash ? `<span class="view-button-container" data-filename="${entry.name}" data-hash="${entry.file_hash}" data-filesize="${formatBytes(entry.size)}"></span>` : '';
            const encIcon = (entry.encryption_alg && entry.encryption_alg !== 'none') ? ` <i class="fas fa-lock text-info encrypted-indicator" title="Encrypted"></i><span class="missing-key-warning text-danger" style="display:none;" title="Missing decryption key"><i class="fas fa-exclamation-triangle"></i></span>` : '';
            row.innerHTML = `
                <td><input type="checkbox" class="select-item" data-type="file" data-id="${entry.id}"></td>
                <td><span class="file-type-icon" data-filename="${entry.name}"></span><strong>${entry.name}</strong>${encIcon}</td>
                <td class="filesize-cell">${formatBytes(entry.size)}</td>
                <td>${entry.folder}</td>
                <td>${link}</td>
                <td><label class="switch"><input type="checkbox" class="public-toggle" data-file-id="${entry.id}" data-file-hash="${entry.file_hash || ''}" ${entry.is_public ? 'checked' : ''}><span class="slider round"></span></label></td>
                <td class="action-buttons">
                    ${viewContainer}
                    <a href="/d/${entry.file_hash}" class="btn btn-primary btn-sm download-btn" data-filename="${entry.name}"><i class="fas fa-download mr-1"></i>Download</a>
                    <div class="btn-group">
                        <button type="button" class="btn btn-secondary btn-sm dropdown-toggle" data-toggle="dropdown" aria-haspopup="true" aria-expanded="false">
                            <i class="fas fa-bars"></i>
                        </button>
                        <div class="dropdown-menu dropdown-menu-right">
                            <button type="button" class="dropdown-item rename-btn" data-file-id="${entry.id}"><i class="fas fa-edit mr-1"></i>Rename</button>
                            <button type="button" class="dropdown-item move-btn" data-file-id="${entry.id}"><i class="fas fa-folder-open mr-1"></i>Move</button>
                            <button type="button" class="dropdown-item delete-btn" data-file-id="${entry.id}" data-file-hash="${entry.file_hash || ''}"><i class="fas fa-trash-alt mr-1"></i>Delete</button>
                        </div>
                    </div>
                </td>`;
        }
        tbody.appendChild(row);

        const checkbox = row.querySelector('.select-item');
        if (checkbox) {
            checkbox.addEventListener('change', updateBulkActionButtons);
        }

        setupFileActionEventHandlers(row);
        setupFolderActionEventHandlers(row);
        markEncryptedRows(row);
    });

    if (typeof initializeFileTypeIcons === 'function') {
        initializeFileTypeIcons();
    }
    updateBulkActionButtons();
}

function markEncryptedRows(root = document) {
    let missing = false;
    const rows = (root.matches && root.matches('tr[data-encryption-alg]')) ? [root] : root.querySelectorAll('tr[data-encryption-alg]');
    rows.forEach(row => {
        const alg = row.dataset.encryptionAlg || 'none';
        if (alg !== 'none') {
            const fp = row.dataset.keyFingerprint;
            const has = window.CryptoManager && window.CryptoManager.hasKey(fp);
            const warn = row.querySelector('.missing-key-warning');
            if (warn) {
                warn.style.display = has ? 'none' : 'inline';
            }
            if (!has) missing = true;
        }
    });
    if (missing && typeof showStatus === 'function') {
        if (!markEncryptedRows.warned) {
            showStatus('Some encrypted files are missing keys. Import the correct keys to decrypt.', 'error');
            markEncryptedRows.warned = true;
        }
    }
}

window.markEncryptedRows = markEncryptedRows;

// Periodically check server for updates
async function checkForUpdates() {
    try {
        const params = new URLSearchParams({
            folder: window.currentFolder || '/',
            since: window.cacheTimestamp || 0
        });
        const resp = await fetch(`/api/files/sync?${params.toString()}`);
        if (!resp.ok) return;
        const data = await resp.json();
        if (Array.isArray(data.results) && data.results.length > 0) {
            await loadFiles();
        }
        if (data.last_sync !== undefined) {
            window.cacheTimestamp = data.last_sync;
        }
    } catch (err) {
        console.error('🚨 DIAGNOSTIC: Error checking updates:', err);
    }
}

class StreamingFileUploader {
    constructor() {
        this.activeUploads = new Map();
        this.defaultChunkSize = 64 * 1024; // 64KB read chunks for streaming (much smaller than before)
        this.failedUpload = null;
    }

    /**
     * Upload a file using continuous streaming
     * @param {File} file - The file to upload
     * @param {Function} progressCallback - Progress callback function
     * @param {Function} statusCallback - Status update callback function
     * @param {string} folderPath - Target folder path for this file
     * @returns {Promise} Upload result
     */    async uploadFile(file, progressCallback, statusCallback, folderPath = window.currentFolder || '/') {
        const uploadId = this.generateUploadId();
        this.failedUpload = null;

        try {
            statusCallback('Initializing streaming upload...', 'info');
            console.log('Starting streaming upload for file:', file.name, 'Size:', file.size);

            // Encrypt file locally before uploading
            statusCallback('Encrypting file...', 'info');
            const { ciphertext, iv, keyFingerprint } = await window.CryptoManager.encryptFile(file, (pct, bytes) => {
                progressCallback(pct / 2, bytes / 2);
            });
            const encryptedBlob = new Blob([ciphertext], { type: 'application/octet-stream' });
            statusCallback('Uploading encrypted file...', 'info');

            // Step 1: Create upload session on server including encryption metadata
            console.log('Creating upload session...');
            const sessionResponse = await fetch('/api/upload/create-session', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    filename: file.name,
                    fileSize: encryptedBlob.size,
                    contentType: file.type || 'application/octet-stream',
                    folderPath: folderPath,
                    encryption: { alg: 'AES-GCM', iv: iv, key_fingerprint: keyFingerprint }
                })
            });

            console.log('Session response status:', sessionResponse.status);
            if (!sessionResponse.ok) {
                const errorText = await sessionResponse.text();
                console.error('Session creation failed:', errorText);
                throw new Error(`Failed to create upload session: ${sessionResponse.statusText}`);
            }

            const sessionData = await sessionResponse.json();
            console.log('Session created:', sessionData);

            const actualUploadId = sessionData.upload_id;

            // Step 2: Start streaming upload
            statusCallback(`Streaming ${file.name} (${this.formatFileSize(encryptedBlob.size)})...`, 'info');
            console.log('Starting file stream for upload ID:', actualUploadId);

            const result = await this.streamFileToServer(
                actualUploadId,
                encryptedBlob,
                (pct, uploaded) => progressCallback(50 + pct / 2, (file.size / 2) + (uploaded / 2)),
                statusCallback,
                { alg: 'AES-GCM', iv: iv, keyFingerprint: keyFingerprint }
            );

            console.log('Upload completed:', result);
            statusCallback(`Upload completed successfully!`, 'success');
            return result;

        } catch (error) {
            console.error('Upload error:', error);
            statusCallback(`Upload failed: ${error.message}`, 'error');

            const info = this.activeUploads.get(uploadId);
            if (info) {
                this.failedUpload = { file, uploadId, bytesUploaded: info.bytesUploaded };
            }

            // Cleanup failed upload
            if (uploadId) {
                this.abortUpload(uploadId);
            }

            throw error;
        }
    }/**
     * Stream file data directly to server using simple fetch approach
     * This eliminates complex ReadableStream and uses direct file upload
     */
    async streamFileToServer(uploadId, file, progressCallback, statusCallback, encInfo = null) {
        const controller = new AbortController();

        // Store upload info for potential cancellation
        this.activeUploads.set(uploadId, {
            file: file,
            controller: controller,
            startTime: Date.now(),
            bytesUploaded: 0
        });

        try {
            // Use XMLHttpRequest for better progress tracking and streaming
            return new Promise((resolve, reject) => {
                const xhr = new XMLHttpRequest();

                // Set up progress tracking
                xhr.upload.onprogress = (event) => {
                    if (event.lengthComputable) {
                        const progress = (event.loaded / event.total) * 100;
                        const info = this.activeUploads.get(uploadId);
                        if (info) {
                            info.bytesUploaded = event.loaded;
                        }
                        progressCallback(progress, event.loaded);
                    }
                };

                // Handle completion
                xhr.onload = () => {
                    if (xhr.status >= 200 && xhr.status < 300) {
                        try {
                            const result = JSON.parse(xhr.responseText);
                            resolve(result);
                        } catch (e) {
                            reject(new Error('Invalid response format'));
                        }
                    } else {
                        reject(new Error(`Upload failed: ${xhr.status} ${xhr.statusText}`));
                    }
                };

                // Handle errors
                xhr.onerror = () => {
                    reject(new Error('Upload failed: Network error'));
                };

                // Handle abort
                xhr.onabort = () => {
                    reject(new Error('Upload was cancelled'));
                };

                // Open connection
                xhr.open('POST', `/api/upload/stream/${uploadId}`, true);

                // Set headers
                xhr.setRequestHeader('Content-Type', 'application/octet-stream');
                xhr.setRequestHeader('X-File-Name', encodeURIComponent(file.name || 'encrypted'));
                xhr.setRequestHeader('X-File-Size', file.size.toString());
                if (encInfo) {
                    xhr.setRequestHeader('X-Encryption-Alg', encInfo.alg);
                    xhr.setRequestHeader('X-Encryption-IV', encInfo.iv);
                    xhr.setRequestHeader('X-Key-Fingerprint', encInfo.keyFingerprint);
                }

                // Send the file directly
                xhr.send(file);
            });

        } finally {
            // Cleanup
            this.activeUploads.delete(uploadId);
        }
    }/**
     * Create a ReadableStream from a File object
     * This streams the file in small chunks without loading everything into memory
     */
    createFileStream(file, progressCallback) {
        let bytesRead = 0;
        let offset = 0;
        const chunkSize = 64 * 1024; // 64KB chunks for continuous reading

        return new ReadableStream({
            async pull(controller) {
                try {
                    if (offset >= file.size) {
                        controller.close();
                        return;
                    }

                    // Read the next chunk
                    const end = Math.min(offset + chunkSize, file.size);
                    const chunk = file.slice(offset, end);

                    // Convert blob to array buffer
                    const arrayBuffer = await chunk.arrayBuffer();
                    const uint8Array = new Uint8Array(arrayBuffer);

                    // Update progress
                    bytesRead += uint8Array.length;
                    const progress = (bytesRead / file.size) * 100;
                    progressCallback(progress, bytesRead);

                    // Move to next chunk
                    offset = end;

                    // Enqueue the chunk
                    controller.enqueue(uint8Array);

                } catch (error) {
                    console.error('Error reading file chunk:', error);
                    controller.error(error);
                }
            },

            cancel(reason) {
                console.log('Stream cancelled:', reason);
            }
        });
    }

    /**
     * Resume a failed upload
     */
    async resumeUpload(uploadId, progressCallback, statusCallback) {
        const info = this.failedUpload;
        if (!info || info.uploadId !== uploadId) {
            throw new Error('No failed upload to resume');
        }
        const { file, bytesUploaded } = info;
        const controller = new AbortController();
        this.activeUploads.set(uploadId, {
            file,
            controller,
            startTime: Date.now(),
            bytesUploaded
        });
        return new Promise((resolve, reject) => {
            const xhr = new XMLHttpRequest();
            xhr.upload.onprogress = (event) => {
                if (event.lengthComputable) {
                    const total = bytesUploaded + event.loaded;
                    const progress = (total / file.size) * 100;
                    const info = this.activeUploads.get(uploadId);
                    if (info) info.bytesUploaded = total;
                    progressCallback(progress, total);
                }
            };
            xhr.onload = () => {
                if (xhr.status >= 200 && xhr.status < 300) {
                    try { resolve(JSON.parse(xhr.responseText)); } catch (e) { reject(new Error('Invalid response format')); }
                } else {
                    reject(new Error(`Resume failed: ${xhr.status} ${xhr.statusText}`));
                }
            };
            xhr.onerror = () => reject(new Error('Resume failed: Network error'));
            xhr.onabort = () => reject(new Error('Resume was cancelled'));
            xhr.open('POST', `/api/upload/resume/${uploadId}`, true);
            xhr.setRequestHeader('Content-Type', 'application/octet-stream');
            xhr.setRequestHeader('X-File-Name', encodeURIComponent(file.name));
            xhr.setRequestHeader('X-File-Size', file.size.toString());
            xhr.setRequestHeader('X-Start-Byte', bytesUploaded.toString());
            const slice = file.slice(bytesUploaded);
            xhr.send(slice);
        }).finally(() => {
            this.activeUploads.delete(uploadId);
            delete this.failedUpload;
        });
    }

    /**
     * Abort an active upload
     */
    abortUpload(uploadId) {
        const uploadInfo = this.activeUploads.get(uploadId);
        if (uploadInfo) {
            uploadInfo.controller.abort();
            this.activeUploads.delete(uploadId);

            // Notify server about cancellation
            fetch(`/api/upload/abort/${uploadId}`, {
                method: 'POST'
            }).catch(err => console.warn('Failed to notify server about upload cancellation:', err));
        }
    }

    /**
     * Generate a unique upload ID
     */
    generateUploadId() {
        return 'upload-' + Date.now() + '-' + Math.random().toString(36).substr(2, 9);
    }

    /**
     * Format file size for display
     */
    formatFileSize(bytes, decimals = 2) {
        if (bytes === 0) return '0 Bytes';
        const k = 1000;
        const dm = decimals < 0 ? 0 : decimals;
        const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
    }

    /**
     * Get upload statistics
     */
    getUploadStats(uploadId) {
        const uploadInfo = this.activeUploads.get(uploadId);
        if (!uploadInfo) return null;

        const elapsed = Date.now() - uploadInfo.startTime;
        return {
            uploadId,
            fileName: uploadInfo.file.name,
            fileSize: uploadInfo.file.size,
            elapsedTime: elapsed,
            status: 'active'
        };
    }

    /**
     * Get all active uploads
     */
    getActiveUploads() {
        return Array.from(this.activeUploads.keys()).map(uploadId =>
            this.getUploadStats(uploadId)
        );
    }
}

// Legacy compatibility functions for existing UI
let streamingUploader = null;

/**
 * Initialize the streaming uploader (replaces the old uploadFile function)
 */
function initializeStreamingUploader() {
    console.log('Initializing streaming uploader...');
    streamingUploader = new StreamingFileUploader();
    console.log('Streaming uploader initialized:', streamingUploader);
}

/**
 * Enhanced upload function that uses streaming instead of chunking
 * This replaces the old WebSocket-based chunked upload
 */
const uploadFile = async () => {
    if (!streamingUploader) {
        initializeStreamingUploader();
    }

    const fileInput = document.getElementById('fileInput');
    const folderInput = document.getElementById('folderInput');
    const files = [
        ...(fileInput ? Array.from(fileInput.files) : []),
        ...(folderInput ? Array.from(folderInput.files) : [])
    ];

    if (files.length === 0) {
        showStatus('Please select files or folders to upload.', 'error');
        return;
    }

    const progressContainer = document.getElementById('progressBars');
    if (progressContainer) {
        progressContainer.innerHTML = '';
    }

    // Create progress bars for all files immediately
    const fileEntries = files.map(file => ({
        file,
        elements: createProgressBarElements(file.name, file.size)
    }));

    const maxConcurrent = 3;
    const executing = [];
    const allUploads = [];

    for (const { file, elements } of fileEntries) {
        const uploadTask = async () => {
            const progressCallback = (progress, bytesUploaded) => {
                updateIndividualProgressBar(elements, Math.floor(progress), bytesUploaded);
            };

            const statusCallback = (message, type) => {
                showStatus(message, type);
            };

            // Determine target folder path for this file
            let folderPath = window.currentFolder || '/';
            if (file.webkitRelativePath) {
                const parts = file.webkitRelativePath.split('/');
                parts.pop(); // remove filename
                const relativeFolder = parts.join('/');
                if (relativeFolder) {
                    // Ensure base path always ends with a single slash before appending subfolder
                    const base = (window.currentFolder && window.currentFolder !== '/')
                        ? window.currentFolder.replace(/\/$/, '') + '/'
                        : '/';
                    // Join base and relative folder, collapsing any accidental double slashes
                    folderPath = `${base}${relativeFolder}`.replace(/\/+/g, '/');
                }
            }

            try {
                const result = await streamingUploader.uploadFile(file, progressCallback, statusCallback, folderPath);
                if (result && result.upload_id && window.pendingUploads) {
                    window.pendingUploads.add(result.upload_id);
                }
                showStatus(`File "${file.name}" uploaded successfully!`, 'success');
            } catch (error) {
                console.error('Upload error:', error);
                showStatus(`Upload failed: ${error.message}`, 'error');
                showRetryButton();
                if (streamingUploader.failedUpload) {
                    streamingUploader.failedUpload.elements = elements;
                }
            }
        };

        const promise = uploadTask();
        allUploads.push(promise);
        executing.push(promise);
        promise.finally(() => {
            const index = executing.indexOf(promise);
            if (index > -1) executing.splice(index, 1);
        });
        if (executing.length >= maxConcurrent) {
            await Promise.race(executing);
        }
    }

    await Promise.all(allUploads);

    const uploadForm = document.getElementById('uploadForm');
    if (uploadForm) {
        uploadForm.reset();
    }

    showStatus('Finalizing uploads...', 'info');

    setTimeout(() => {
        if (progressContainer) {
            progressContainer.innerHTML = '';
        }
    }, 3000);
};

async function resumeFailedUpload() {
    if (!streamingUploader || !streamingUploader.failedUpload) {
        showStatus('No failed upload to resume.', 'error');
        return;
    }
    const { uploadId, file, elements } = streamingUploader.failedUpload;

    let progressElements = elements;
    if (!progressElements) {
        progressElements = createProgressBarElements(file.name, file.size);
        streamingUploader.failedUpload.elements = progressElements;
    }

    // Get authoritative progress from the server before resuming
    try {
        const statusResp = await fetch(`/api/upload/status/${uploadId}`);
        if (statusResp.ok) {
            const statusData = await statusResp.json();
            if (statusData && typeof statusData.bytes_uploaded === 'number') {
                streamingUploader.failedUpload.bytesUploaded = statusData.bytes_uploaded;
            }
        }
    } catch (statusErr) {
        console.warn('Failed to fetch resume status:', statusErr);
    }

    const alreadyUploaded = streamingUploader.failedUpload.bytesUploaded || 0;
    updateIndividualProgressBar(
        progressElements,
        Math.floor((alreadyUploaded / file.size) * 100),
        alreadyUploaded
    );

    const progressCallback = (progress, bytesUploaded) => {
        updateIndividualProgressBar(progressElements, Math.floor(progress), bytesUploaded);
    };
    const statusCallback = (message, type) => { showStatus(message, type); };
    try {
        const result = await streamingUploader.resumeUpload(uploadId, progressCallback, statusCallback);
        if (result && result.upload_id && window.pendingUploads) {
            window.pendingUploads.add(result.upload_id);
        }
        showStatus(`File "${file.name}" uploaded successfully!`, 'success');
        showStatus('Finalizing uploads...', 'info');
    } catch (error) {
        console.error('Resume upload error:', error);
        showStatus(`Resume failed: ${error.message}`, 'error');
        showRetryButton();
    }
}

// Initialize when page loads
document.addEventListener('DOMContentLoaded', function () {
    initializeStreamingUploader();

    // Update upload button click handler
    const uploadButton = document.querySelector('button[onclick="uploadFile()"]');
    if (uploadButton) {
        uploadButton.onclick = uploadFile;
    }

    const folderButton = document.getElementById('selectFolderButton');
    if (folderButton) {
        folderButton.addEventListener('click', () => {
            const folderInput = document.getElementById('folderInput');
            if (folderInput) {
                folderInput.click();
            }
        });
    }

    const searchBtn = document.getElementById('searchButton');
    const searchInput = document.getElementById('searchInput');
    if (searchBtn && searchInput) {
        searchBtn.addEventListener('click', () => {
            const query = searchInput.value.trim();
            if (query) {
                searchFiles(query);
            } else {
                loadFiles();
            }
        });
        searchInput.addEventListener('keyup', (e) => {
            if (e.key === 'Enter') {
                searchBtn.click();
            }
        });
    }

    if (selectAllBtn && !selectAllBtn.dataset.listenerAdded) {
        selectAllBtn.addEventListener('click', () => {
            const checkboxes = document.querySelectorAll('.select-item');
            const allChecked = Array.from(checkboxes).every(cb => cb.checked);
            checkboxes.forEach(cb => { cb.checked = !allChecked; });
            updateBulkActionButtons();
        });
        selectAllBtn.dataset.listenerAdded = 'true';
    }
    document.querySelectorAll('.select-item').forEach(cb => cb.addEventListener('change', updateBulkActionButtons));
    updateBulkActionButtons();
    markEncryptedRows();
    if (window.nextCursor !== null && window.nextCursor !== undefined) {
        fetchRemainingEntries();
    }
    setInterval(checkForUpdates, 5000);
});

// Function to refresh file list - WITH DIAGNOSTIC LOGGING
async function loadFiles() {
    try {
        console.log('🔍 DIAGNOSTIC: loadFiles() called from streaming upload');
        console.log('🔍 DIAGNOSTIC: Fetching entry list from /api/entries...');

        const folderParam = encodeURIComponent(window.currentFolder || '/');
        const response = await fetch(`/api/entries?folder=${folderParam}`);
        if (!response.ok) {
            throw new Error('Failed to fetch file list');
        }

        const data = await response.json();
        console.log('🔍 DIAGNOSTIC: API Response received:', data);

        if (!data.entries) {
            throw new Error('Invalid response format');
        }

        renderEntries(data.entries);
        refreshServerCache();
    } catch (error) {
        console.error('🚨 DIAGNOSTIC: Error loading files:', error);
        showStatus('Failed to refresh file list: ' + error.message, 'error');
    }
}

function renderEntries(entries) {
    console.log('🔍 DIAGNOSTIC: Entries array length:', entries.length);
    if (entries.length > 0) {
        console.log('🔍 DIAGNOSTIC: First entry structure:', entries[0]);
    }

    // Keep a copy of entries for local state updates
    window.cachedEntries = entries;

    const filesContainer = document.getElementById('files-container');
    if (!filesContainer) {
        console.error('🚨 DIAGNOSTIC: Files container not found');
        return;
    }

    if (entries.length === 0) {
        filesContainer.innerHTML = `
            <div class="alert alert-info text-center">
                <p><i class="fas fa-info-circle mr-2"></i>No files found. Upload your first file above.</p>
            </div>
        `;
        return;
    }

    let tableHTML = `
        <div class="table-responsive">
            <table class="table" id="fileTable">
                <thead>
                    <tr>
                        <th></th>
                        <th><i class="fas fa-file mr-1"></i> Filename</th>
                        <th><i class="fas fa-weight mr-1"></i> Size</th>
                        <th>Folder</th>
                        <th><i class="fas fa-link mr-1"></i> Public Link</th>
                        <th><i class="fas fa-lock-open mr-1"></i> Public Access</th>
                        <th><i class="fas fa-cogs mr-1"></i> Actions</th>
                    </tr>
                </thead>
                <tbody>
    `;

    entries.forEach(entry => {
        if (entry.type === 'folder') {
            tableHTML += `
            <tr class="folder-row" data-folder-path="${entry.full_path}">
                <td><input type="checkbox" class="select-item" data-type="folder" data-id="${entry.id}"></td>
                <td><i class="fas fa-folder mr-1"></i><strong>${entry.name}</strong></td>
                <td class="filesize-cell">${formatFileSize(entry.size)}</td>
                <td>${entry.full_path}</td>
                <td></td>
                <td></td>
                <td>
                    <a href="/?folder=${encodeURIComponent(entry.full_path)}" class="btn btn-primary btn-sm">
                        <i class="fas fa-folder-open mr-1"></i>Open
                    </a>
                    <button class="btn btn-secondary btn-sm rename-folder-btn" data-folder-id="${entry.id}" data-folder-name="${entry.name}">
                        <i class="fas fa-edit mr-1"></i>Rename
                    </button>
                    <button class="btn btn-danger btn-sm delete-folder-btn" data-folder-id="${entry.id}" data-folder-path="${entry.full_path}">
                        <i class="fas fa-trash-alt mr-1"></i>Delete
                    </button>
                </td>
            </tr>`;
        } else {
            const fileId = entry.id || '';
            const fileHash = entry.file_hash || '';
            const isPublic = entry.is_public || false;
            const fileInfo = getFileTypeInfo(entry.name);
            const viewButtonHTML = fileInfo.isViewable && fileHash ? createViewButton(fileHash, fileInfo.type, entry.name) : '';

            tableHTML += `
            <tr data-file-id="${fileId}" data-file-hash="${fileHash}" data-filename="${entry.name}" data-encryption-alg="${entry.encryption_alg || 'none'}" data-iv="${entry.iv || 'none'}" data-key-fingerprint="${entry.key_fingerprint || 'none'}">
                <td><input type="checkbox" class="select-item" data-type="file" data-id="${fileId}"></td>
                <td>
                    <span style="margin-right: 8px;">${fileInfo.icon}</span>
                    <strong>${entry.name}</strong>
                </td>
                <td class="filesize-cell">${formatFileSize(entry.size)}</td>
                <td>${entry.folder}</td>
                <td>
                    ${fileHash ?
                `<a href="/d/${fileHash}" target="_blank" class="public-link">
                        <i class="fas fa-external-link-alt mr-1"></i>${window.location.origin}/d/${fileHash.substring(0,10)}...
                    </a>` :
                '<span class="text-muted">N/A</span>'}
                </td>
                <td>
                    <label class="switch">
                        <input type="checkbox" class="public-toggle" data-file-id="${fileId}" data-file-hash="${fileHash}" ${isPublic ? 'checked' : ''}>
                        <span class="slider round"></span>
                    </label>
                </td>
                <td class="action-buttons">
                    ${viewButtonHTML}
                    <a href="/d/${fileHash}" class="btn btn-primary btn-sm download-btn" data-filename="${entry.name}">
                        <i class="fas fa-download mr-1"></i>Download
                    </a>
                    <div class="btn-group">
                        <button type="button" class="btn btn-secondary btn-sm dropdown-toggle" data-toggle="dropdown" aria-haspopup="true" aria-expanded="false">
                            <i class="fas fa-bars"></i>
                        </button>
                        <div class="dropdown-menu dropdown-menu-right">
                            <button type="button" class="dropdown-item rename-btn" data-file-id="${fileId}"><i class="fas fa-edit mr-1"></i>Rename</button>
                            <button type="button" class="dropdown-item move-btn" data-file-id="${fileId}"><i class="fas fa-folder-open mr-1"></i>Move</button>
                            <button type="button" class="dropdown-item delete-btn" data-file-id="${fileId}" data-file-hash="${fileHash}"><i class="fas fa-trash-alt mr-1"></i>Delete</button>
                        </div>
                    </div>
                </td>
            </tr>`;
        }
    });

    tableHTML += `</tbody></table></div>`;
    filesContainer.innerHTML = tableHTML;

    setupFileActionEventHandlers();
    setupFolderActionEventHandlers();
    document.querySelectorAll('.select-item').forEach(cb => cb.addEventListener('change', updateBulkActionButtons));
    updateBulkActionButtons();

    if (typeof initializeFileTypeIcons === 'function') {
        initializeFileTypeIcons();
    }

    document.dispatchEvent(new CustomEvent('contentUpdated'));
}

async function searchFiles(query) {
    try {
        const response = await fetch(`/api/files/search?q=${encodeURIComponent(query)}`);
        if (!response.ok) {
            throw new Error('Failed to search files');
        }
        const data = await response.json();
        if (!data.entries) {
            throw new Error('Invalid response format');
        }
        renderEntries(data.entries);
    } catch (error) {
        console.error('🚨 DIAGNOSTIC: Error searching files:', error);
        showStatus('Failed to search files: ' + error.message, 'error');
    }
}
// Utility function to format file sizes (matching the server-side filter)
function formatFileSize(bytes, decimals = 2) {
    if (bytes === 0) return '0 Bytes';
    const k = 1000; // Use 1000 instead of 1024 to match server-side formatting
    const dm = decimals < 0 ? 0 : decimals;
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
}

// File type detection functions for dynamic file list
function getFileTypeInfo(filename) {
    const ext = filename.split('.').pop().toLowerCase();
    
    // Define supported media types
    const mediaTypes = {
        video: ['mp4', 'webm', 'avi', 'mov', 'mkv', 'wmv', 'flv', 'm4v'],
        audio: ['mp3', 'wav', 'ogg', 'aac', 'flac', 'm4a', 'wma'],
        image: ['jpg', 'jpeg', 'png', 'gif', 'webp', 'svg', 'bmp', 'tiff'],
        pdf: ['pdf']
    };

    // Check if file is viewable
    for (const [type, extensions] of Object.entries(mediaTypes)) {
        if (extensions.includes(ext)) {
            return {
                type: type,
                icon: getFileTypeIcon(type),
                isViewable: true,
                extension: ext
            };
        }
    }

    // Default for non-viewable files
    return {
        type: 'file',
        icon: '📄',
        isViewable: false,
        extension: ext
    };
}

function getFileTypeIcon(type) {
    const icons = {
        video: '🎥',
        audio: '🎵',
        image: '🖼️',
        pdf: '📄',
        file: '📄'
    };
    return icons[type] || icons.file;
}

function createViewButton(fileHash, fileType, filename) {
    const viewUrl = `/v/${fileHash}`;
    let buttonClass = 'btn-success';
    let icon = 'fas fa-eye';
    
    // For video files, create both inline player and modal view button
    if (fileType === 'video') {
        const videoId = `video-${fileHash}`;
        return `<div class="video-player-container" style="margin-bottom: 10px;">
                    <video
                        id="${videoId}"
                        controls
                        preload="metadata"
                        playsinline
                        webkit-playsinline
                        muted
                        style="width: 100%; max-width: 400px; height: auto; border-radius: 4px;"
                        poster=""
                        controlslist="nodownload"
                        crossorigin="anonymous"
                        onloadstart="this.setAttribute('data-loading', 'true')"
                        oncanplay="this.removeAttribute('data-loading')"
                        onerror="this.setAttribute('data-error', 'true'); console.error('Video load error for ${filename}');"
                    >
                        <source src="${viewUrl}" type="video/mp4">
                        <source src="${viewUrl}" type="video/webm">
                        <source src="${viewUrl}" type="video/quicktime">
                        Your browser does not support the video tag.
                        <a href="${viewUrl}" class="modal-view-link">View Video</a>
                    </video>
                    <div style="margin-top: 5px;">
                        <a href="${viewUrl}" class="btn btn-success btn-sm modal-view-btn" data-filename="${filename}" data-filetype="video">
                            <i class="fas fa-expand mr-1"></i>Fullscreen
                        </a>
                        <button onclick="toggleMute('${videoId}')" class="btn btn-secondary btn-sm">
                            <i class="fas fa-volume-mute mr-1"></i>Unmute
                        </button>
                    </div>
                </div>`;
    }
    
    // For audio files, create both inline player and modal view button
    if (fileType === 'audio') {
        return `<div class="audio-player-container" style="margin-bottom: 10px;">
                    <audio
                        controls
                        preload="metadata"
                        style="width: 100%; max-width: 400px; height: 40px;"
                        crossorigin="anonymous"
                    >
                        <source src="${viewUrl}" type="audio/mpeg">
                        <source src="${viewUrl}" type="audio/wav">
                        <source src="${viewUrl}" type="audio/ogg">
                        Your browser does not support the audio tag.
                        <a href="${viewUrl}" class="modal-view-link">Play Audio</a>
                    </audio>
                    <div style="margin-top: 5px;">
                        <a href="${viewUrl}" class="btn btn-success btn-sm modal-view-btn" data-filename="${filename}" data-filetype="audio">
                            <i class="fas fa-external-link-alt mr-1"></i>Open
                        </a>
                    </div>
                </div>`;
    }
    
    // Customize button based on file type for other files
    switch(fileType) {
        case 'image':
            icon = 'fas fa-image';
            buttonClass = 'btn-success';
            break;
        case 'pdf':
            icon = 'fas fa-file-pdf';
            buttonClass = 'btn-success';
            break;
    }

    return `<a href="${viewUrl}" class="btn ${buttonClass} btn-sm modal-view-btn" data-filename="${filename}" data-filetype="${fileType}">
                <i class="${icon} mr-1"></i>View
            </a>`;
}

// Helper function to toggle video mute (useful for iOS autoplay policies)
function toggleMute(videoId) {
    const video = document.getElementById(videoId);
    const button = event.target.closest('button');
    
    if (video) {
        if (video.muted) {
            video.muted = false;
            button.innerHTML = '<i class="fas fa-volume-up mr-1"></i>Mute';
            button.classList.remove('btn-secondary');
            button.classList.add('btn-warning');
        } else {
            video.muted = true;
            button.innerHTML = '<i class="fas fa-volume-mute mr-1"></i>Unmute';
            button.classList.remove('btn-warning');
            button.classList.add('btn-secondary');
        }
    }
}

// Set up event handlers for file actions (delete, public toggle)
function setupFileActionEventHandlers(root = document) {
    // Add event handlers for delete buttons
    root.querySelectorAll('.delete-btn').forEach(button => {
        if (button.dataset.listenerAttached) return;
        button.dataset.listenerAttached = 'true';
        button.addEventListener('click', async function () {
            const fileId = this.dataset.fileId;
            const fileHash = this.dataset.fileHash || this.closest('tr').dataset.fileHash;

            if (!confirm('Are you sure you want to delete this file?')) {
                return;
            }

            try {
                const response = await fetch('/delete_file', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify({
                        file_id: fileId,
                        file_hash: fileHash
                    })
                });

                if (!response.ok) {
                    const errorData = await response.json();
                    throw new Error(errorData.error || 'Failed to delete file');
                }

                const responseData = await response.json();
                if (responseData.status === 'success') {
                    this.closest('tr').remove();
                    if (window.cachedEntries) {
                        window.cachedEntries = window.cachedEntries.filter(e => e.id !== fileId);
                    }
                    showStatus('File deleted successfully', 'success');
                    refreshServerCache();

                    // Check if there are any remaining files
                    if (document.querySelectorAll('#fileTable tbody tr').length === 0) {
                        // If no files left, update the container
                        document.getElementById('files-container').innerHTML = `
                            <div class="alert alert-info text-center">
                                <p><i class="fas fa-info-circle mr-2"></i>No files found. Upload your first file above.</p>
                            </div>
                        `;
                    }
                } else {
                    showStatus(responseData.error || 'Failed to delete file', 'error');
                }
            } catch (error) {
                console.error('Delete Error:', error);
                showStatus('Error deleting file: ' + error.message, 'error');
            }
        });
    });
    // Add event handlers for public toggles
    root.querySelectorAll('.public-toggle').forEach(toggle => {
        if (toggle.dataset.listenerAttached) return;
        toggle.dataset.listenerAttached = 'true';
        toggle.addEventListener('change', async function () {
            const fileId = this.dataset.fileId;
            const fileHash = this.dataset.fileHash;
            const isPublic = this.checked;

            try {
                const response = await fetch('/toggle_public_access', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify({
                        file_id: fileId,
                        is_public: isPublic,
                        salted_sha512_hash: fileHash
                    })
                });

                if (!response.ok) {
                    const errorData = await response.json();
                    throw new Error(errorData.error || 'Failed to update public status');
                }

                const responseData = await response.json();
                if (responseData.status === 'success') {
                    showStatus(`File is now ${isPublic ? 'public' : 'private'}`, 'success');
                } else {
                    // Revert toggle if update failed
                    this.checked = !isPublic;
                    showStatus(responseData.error || 'Failed to update public status', 'error');
                }
            } catch (error) {
                console.error('Toggle Public Access Error:', error);
                this.checked = !isPublic; // Revert toggle on error
                showStatus('Error updating public status: ' + error.message, 'error');
            }
        });
    });

    // Intercept download buttons to perform client-side decryption
    root.querySelectorAll('.download-btn').forEach(btn => {
        if (btn.dataset.listenerAttached) return;
        btn.dataset.listenerAttached = 'true';
        btn.addEventListener('click', async function (e) {
            const row = this.closest('tr');
            const alg = row?.dataset.encryptionAlg || 'none';
            if (alg !== 'none') {
                e.preventDefault();
                if (!window.CryptoManager.hasKey(row.dataset.keyFingerprint)) {
                    alert('Missing decryption key for this file.');
                    return;
                }
                try {
                    showStatus('Downloading encrypted file...', 'info');
                    updateProgressBar(0, 'Downloading...');
                    const resp = await fetch(this.href);
                    const total = parseInt(resp.headers.get('Content-Length')) || 0;
                    const reader = resp.body.getReader();
                    let received = 0;
                    const chunks = [];
                    while (true) {
                        const { done, value } = await reader.read();
                        if (done) break;
                        chunks.push(value);
                        received += value.length;
                        if (total) {
                            updateProgressBar((received / total) * 50, 'Downloading...');
                        }
                    }
                    const cipher = new Uint8Array(received);
                    let offset = 0;
                    for (const chunk of chunks) { cipher.set(chunk, offset); offset += chunk.length; }
                    updateProgressBar(50, 'Decrypting...');
                    const plain = await window.CryptoManager.decrypt(cipher, row.dataset.iv, row.dataset.keyFingerprint, p => updateProgressBar(50 + p / 2, 'Decrypting...'));
                    updateProgressBar(100, 'Done');
                    const blob = new Blob([plain]);
                    const url = URL.createObjectURL(blob);
                    const a = document.createElement('a');
                    a.href = url;
                    a.download = this.dataset.filename || row.dataset.filename || 'download';
                    document.body.appendChild(a);
                    a.click();
                    a.remove();
                    URL.revokeObjectURL(url);
                    showStatus('Download complete', 'success');
                } catch (err) {
                    showStatus('Failed to decrypt file: ' + err.message, 'error');
                }
            }
        });
    });

    // Add event handlers for rename buttons
    root.querySelectorAll('.rename-btn').forEach(btn => {
        if (btn.dataset.listenerAttached) return;
        btn.dataset.listenerAttached = 'true';
        btn.addEventListener('click', async function () {
            const fileId = this.dataset.fileId;
            const newName = prompt('New filename:');
            if (!newName) return;
            await fetch('/update_file_metadata', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ file_id: fileId, filename: newName })
            });
            refreshServerCache();
            setTimeout(() => {
                if (typeof loadFiles === 'function') {
                    loadFiles();
                }
            }, 500);
        });
    });

    // Add event handlers for move buttons
    root.querySelectorAll('.move-btn').forEach(btn => {
        if (btn.dataset.listenerAttached) return;
        btn.dataset.listenerAttached = 'true';
        btn.addEventListener('click', function () {
            const fileId = this.dataset.fileId;
            openMoveDialog([fileId], []);
        });
    });

    // Update encrypted file indicators
    markEncryptedRows(root);
}

// Set up event handlers for folder actions (rename, delete)
function setupFolderActionEventHandlers(root = document) {
    root.querySelectorAll('.rename-folder-btn').forEach(btn => {
        if (btn.dataset.listenerAttached) return;
        btn.dataset.listenerAttached = 'true';
        btn.addEventListener('click', async function () {
            const folderId = this.dataset.folderId;
            const currentName = this.dataset.folderName || '';
            const newName = prompt('New folder name:', currentName);
            if (!newName) return;
            try {
                const resp = await fetch('/rename_folder', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ folder_id: folderId, new_name: newName })
                });
                if (!resp.ok) {
                    alert('Failed to rename folder');
                    return;
                }
                loadFiles();
            } catch (error) {
                console.error('Rename folder error:', error);
                alert('Error renaming folder: ' + error.message);
            }
        });
    });

    root.querySelectorAll('.delete-folder-btn').forEach(btn => {
        if (btn.dataset.listenerAttached) return;
        btn.dataset.listenerAttached = 'true';
        btn.addEventListener('click', async function () {
            const folderId = this.dataset.folderId;
            let resp = await fetch('/delete_folder', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ folder_id: folderId })
            });
            if (!resp.ok) {
                alert('Failed to check folder');
                return;
            }
            const data = await resp.json();
            if (data.needs_confirm) {
                let msg = `Folder is not empty. Delete ${data.file_count} file`;
                msg += data.file_count === 1 ? '' : 's';
                if (data.folder_count > 0) {
                    msg += ` and ${data.folder_count} folder`;
                    msg += data.folder_count === 1 ? '' : 's';
                    if (data.subfolder_file_count > 0) {
                        msg += ` containing ${data.subfolder_file_count} file`;
                        msg += data.subfolder_file_count === 1 ? '' : 's';
                    }
                }
                msg += ' as well?';
                if (!confirm(msg)) {
                    return;
                }
                resp = await fetch('/delete_folder', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ folder_id: folderId, delete_contents: true })
                });
                if (!resp.ok) {
                    alert('Failed to delete folder');
                    return;
                }
            }
            loadFiles();
        });
    });
}

// =============================================
// Folder selection modal for moving files
// =============================================
let moveTargets = { fileIds: [], folderIds: [] };

async function openMoveDialog(fileIds = [], folderIds = []) {
    moveTargets = { fileIds, folderIds };
    const list = document.getElementById('folderList');
    if (!list) {
        return;
    }
    list.innerHTML = '';
    try {
        const resp = await fetch('/api/folders');
        const data = await resp.json();
        (data.folders || []).forEach(folder => {
            const li = document.createElement('li');
            li.className = 'list-group-item folder-option';
            li.textContent = folder.path;
            li.dataset.path = folder.path;
            list.appendChild(li);
        });
    } catch (error) {
        console.error('🚨 DIAGNOSTIC: Error loading folders', error);
        showStatus('Failed to load folders: ' + error.message, 'error');
    }
    $('#moveModal').modal('show');
}

document.addEventListener('DOMContentLoaded', function () {
    const list = document.getElementById('folderList');
    if (list) {
        list.addEventListener('click', async function (e) {
            const item = e.target.closest('.folder-option');
            if (!item) return;
            const path = item.dataset.path;
            $('#moveModal').modal('hide');
            try {
                if (moveTargets.folderIds.length === 0 && moveTargets.fileIds.length === 1) {
                    await fetch('/update_file_metadata', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ file_id: moveTargets.fileIds[0], folder_path: path })
                    });
                } else {
                    await fetch('/move_selected', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ file_ids: moveTargets.fileIds, folder_ids: moveTargets.folderIds, destination: path })
                    });
                }
                refreshServerCache();
                setTimeout(() => {
                    if (typeof loadFiles === 'function') {
                        loadFiles();
                    }
                }, 500);
            } catch (error) {
                console.error('🚨 DIAGNOSTIC: Error moving file', error);
                showStatus('Error moving file: ' + error.message, 'error');
            }
        });
    }
});

/**
 * Performance Improvements Explanation:
 * 
 * 1. **Eliminated Client-Side Chunking Delays**: 
 *    - The old system created 5MB chunks and introduced artificial delays (300-1000ms) between uploads
 *    - The new system streams data continuously without delays, maintaining consistent upload speed
 * 
 * 2. **Reduced Memory Usage**:
 *    - Old system: Cached entire 5MB chunks in memory on both client and server
 *    - New system: Uses small 64KB read buffers and processes data as a stream
 * 
 * 3. **Eliminated WebSocket Overhead**:
 *    - WebSocket protocol adds framing overhead and complexity for binary data
 *    - Direct HTTP streaming is more efficient for large file uploads
 * 
 * 4. **Reduced Server-Side Worker Thread Overhead**:
 *    - Old system: Created new thread for each 5MB chunk
 *    - New system: Single stream processing reduces thread creation overhead
 * 
 * 5. **Better Error Handling**:
 *    - Continuous stream allows for immediate error detection
 *    - Built-in retry mechanisms without re-uploading completed portions
 * 
 * 6. **Notion API Compliance**:
 *    - Server automatically handles single-part (≤20MB) vs multipart (>20MB) decisions
 *    - Proper 5MB chunk sizes for multipart uploads as required by Notion API
 *    - Handles complete multipart upload lifecycle (init, upload parts, complete/abort)
 */
