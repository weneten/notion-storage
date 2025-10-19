// Global reference to spinning icon element to maintain continuous animation
let persistentSpinningIcon = null;

// Function to get or create a persistent spinning icon
function getSpinningIcon() {
    if (!persistentSpinningIcon) {
        persistentSpinningIcon = document.createElement('i');
        persistentSpinningIcon.className = 'fas fa-cog fa-spin mr-2';
    }
    return persistentSpinningIcon;
}

// Utility function to format file sizes
function formatFileSize(bytes, decimals = 2) {
    if (bytes === 0) return '0 Bytes';

    // Use 1000 instead of 1024 to get MB/GB instead of MiB/GiB
    const k = 1000;
    const dm = decimals < 0 ? 0 : decimals;
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
}

// Function to show status messages
function showStatus(message, type) {
    const messageContainer = document.getElementById('messageContainer');
    if (!messageContainer) return;

    // Reset spinning icon since we're showing a different type of message
    persistentSpinningIcon = null;

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
        uploadFile();
    });

    container.appendChild(retryBtn);
}

// Function to update progress bar
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

// Function to reset upload state
function resetUploadState() {
    // Reset progress bar
    updateProgressBar(0, '');

    // Hide progress bar
    const progressBarContainer = document.getElementById('progressBarContainer');
    if (progressBarContainer) {
        progressBarContainer.style.display = 'none';
    }

    // Clear status messages
    const messageContainer = document.getElementById('messageContainer');
    if (messageContainer) {
        messageContainer.innerHTML = '';
    }

    // Reset persistent spinning icon
    persistentSpinningIcon = null;

    // Reset file input
    const fileInput = document.getElementById('fileInput');
    if (fileInput) {
        fileInput.value = '';
    }
}

// Preserve checkbox selections across table refreshes
window.preservedSelections = window.preservedSelections || new Set();

function captureSelectedItems() {
    window.preservedSelections = new Set(
        Array.from(document.querySelectorAll('.select-item:checked'))
            .map(cb => `${cb.dataset.type}:${cb.dataset.id}`)
    );
}

function restoreSelectedItems() {
    if (!window.preservedSelections) return;
    window.preservedSelections.forEach(key => {
        const [type, id] = key.split(':');
        const cb = document.querySelector(`.select-item[data-type="${type}"][data-id="${id}"]`);
        if (cb) cb.checked = true;
    });
    if (typeof updateBulkActionButtons === 'function') {
        updateBulkActionButtons();
    }
}

// Function to load files (refresh file list)
async function loadFiles() {
    try {
        // Preserve state of any open dropdown menu before refresh
        let openDropdownId = null;
        let dropdownScroll = 0;
        const openMenu = document.querySelector('.action-buttons .dropdown-menu.show');
        if (openMenu) {
            const row = openMenu.closest('tr');
            if (row) openDropdownId = row.dataset.fileId;
            dropdownScroll = openMenu.scrollTop;
        }

        // Capture currently selected checkboxes
        captureSelectedItems();

        // Preserve current scroll positions before refreshing
        const filesContainer = document.getElementById('files-container');
        const containerScroll = filesContainer ? filesContainer.scrollTop : 0;
        const windowScroll = window.scrollY;

        console.log('Refreshing file list with AJAX...');
        // Fetch the file list data from the API
        const folderParam = encodeURIComponent(window.currentFolder || '/');
        const response = await fetch(`/files-api?folder=${folderParam}`, {
            credentials: 'include'
        });

        if (response.redirected) {
            // If the user was redirected (e.g. session expired), follow the redirect
            window.location.href = response.url;
            return;
        }

        if (!response.ok) {
            throw new Error(`Failed to fetch file list (${response.status})`);
        }

        // Ensure the response is JSON before attempting to parse
        let data;
        const contentType = response.headers.get('content-type') || '';
        if (contentType.includes('application/json')) {
            data = await response.json();
        } else {
            const text = await response.text();
            throw new Error(`Unexpected response: ${text.slice(0, 100)}`);
        }

        if (!data.files) {
            throw new Error('Invalid response format');
        }

        // Get the files container to update
        if (!filesContainer) {
            console.error('Files container not found');
            return;
        }

        // If there are no files, show empty state
        if (data.files.length === 0) {
            filesContainer.innerHTML = `
                <div class="alert alert-info text-center">
                    <p><i class="fas fa-info-circle mr-2"></i>No files found. Upload your first file above.</p>
                </div>
            `;
            return;
        }

        // Generate the table HTML to match the initial server-rendered table
        let tableHTML = `
                <table class="table" id="fileTable">
                    <thead>
                        <tr>
                            <th class="select-column"></th>
                            <th class="filename-column"><i class="fas fa-file mr-1"></i> Filename</th>
                            <th class="size-column"><i class="fas fa-weight mr-1"></i> Size</th>
                            <th class="folder-column">Folder</th>
                            <th class="public-link-column"><i class="fas fa-link mr-1"></i> Public Link</th>
                            <th class="public-access-column"><i class="fas fa-lock-open mr-1"></i> Public Access</th>
                            <th><i class="fas fa-cogs mr-1"></i> Actions</th>
                        </tr>
                    </thead>
                    <tbody>
        `;

        // Generate rows for each file
        data.files.forEach(file => {
            const fileId = file.id || '';
            const fileHash = file.file_hash || '';
            const saltedHash = file.salted_hash || fileHash;
            const isPublic = file.is_public || false;

            tableHTML += `
                <tr data-file-id="${fileId}" data-file-hash="${fileHash}">
                    <td class="select-column"><input type="checkbox" class="select-item" data-type="file" data-id="${fileId}"></td>
                    <td class="filename-column">
                        <span class="file-type-icon" data-filename="${file.name}"></span>
                        <strong>${file.name}</strong>
                    </td>
                    <td class="size-column filesize-cell">${formatFileSize(file.size)}</td>
                    <td class="folder-column">${file.folder || window.currentFolder || ''}</td>
                    <td class="public-link-column">
                        ${saltedHash ?
                    `<a href="/d/${saltedHash}" target="_blank" class="public-link">
                                <i class="fas fa-external-link-alt mr-1"></i>${window.location.origin}/d/${saltedHash.substring(0, 10)}...
                            </a>` :
                    '<span class="text-muted">N/A</span>'
                }
                    </td>
                    <td class="public-access-column">
                        <label class="switch">
                            <input type="checkbox" class="public-toggle" ${isPublic ? 'checked' : ''}>
                            <span class="slider round"></span>
                        </label>
                    </td>
                    <td>
                        <div class="action-buttons">
                            <span class="view-button-container" data-filename="${file.name}" data-hash="${saltedHash}" data-filesize="${formatFileSize(file.size)}"></span>
                            <a href="/d/${saltedHash}" class="btn btn-primary btn-sm">
                                <i class="fas fa-download mr-1"></i>Download
                            </a>
                            <button class="btn btn-danger btn-sm delete-btn" data-file-id="${fileId}">
                                <i class="fas fa-trash-alt mr-1"></i>Delete
                            </button>
                        </div>
                    </td>
                </tr>
            `;
        });

        // Close the table
        tableHTML += `
                    </tbody>
                </table>
        `;

        // Update the container
        filesContainer.innerHTML = tableHTML;

        // Add event listeners to the buttons
        setupFileActionEventHandlers();

        // Reinitialize file type icons for dynamically loaded files
        if (typeof initializeFileTypeIcons === 'function') {
            initializeFileTypeIcons();
        }

        // Restore previously selected checkboxes
        restoreSelectedItems();

        // Restore previously open dropdown menu and its scroll position
        if (openDropdownId) {
            const row = document.querySelector(`tr[data-file-id="${openDropdownId}"]`);
            if (row) {
                const btnGroup = row.querySelector('.btn-group');
                const menu = row.querySelector('.dropdown-menu');
                const toggle = row.querySelector('.dropdown-toggle');
                if (btnGroup && menu && toggle) {
                    btnGroup.classList.add('show');
                    menu.classList.add('show');
                    toggle.setAttribute('aria-expanded', 'true');
                    menu.scrollTop = dropdownScroll;
                }
            }
        }

        // Restore scroll positions
        if (filesContainer && filesContainer.scrollHeight > filesContainer.clientHeight) {
            filesContainer.scrollTop = containerScroll;
        }
        window.scrollTo(0, windowScroll);
        console.log('File list refreshed successfully');
    } catch (error) {
        console.error('Error loading files:', error);
        showStatus('Failed to refresh file list: ' + error.message, 'error');
    }
}

// Set up event handlers for file actions (delete, public toggle)
function setupFileActionEventHandlers() {
    // Add event handlers for delete buttons
    document.querySelectorAll('.delete-btn').forEach(button => {
        button.addEventListener('click', async function () {
            const fileId = this.dataset.fileId;
            const fileHash = this.closest('tr').dataset.fileHash;

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
                    showStatus('File deleted successfully', 'success');

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
    document.querySelectorAll('.public-toggle').forEach(toggle => {
        toggle.addEventListener('change', async function () {
            const row = this.closest('tr');
            const fileId = row.dataset.fileId;
            const fileHash = row.dataset.fileHash;
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
}

// Function to show initialization message
function showInitializingMessage(message) {
    const messageContainer = document.getElementById('messageContainer');
    if (!messageContainer) return;

    // Check if there's already an initializing message displayed
    const existingAlert = messageContainer.querySelector('.alert-processing');

    if (existingAlert) {
        // If a message already exists, just update the text content
        // Preserve the existing icon by finding and updating only text nodes
        let hasUpdatedText = false;
        for (let i = 0; i < existingAlert.childNodes.length; i++) {
            const node = existingAlert.childNodes[i];
            if (node.nodeType === Node.TEXT_NODE) {
                node.nodeValue = message;
                hasUpdatedText = true;
                break;
            }
        }

        // If we couldn't find a text node to update, append message after icon
        if (!hasUpdatedText) {
            // Clear all nodes except the icon
            while (existingAlert.firstChild) {
                existingAlert.removeChild(existingAlert.firstChild);
            }

            // Add the icon and message back
            existingAlert.appendChild(getSpinningIcon());
            existingAlert.appendChild(document.createTextNode(message));
        }
    } else {
        // Create a new alert if none exists
        messageContainer.innerHTML = '';

        const alertDiv = document.createElement('div');
        alertDiv.className = 'alert alert-processing';
        alertDiv.style.backgroundColor = '#311b3f !important';  // Darker purple background (matching finalizing)
        alertDiv.style.borderColor = '#bb86fc !important';      // Purple border (matching finalizing)
        alertDiv.style.color = '#bb86fc !important';            // Purple text (matching finalizing)

        // Append the persistent spinning icon and message text
        alertDiv.appendChild(getSpinningIcon());
        alertDiv.appendChild(document.createTextNode(message));

        messageContainer.appendChild(alertDiv);
    }
}

// Function to show finalization message
function showFinalizingMessage(message) {
    const messageContainer = document.getElementById('messageContainer');
    if (!messageContainer) return;

    // Check if there's already a processing or finalizing message displayed
    let existingAlert = messageContainer.querySelector('.alert-finalizing');
    if (!existingAlert) {
        existingAlert = messageContainer.querySelector('.alert-processing');
    }

    if (existingAlert) {
        // Change class to finalizing if it was processing
        if (existingAlert.classList.contains('alert-processing')) {
            existingAlert.classList.remove('alert-processing');
            existingAlert.classList.add('alert-finalizing');
        }

        // If a message already exists, just update the text content
        // Preserve the existing icon by finding and updating only text nodes
        let hasUpdatedText = false;
        for (let i = 0; i < existingAlert.childNodes.length; i++) {
            const node = existingAlert.childNodes[i];
            if (node.nodeType === Node.TEXT_NODE) {
                node.nodeValue = message;
                hasUpdatedText = true;
                break;
            }
        }

        // If we couldn't find a text node to update, append message after icon
        if (!hasUpdatedText) {
            // Clear all nodes except the icon
            while (existingAlert.firstChild) {
                existingAlert.removeChild(existingAlert.firstChild);
            }

            // Add the icon and message back
            existingAlert.appendChild(getSpinningIcon());
            existingAlert.appendChild(document.createTextNode(message));
        }
    } else {
        // Create a new alert if none exists
        messageContainer.innerHTML = '';

        const alertDiv = document.createElement('div');
        alertDiv.className = 'alert alert-finalizing';
        alertDiv.style.backgroundColor = '#311b3f !important';  // Darker purple background
        alertDiv.style.borderColor = '#bb86fc !important';      // Purple border
        alertDiv.style.color = '#bb86fc !important';            // Purple text

        // Append the persistent spinning icon and message text
        alertDiv.appendChild(getSpinningIcon());
        alertDiv.appendChild(document.createTextNode(message));

        messageContainer.appendChild(alertDiv);
    }
}

// Function to upload a file with WebSockets for faster transfers
const uploadFile = async () => {
    const fileInput = document.getElementById('fileInput');

    if (!fileInput.files || fileInput.files.length === 0) {
        showStatus('Please select a file first', 'error');
        return;
    }

    const file = fileInput.files[0];
    const fileName = file.name;
    const fileSize = file.size;

    // Reset UI state
    resetUploadState();

    // Upload state management
    let uploadState = {
        isActive: true,
        socket: null,
        uploadId: null,
        completedParts: 0,
        totalParts: 0,
        uploadedBytes: 0,
        lastProgressUpdate: Date.now(),
        retryCount: 0,
        maxRetries: 5,
        timeoutHandle: null,
        chunkQueue: [],
        currentChunk: null,
        isProcessingChunk: false
    };

    // Cleanup function
    const cleanup = () => {
        uploadState.isActive = false;
        if (uploadState.socket) {
            uploadState.socket.disconnect();
            uploadState.socket = null;
        }
        if (uploadState.timeoutHandle) {
            clearTimeout(uploadState.timeoutHandle);
            uploadState.timeoutHandle = null;
        }
    };

    // Progress timeout handler
    const resetProgressTimeout = () => {
        if (uploadState.timeoutHandle) {
            clearTimeout(uploadState.timeoutHandle);
        }
        uploadState.timeoutHandle = setTimeout(() => {
            if (uploadState.isActive) {
                console.error('Upload timeout - no progress for 60 seconds');
                cleanup();
                showStatus('Upload timeout - no progress detected. Please try again.', 'error');
            }
        }, 60000); // 60 second timeout
    };

    try {
        // Step 1: Initialize upload and get upload ID
        showInitializingMessage(`Initializing upload for ${fileName} (${formatFileSize(fileSize)})...`);
        const initResponse = await fetch('/init_upload', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                filename: fileName,
                fileSize: fileSize
            })
        });

        if (!initResponse.ok) {
            const errorData = await initResponse.json();
            throw new Error(`Upload initialization failed: ${errorData.error || 'Unknown error'}`);
        }

        const uploadData = await initResponse.json();
        uploadState.uploadId = uploadData.id;
        const salt = uploadData.salt;
        uploadState.totalParts = uploadData.total_parts || 1;

        console.log(`Upload initialized with ID: ${uploadState.uploadId}, ${uploadState.totalParts} parts`);
        updateProgressBar(0, `Preparing upload: ${formatFileSize(0)}/${formatFileSize(fileSize)}`);

        // Update the initialization message to show we're now uploading
        showInitializingMessage(`Uploading file: ${fileName} (${formatFileSize(fileSize)})...`);

        // Step 2: Create Socket.IO connection for chunk uploads
        const socketConfig = {
            ...window.socketIOConfig,
            query: { upload_id: uploadState.uploadId },
            timeout: 30000,
            forceNew: true
        };

        console.log('Creating Socket.IO connection with config:', socketConfig);
        uploadState.socket = io('/ws/upload', socketConfig);
        uploadState.socket.binaryType = 'arraybuffer';

        // Determine if this is a large file (over 500MB)
        const isLargeFile = fileSize > 500 * 1024 * 1024;

        // Chunk size and timing
        const chunkSize = 5 * 1024 * 1024; // 5MB chunks for Notion API
        const delayBetweenChunks = isLargeFile ? 1000 : 300; // ms

        // Create upload queue
        for (let partNumber = 1; partNumber <= uploadState.totalParts; partNumber++) {
            const start = (partNumber - 1) * chunkSize;
            const end = Math.min(start + chunkSize, fileSize);
            const isLastChunk = partNumber === uploadState.totalParts;

            uploadState.chunkQueue.push({
                partNumber,
                start,
                end,
                isLastChunk,
                size: end - start,
                retryCount: 0
            });
        }
        // Socket.IO event handlers with robust error handling
        uploadState.socket.on('message', (data) => {
            if (!uploadState.isActive) return;

            try {
                const response = JSON.parse(data);
                console.log('Socket.IO message:', response);

                // Reset progress timeout on any message
                resetProgressTimeout();
                uploadState.lastProgressUpdate = Date.now();

                if (response.status === 'ready_for_chunk') {
                    // Server is ready for the next chunk
                    uploadState.retryCount = 0; // Reset retry count
                    setTimeout(() => {
                        if (uploadState.isActive) processNextChunk();
                    }, delayBetweenChunks);
                }
                else if (response.status === 'binary_received') {
                    // Binary data was received by the server
                    console.log(`Binary data for part ${response.part_number} received by server`);
                }
                else if (response.status === 'chunk_received') {
                    // Chunk successfully received and processed
                    const partNumber = response.part_number;
                    console.log(`Part ${partNumber}/${uploadState.totalParts} upload confirmed`);

                    // Update progress
                    uploadState.completedParts++;
                    const progress = Math.floor((uploadState.completedParts / uploadState.totalParts) * 100);
                    uploadState.uploadedBytes += response.chunk_size || 0;
                    updateProgressBar(progress, `Uploading: ${formatFileSize(uploadState.uploadedBytes)}/${formatFileSize(fileSize)}`);

                    // Mark current chunk as completed
                    uploadState.isProcessingChunk = false;
                    uploadState.retryCount = 0;

                    // Continue with next chunk or finalize
                    if (uploadState.chunkQueue.length > 0) {
                        setTimeout(() => {
                            if (uploadState.isActive) {
                                uploadState.socket.emit('message', JSON.stringify({
                                    action: 'ready_for_next_chunk',
                                    upload_id: uploadState.uploadId
                                }));
                            }
                        }, delayBetweenChunks);
                    } else {
                        // All chunks uploaded, request finalization
                        uploadState.socket.emit('message', JSON.stringify({
                            action: 'finalize',
                            upload_id: uploadState.uploadId,
                            total_parts: uploadState.totalParts
                        }));

                        updateProgressBar(100, '');
                        showFinalizingMessage('Transferring to storage server...');
                    }
                }
                else if (response.status === 'finalized') {
                    // Upload successfully finalized
                    console.log('Upload finalized:', response);
                    updateProgressBar(100, '');
                    showStatus(`Upload completed successfully. File ID: ${response.file_id}`, 'success');

                    cleanup();
                    loadFiles(); // Refresh file list
                }
                else if (response.status === 'error') {
                    throw new Error(response.message || 'Unknown WebSocket error');
                }
            } catch (error) {
                console.error('Error processing Socket.IO message:', error);
                handleUploadError(error);
            }
        });
        uploadState.socket.on('connect', () => {
            console.log('Socket.IO connection established');
            resetProgressTimeout();
            startHeartbeat(); // Start heartbeat monitoring

            // Tell server we're ready to start uploading
            uploadState.socket.emit('message', JSON.stringify({
                action: 'start_upload',
                upload_id: uploadState.uploadId,
                filename: uploadData.sanitized_filename || fileName,
                original_filename: fileName,
                total_size: fileSize,
                total_parts: uploadState.totalParts,
                salt: salt
            }));
        });

        uploadState.socket.on('connect_error', (error) => {
            console.error('Socket.IO connection error:', error);
            handleUploadError(new Error('Connection error: ' + error.message));
        });

        uploadState.socket.on('disconnect', (reason) => {
            console.log('Socket.IO disconnected:', reason);
            if (uploadState.isActive && uploadState.completedParts < uploadState.totalParts && reason !== 'io client disconnect') {
                handleUploadError(new Error('Connection closed unexpectedly. Please try again.'));
            }
        });

        uploadState.socket.on('error', (error) => {
            console.error('Socket.IO error:', error);
            handleUploadError(new Error('Socket error: ' + (error.message || 'Unknown error')));
        });

        // Heartbeat mechanism to detect connection issues
        const startHeartbeat = () => {
            uploadState.heartbeatInterval = setInterval(() => {
                if (uploadState.isActive && uploadState.socket && uploadState.socket.connected) {
                    // Check if we haven't received any progress in the last 30 seconds
                    const timeSinceLastProgress = Date.now() - uploadState.lastProgressUpdate;
                    if (timeSinceLastProgress > 30000) {
                        console.warn('No progress for 30 seconds, connection may be stalled');
                        // Don't automatically retry here, let the timeout handler deal with it
                    }
                } else if (uploadState.isActive) {
                    console.warn('Socket disconnected during upload');
                    handleUploadError(new Error('Connection lost during upload'));
                }
            }, 10000); // Check every 10 seconds
        };

        const stopHeartbeat = () => {
            if (uploadState.heartbeatInterval) {
                clearInterval(uploadState.heartbeatInterval);
                uploadState.heartbeatInterval = null;
            }
        };

        // Update cleanup function
        const originalCleanup = cleanup;
        cleanup = () => {
            stopHeartbeat();
            originalCleanup();
        };
        const handleUploadError = (error) => {
            if (!uploadState.isActive) return;

            console.error('Upload error:', error);
            uploadState.retryCount++;

            if (uploadState.retryCount <= uploadState.maxRetries) {
                console.log(`Retrying upload (attempt ${uploadState.retryCount}/${uploadState.maxRetries})`);
                showInitializingMessage(`Retrying upload (attempt ${uploadState.retryCount}/${uploadState.maxRetries})...`);

                // Wait before retry with exponential backoff
                const retryDelay = Math.min(1000 * Math.pow(2, uploadState.retryCount - 1), 10000);
                setTimeout(() => {
                    if (uploadState.isActive) {
                        // Reset socket connection
                        if (uploadState.socket) {
                            uploadState.socket.disconnect();
                        }

                        // Recreate socket
                        uploadState.socket = io('/ws/upload', socketConfig);
                        uploadState.socket.binaryType = 'arraybuffer';

                        // Re-attach event handlers (simplified version)
                        attachSocketHandlers();
                    }
                }, retryDelay);
            } else {
                cleanup();
                showStatus(`Upload failed after ${uploadState.maxRetries} attempts: ${error.message}`, 'error');
                showRetryButton();
            }
        };

        // Simplified socket handler attachment for retries
        const attachSocketHandlers = () => {
            // Re-attach the essential handlers for retry
            uploadState.socket.on('connect', () => {
                console.log('Socket.IO reconnected for retry');
                resetProgressTimeout();

                uploadState.socket.emit('message', JSON.stringify({
                    action: 'start_upload',
                    upload_id: uploadState.uploadId,
                    filename: uploadData.sanitized_filename || fileName,
                    original_filename: fileName,
                    total_size: fileSize,
                    total_parts: uploadState.totalParts,
                    salt: salt
                }));
            });

            // Re-attach message handler (same as above but shorter for retry)
            uploadState.socket.on('message', (data) => {
                if (!uploadState.isActive) return;

                try {
                    const response = JSON.parse(data);
                    resetProgressTimeout();

                    if (response.status === 'ready_for_chunk') {
                        setTimeout(() => {
                            if (uploadState.isActive) processNextChunk();
                        }, delayBetweenChunks);
                    } else if (response.status === 'chunk_received') {
                        uploadState.completedParts++;
                        const progress = Math.floor((uploadState.completedParts / uploadState.totalParts) * 100);
                        uploadState.uploadedBytes += response.chunk_size || 0;
                        updateProgressBar(progress, `Uploading: ${formatFileSize(uploadState.uploadedBytes)}/${formatFileSize(fileSize)}`);

                        uploadState.isProcessingChunk = false;

                        if (uploadState.chunkQueue.length > 0) {
                            setTimeout(() => {
                                if (uploadState.isActive) {
                                    uploadState.socket.emit('message', JSON.stringify({
                                        action: 'ready_for_next_chunk',
                                        upload_id: uploadState.uploadId
                                    }));
                                }
                            }, delayBetweenChunks);
                        } else {
                            uploadState.socket.emit('message', JSON.stringify({
                                action: 'finalize',
                                upload_id: uploadState.uploadId,
                                total_parts: uploadState.totalParts
                            }));
                            showFinalizingMessage('Transferring to storage server...');
                        }
                    } else if (response.status === 'finalized') {
                        updateProgressBar(100, '');
                        showStatus(`Upload completed successfully. File ID: ${response.file_id}`, 'success');
                        cleanup();
                        loadFiles();
                    } else if (response.status === 'error') {
                        throw new Error(response.message || 'Unknown WebSocket error');
                    }
                } catch (error) {
                    handleUploadError(error);
                }
            });

            uploadState.socket.on('error', handleUploadError);
            uploadState.socket.on('connect_error', handleUploadError);
        };

        // Function to process the next chunk in the queue
        const processNextChunk = () => {
            if (!uploadState.isActive || uploadState.isProcessingChunk || uploadState.chunkQueue.length === 0) {
                return;
            }

            uploadState.isProcessingChunk = true;
            const chunk = uploadState.chunkQueue.shift();
            uploadState.currentChunk = chunk;

            console.log(`Processing part ${chunk.partNumber}/${uploadState.totalParts} (${formatFileSize(chunk.size)})`);

            try {
                // Extract blob for this chunk
                const chunkBlob = file.slice(chunk.start, chunk.end);

                // Send chunk metadata
                uploadState.socket.emit('message', JSON.stringify({
                    action: 'chunk_metadata',
                    upload_id: uploadState.uploadId,
                    part_number: chunk.partNumber,
                    is_last_chunk: chunk.isLastChunk,
                    chunk_size: chunk.size
                }));

                console.log(`Sent metadata for part ${chunk.partNumber}`);

                // Send binary data after a small delay
                setTimeout(() => {
                    if (!uploadState.isActive) return;

                    const reader = new FileReader();
                    reader.onload = function (e) {
                        try {
                            if (!uploadState.isActive) return;

                            const arrayBuffer = e.target.result;
                            console.log(`Sending binary data for part ${chunk.partNumber}, size: ${arrayBuffer.byteLength} bytes`);

                            uploadState.socket.emit('binary_chunk', arrayBuffer);
                            console.log(`Binary data sent for part ${chunk.partNumber}`);
                        } catch (error) {
                            console.error('Error sending binary chunk:', error);
                            uploadState.isProcessingChunk = false;

                            // Retry this chunk if possible
                            chunk.retryCount = (chunk.retryCount || 0) + 1;
                            if (chunk.retryCount <= 3) {
                                console.log(`Retrying chunk ${chunk.partNumber} (attempt ${chunk.retryCount})`);
                                uploadState.chunkQueue.unshift(chunk); // Put it back at the front
                                setTimeout(() => processNextChunk(), 2000);
                            } else {
                                handleUploadError(new Error(`Failed to send chunk ${chunk.partNumber} after 3 attempts`));
                            }
                        }
                    };

                    reader.onerror = function (error) {
                        console.error('Error reading file chunk:', error);
                        uploadState.isProcessingChunk = false;
                        handleUploadError(new Error(`Error reading file chunk: ${error.message || 'Unknown error'}`));
                    };

                    reader.readAsArrayBuffer(chunkBlob);
                }, 300); // 300ms delay between metadata and binary data

            } catch (error) {
                console.error('Error processing chunk:', error);
                uploadState.isProcessingChunk = false;
                handleUploadError(error);
            }
        };

    } catch (error) {
        console.error('Upload failed:', error);
        showStatus(`Upload failed: ${error.message}`, 'error');
        showRetryButton();
    }
};

// ---------------------------------------------------------------------------
// Remote import workflow helpers
// ---------------------------------------------------------------------------

const REMOTE_IMPORT_ENDPOINT = '/api/yt-dlp/jobs';
const REMOTE_IMPORT_STATUS_ENDPOINT = jobId => `/api/yt-dlp/jobs/${encodeURIComponent(jobId)}`;
const REMOTE_IMPORT_POLL_INTERVAL_MS = 3000;

function getRemoteImportActivityCard() {
    return document.getElementById('remoteImportActivity');
}

function getRemoteImportLogContainer() {
    return document.getElementById('remoteImportLog');
}

function ensureRemoteImportActivityVisible() {
    const card = getRemoteImportActivityCard();
    if (card && card.style.display === 'none') {
        card.style.display = 'block';
    }
}

function appendRemoteImportLog(message, level = 'info') {
    if (!message) {
        return;
    }

    const container = getRemoteImportLogContainer();
    if (!container) {
        return;
    }

    ensureRemoteImportActivityVisible();

    const entry = document.createElement('div');
    entry.className = `remote-import-log-entry remote-import-log-${level}`;

    const timestamp = document.createElement('span');
    timestamp.className = 'remote-import-log-time';
    timestamp.textContent = new Date().toLocaleTimeString();
    entry.appendChild(timestamp);

    const messageSpan = document.createElement('span');
    messageSpan.className = 'remote-import-log-message';
    messageSpan.textContent = message;
    entry.appendChild(messageSpan);

    container.appendChild(entry);
    container.setAttribute('data-has-entries', 'true');

    while (container.children.length > 200) {
        container.removeChild(container.firstChild);
    }

    container.scrollTop = container.scrollHeight;
}

const remoteImportState = {
    timerId: null,
    jobId: null,
    lastStatus: null,
    lastMessage: null,
    lastProgressText: null,
    lastFilesCompleted: 0,
    lastStage: null,
    isSubmitting: false,
    useSocket: false,
    socketConnected: false,
    socketSubscribedJobId: null,
    lastLoggedMessage: null,
    lastLoggedProgressText: null
};

function getRemoteImportElements() {
    return {
        form: document.getElementById('remoteImportForm'),
        sourceInput: document.getElementById('remoteSourceUrl'),
        destinationInput: document.getElementById('remoteDestinationFolder'),
        commandInput: document.getElementById('remoteAdvancedCommand'),
        errorAlert: document.getElementById('remoteImportFormErrors'),
        submitButton: document.getElementById('remoteImportSubmit')
    };
}

function resetRemoteImportValidation() {
    const { sourceInput, destinationInput, commandInput, errorAlert } = getRemoteImportElements();
    [sourceInput, destinationInput, commandInput].forEach(input => {
        if (input) {
            input.classList.remove('is-invalid');
            const feedback = input.parentElement ? input.parentElement.querySelector('.invalid-feedback') : null;
            if (feedback) {
                feedback.textContent = '';
            }
        }
    });

    if (errorAlert) {
        errorAlert.classList.add('d-none');
        errorAlert.textContent = '';
    }
}

function applyRemoteImportErrors(errorData) {
    const elements = getRemoteImportElements();
    const fieldErrors = (errorData && (errorData.field_errors || errorData.errors)) || {};
    const generalMessages = [];

    if (errorData && (errorData.message || errorData.detail || errorData.error)) {
        generalMessages.push(errorData.message || errorData.detail || errorData.error);
    }

    const applyFieldError = (input, message) => {
        if (!input) return;
        input.classList.add('is-invalid');
        const feedback = input.parentElement ? input.parentElement.querySelector('.invalid-feedback') : null;
        if (feedback) {
            feedback.textContent = Array.isArray(message) ? message.join(' ') : message;
        }
    };

    if (fieldErrors && typeof fieldErrors === 'object' && !Array.isArray(fieldErrors)) {
        Object.entries(fieldErrors).forEach(([key, message]) => {
            const normalizedKey = key.toLowerCase();
            if (normalizedKey.includes('source')) {
                applyFieldError(elements.sourceInput, message);
            } else if (normalizedKey.includes('dest')) {
                applyFieldError(elements.destinationInput, message);
            } else if (normalizedKey.includes('command')) {
                applyFieldError(elements.commandInput, message);
            } else if (message) {
                generalMessages.push(Array.isArray(message) ? message.join(' ') : message);
            }
        });
    } else if (Array.isArray(fieldErrors)) {
        generalMessages.push(fieldErrors.join(' '));
    }

    if (elements.errorAlert && generalMessages.length > 0) {
        elements.errorAlert.classList.remove('d-none');
        elements.errorAlert.textContent = generalMessages.join(' ');
    }

    if (generalMessages.length > 0) {
        showStatus(generalMessages.join(' '), 'error');
        appendRemoteImportLog(generalMessages.join(' '), 'error');
    }
}

function toggleRemoteImportFormDisabled(isDisabled) {
    const { sourceInput, destinationInput, commandInput, submitButton } = getRemoteImportElements();
    [sourceInput, destinationInput, commandInput, submitButton].forEach(input => {
        if (input) {
            input.disabled = isDisabled;
        }
    });
}

function resetRemoteImportForm() {
    const { form, destinationInput } = getRemoteImportElements();
    if (form) {
        form.reset();
    }
    if (destinationInput) {
        destinationInput.value = (window.currentFolder && window.currentFolder.trim()) ? window.currentFolder : '/';
    }
    resetRemoteImportValidation();
}

function closeRemoteImportModal() {
    const modalElement = document.getElementById('remoteImportModal');
    if (!modalElement) return;

    if (window.jQuery && window.jQuery(modalElement).modal) {
        window.jQuery(modalElement).modal('hide');
    } else {
        modalElement.classList.remove('show');
        modalElement.setAttribute('aria-hidden', 'true');
        modalElement.style.display = 'none';
        document.body.classList.remove('modal-open');
        const backdrop = document.querySelector('.modal-backdrop');
        if (backdrop) {
            backdrop.parentNode.removeChild(backdrop);
        }
    }
}

function stopRemoteImportTracking() {
    if (remoteImportState.timerId) {
        clearInterval(remoteImportState.timerId);
    }
    remoteImportState.timerId = null;
    remoteImportState.jobId = null;
    remoteImportState.lastStatus = null;
    remoteImportState.lastMessage = null;
    remoteImportState.lastProgressText = null;
    remoteImportState.lastFilesCompleted = 0;
    remoteImportState.lastStage = null;
    remoteImportState.useSocket = false;
    remoteImportState.socketSubscribedJobId = null;
    remoteImportState.lastLoggedMessage = null;
    remoteImportState.lastLoggedProgressText = null;
}

function processRemoteImportUpdate(payload) {
    if (!payload) {
        return;
    }

    const job = payload.job || payload;
    if (!job) {
        return;
    }

    if (remoteImportState.jobId && job.id && job.id !== remoteImportState.jobId) {
        return;
    }

    const progress = job.progress || {};
    const status = (job.status || '').toString().toLowerCase();
    const stage = (progress.stage || '').toString().toLowerCase();
    const terminalState = (job.terminal_state || '').toString().toLowerCase();

    const messageSources = [
        payload.error,
        payload.message,
        payload.detail,
        payload.status_message,
        job.error,
        job.message,
        job.detail,
        job.status_message,
        progress.status_message,
    ];
    let combinedMessage = messageSources.find(msg => typeof msg === 'string' && msg.trim().length > 0);

    if (!combinedMessage) {
        if (stage) {
            combinedMessage = `Remote import ${stage}`;
        } else if (status) {
            combinedMessage = `Remote import ${status}`;
        } else {
            combinedMessage = 'Remote import update';
        }
    }

    const currentFile = progress.current_file;
    if (currentFile) {
        if (stage === 'uploading') {
            combinedMessage = `Uploading ${currentFile}`;
        } else if (stage === 'deleting') {
            combinedMessage = `Finalizing ${currentFile}`;
        }
    }

    let progressValue = null;
    if (typeof progress.percentage === 'number') {
        progressValue = progress.percentage;
    } else if (typeof progress.progress === 'number') {
        progressValue = progress.progress;
    } else if (typeof payload.progress === 'number') {
        progressValue = payload.progress;
    } else if (typeof payload.percent === 'number') {
        progressValue = payload.percent;
    } else if (typeof payload.percentage === 'number') {
        progressValue = payload.percentage;
    }

    let progressText = null;
    if (progressValue !== null && !Number.isNaN(progressValue)) {
        let percent = progressValue;
        if (percent <= 1) {
            percent = percent * 100;
        }
        percent = Math.max(0, Math.min(100, Math.round(percent)));
        progressText = ` (${percent}% complete)`;
    }

    const statusTokens = [status, stage, terminalState].filter(Boolean);
    const terminalSuccessTokens = new Set(['success', 'completed', 'complete', 'done', 'finished']);
    const terminalFailureTokens = new Set(['failed', 'failure', 'error', 'cancelled', 'canceled', 'rejected']);
    const terminalSuccess = statusTokens.some(token => terminalSuccessTokens.has(token));
    const terminalFailure = statusTokens.some(token => terminalFailureTokens.has(token));

    const logLevel = terminalFailure ? 'error' : (terminalSuccess ? 'success' : 'info');
    const logMessage = progressText ? `${combinedMessage}${progressText}` : combinedMessage;
    if (
        logMessage && (
            logMessage !== remoteImportState.lastLoggedMessage ||
            progressText !== remoteImportState.lastLoggedProgressText
        )
    ) {
        appendRemoteImportLog(logMessage, logLevel);
        remoteImportState.lastLoggedMessage = logMessage;
        remoteImportState.lastLoggedProgressText = progressText;
    }

    const filesCompleted = typeof progress.files_completed === 'number' ? progress.files_completed : null;
    if (filesCompleted !== null) {
        if (filesCompleted > (remoteImportState.lastFilesCompleted || 0)) {
            remoteImportState.lastFilesCompleted = filesCompleted;
            if (typeof loadFiles === 'function') {
                try {
                    loadFiles();
                } catch (err) {
                    console.warn('loadFiles failed:', err);
                }
            }
        } else if (filesCompleted > remoteImportState.lastFilesCompleted) {
            remoteImportState.lastFilesCompleted = filesCompleted;
        }
    }

    if (stage && stage !== remoteImportState.lastStage) {
        remoteImportState.lastStage = stage;
    }

    if (terminalSuccess) {
        showStatus(progressText ? `${combinedMessage}${progressText}` : combinedMessage, 'success');
        stopRemoteImportTracking();
        if (typeof refreshServerCache === 'function') {
            try { refreshServerCache(); } catch (err) { console.warn('refreshServerCache failed:', err); }
        }
        if (typeof loadFiles === 'function') {
            setTimeout(() => {
                try { loadFiles(); } catch (error) { console.warn('loadFiles failed:', error); }
            }, 500);
        }
        return;
    }

    if (terminalFailure) {
        const failureMessage = progressText ? `${combinedMessage}${progressText}` : combinedMessage;
        showStatus(failureMessage || 'Remote import failed.', 'error');
        stopRemoteImportTracking();
        return;
    }

    if (
        status !== remoteImportState.lastStatus ||
        combinedMessage !== remoteImportState.lastMessage ||
        progressText !== remoteImportState.lastProgressText
    ) {
        showStatus(progressText ? `${combinedMessage}${progressText}` : combinedMessage, 'info');
        remoteImportState.lastStatus = status;
        remoteImportState.lastMessage = combinedMessage;
        remoteImportState.lastProgressText = progressText;
    }
}

async function pollRemoteImportStatus(jobId) {
    try {
        if (remoteImportState.useSocket && remoteImportState.socketConnected) {
            return;
        }
        const response = await fetch(REMOTE_IMPORT_STATUS_ENDPOINT(jobId), { credentials: 'include' });
        if (response.status === 404) {
            console.warn('Remote import status endpoint returned 404; will retry shortly.');
            return;
        }
        if (!response.ok) {
            const errorText = await response.text();
            throw new Error(errorText || `Status request failed with ${response.status}`);
        }
        const data = await response.json();
        processRemoteImportUpdate(data && (data.job || data));
    } catch (error) {
        console.error('Error polling remote import status:', error);
        showStatus(`Unable to retrieve import status: ${error.message}`, 'error');
        appendRemoteImportLog(`Unable to retrieve import status: ${error.message}`, 'error');
        stopRemoteImportTracking();
    }
}

function startRemoteImportTracking(jobId, initialPayload) {
    stopRemoteImportTracking();
    remoteImportState.jobId = jobId;
    remoteImportState.lastFilesCompleted = 0;
    remoteImportState.lastStage = null;
    remoteImportState.useSocket = remoteImportState.socketConnected;
    remoteImportState.socketSubscribedJobId = remoteImportState.useSocket ? jobId : null;
    appendRemoteImportLog(`Monitoring remote import job ${jobId}`, 'info');

    if (initialPayload) {
        processRemoteImportUpdate(initialPayload && (initialPayload.job || initialPayload));
    }

    if (!remoteImportState.useSocket) {
        pollRemoteImportStatus(jobId);
        remoteImportState.timerId = setInterval(() => {
            pollRemoteImportStatus(jobId);
        }, REMOTE_IMPORT_POLL_INTERVAL_MS);
    }
}

function handleRemoteImportSocketUpdate(eventPayload) {
    if (!eventPayload) {
        return;
    }
    const job = eventPayload.job || eventPayload;
    if (!job || !remoteImportState.jobId) {
        return;
    }
    if (job.id && job.id !== remoteImportState.jobId) {
        return;
    }

    if (job.id && remoteImportState.jobId === job.id) {
        remoteImportState.useSocket = true;
        remoteImportState.socketSubscribedJobId = job.id;
    }

    processRemoteImportUpdate(job);
}

function onRemoteImportSocketConnect() {
    remoteImportState.socketConnected = true;
    if (!remoteImportState.jobId) {
        return;
    }

    if (remoteImportState.timerId) {
        clearInterval(remoteImportState.timerId);
        remoteImportState.timerId = null;
    }

    remoteImportState.useSocket = true;
    remoteImportState.socketSubscribedJobId = remoteImportState.jobId;
}

function onRemoteImportSocketDisconnect() {
    remoteImportState.socketConnected = false;
    remoteImportState.socketSubscribedJobId = null;

    if (!remoteImportState.jobId) {
        return;
    }

    if (!remoteImportState.timerId) {
        remoteImportState.useSocket = false;
        pollRemoteImportStatus(remoteImportState.jobId);
        remoteImportState.timerId = setInterval(() => {
            pollRemoteImportStatus(remoteImportState.jobId);
        }, REMOTE_IMPORT_POLL_INTERVAL_MS);
    }
}

async function handleRemoteImportSubmit(event) {
    event.preventDefault();
    if (remoteImportState.isSubmitting) {
        return;
    }

    const elements = getRemoteImportElements();
    if (!elements.form) {
        return;
    }

    resetRemoteImportValidation();

    const sourceUrl = elements.sourceInput ? elements.sourceInput.value.trim() : '';
    const destinationFolder = elements.destinationInput ? elements.destinationInput.value.trim() : '';
    const advancedCommand = elements.commandInput ? elements.commandInput.value.trim() : '';

    if (!sourceUrl) {
        if (elements.sourceInput) {
            elements.sourceInput.classList.add('is-invalid');
            const feedback = elements.sourceInput.parentElement ? elements.sourceInput.parentElement.querySelector('.invalid-feedback') : null;
            if (feedback) {
                feedback.textContent = 'A source URL is required.';
            }
        }
        showStatus('Please provide a source URL to import.', 'error');
        appendRemoteImportLog('Remote import blocked: a source URL is required.', 'error');
        return;
    }

    const payload = {
        url: sourceUrl
    };

    const resolvedDestination = destinationFolder || (window.currentFolder && window.currentFolder.trim()) || '/';
    if (resolvedDestination) {
        payload.folder_path = resolvedDestination;
    }
    if (advancedCommand) {
        payload.command = advancedCommand;
    }

    remoteImportState.isSubmitting = true;
    toggleRemoteImportFormDisabled(true);
    showStatus('Submitting remote import request...', 'info');
    appendRemoteImportLog(`Submitting remote import request for ${sourceUrl} → ${resolvedDestination}`, 'info');
    remoteImportState.lastLoggedMessage = null;
    remoteImportState.lastLoggedProgressText = null;

    try {
        const response = await fetch(REMOTE_IMPORT_ENDPOINT, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            credentials: 'include',
            body: JSON.stringify(payload)
        });

        const contentType = response.headers.get('content-type') || '';
        const hasJson = contentType.includes('application/json');
        const data = hasJson ? await response.json() : null;

        if (!response.ok) {
            applyRemoteImportErrors(data || { message: 'The server rejected the import request.' });
            return;
        }

        const jobPayload = data && (data.job || data);
        const jobId = jobPayload && (jobPayload.job_id || jobPayload.jobId || jobPayload.id || jobPayload.identifier);
        if (!jobId) {
            showStatus('Import started but no job identifier was returned.', 'error');
            appendRemoteImportLog('Import started but no job identifier was returned.', 'error');
            return;
        }

        const confirmationMessage = (data && data.message) || (jobPayload && jobPayload.message) || 'Import request accepted. Monitoring progress...';
        showStatus(confirmationMessage, 'info');
        appendRemoteImportLog(`${confirmationMessage} Job ID: ${jobId}`, 'info');
        closeRemoteImportModal();
        resetRemoteImportForm();
        startRemoteImportTracking(jobId, jobPayload);
    } catch (error) {
        console.error('Remote import submission failed:', error);
        showStatus(`Failed to start remote import: ${error.message}`, 'error');
        appendRemoteImportLog(`Failed to start remote import: ${error.message}`, 'error');
    } finally {
        remoteImportState.isSubmitting = false;
        toggleRemoteImportFormDisabled(false);
    }
}

function initializeRemoteImportWorkflow() {
    const elements = getRemoteImportElements();
    if (!elements.form) {
        return;
    }

    resetRemoteImportForm();
    elements.form.addEventListener('submit', handleRemoteImportSubmit);

    const modalElement = document.getElementById('remoteImportModal');
    if (modalElement) {
        const onShow = () => {
            resetRemoteImportValidation();
            const { destinationInput } = getRemoteImportElements();
            if (destinationInput) {
                destinationInput.value = (window.currentFolder && window.currentFolder.trim()) ? window.currentFolder : '/';
            }
        };

        const onHidden = () => {
            remoteImportState.isSubmitting = false;
            toggleRemoteImportFormDisabled(false);
            resetRemoteImportValidation();
        };

        if (window.jQuery && window.jQuery(modalElement).on) {
            window.jQuery(modalElement).on('show.bs.modal', onShow);
            window.jQuery(modalElement).on('hidden.bs.modal', onHidden);
        } else {
            modalElement.addEventListener('show.bs.modal', onShow);
            modalElement.addEventListener('hidden.bs.modal', onHidden);
        }
    }
}

document.addEventListener('DOMContentLoaded', initializeRemoteImportWorkflow);

// Expose helpers for debugging/testing
window.__remoteImport = {
    startRemoteImportTracking,
    stopRemoteImportTracking,
    pollRemoteImportStatus,
    initializeRemoteImportWorkflow,
    handleRemoteImportSocketUpdate,
    onSocketConnect: onRemoteImportSocketConnect,
    onSocketDisconnect: onRemoteImportSocketDisconnect
};