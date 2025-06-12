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
    
    // Clear status messages
    const messageContainer = document.getElementById('messageContainer');
    if (messageContainer) {
        messageContainer.innerHTML = '';
    }
    
    // Reset persistent spinning icon
    persistentSpinningIcon = null;
}

// Function to load files (refresh file list)
async function loadFiles() {
    try {
        console.log('Refreshing file list with AJAX...');
        // Fetch the file list data from the API
        const response = await fetch('/files-api');
        if (!response.ok) {
            throw new Error('Failed to fetch file list');
        }
        
        const data = await response.json();
        if (!data.files) {
            throw new Error('Invalid response format');
        }
        
        // Get the files container to update
        const filesContainer = document.getElementById('files-container');
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
        
        // Generate the table HTML
        let tableHTML = `
            <div class="table-responsive">
                <table class="table" id="fileTable">
                    <thead>
                        <tr>
                            <th><i class="fas fa-file mr-1"></i> Filename</th>
                            <th><i class="fas fa-weight mr-1"></i> Size</th>
                            <th><i class="fas fa-link mr-1"></i> Public Link</th>
                            <th><i class="fas fa-lock-open mr-1"></i> Public Access</th>
                            <th><i class="fas fa-cogs mr-1"></i> Actions</th>
                        </tr>
                    </thead>
                    <tbody>
        `;
        
        // Generate rows for each file
        data.files.forEach(file => {
            const fileId = file.id || '';
            const fileHash = file.file_hash || '';
            const isPublic = file.is_public || false;
            
            tableHTML += `
                <tr data-file-id="${fileId}" data-file-hash="${fileHash}">
                    <td><strong>${file.name}</strong></td>
                    <td class="filesize-cell">${formatFileSize(file.size)}</td>
                    <td>
                        ${fileHash ? 
                            `<a href="/d/${fileHash}" target="_blank" class="public-link">
                                <i class="fas fa-external-link-alt mr-1"></i>${window.location.origin}/d/${fileHash.substring(0, 10)}...
                            </a>` : 
                            '<span class="text-muted">N/A</span>'
                        }
                    </td>
                    <td>
                        <label class="switch">
                            <input type="checkbox" class="public-toggle" ${isPublic ? 'checked' : ''}>
                            <span class="slider round"></span>
                        </label>
                    </td>
                    <td class="action-buttons">
                        <a href="/d/${fileHash}" class="btn btn-primary btn-sm">
                            <i class="fas fa-download mr-1"></i>Download
                        </a>
                        <button class="btn btn-danger btn-sm delete-btn" data-file-id="${fileId}">
                            <i class="fas fa-trash-alt mr-1"></i>Delete
                        </button>
                    </td>
                </tr>
            `;
        });
        
        // Close the table
        tableHTML += `
                    </tbody>
                </table>
            </div>
        `;
        
        // Update the container
        filesContainer.innerHTML = tableHTML;
        
        // Add event listeners to the buttons
        setupFileActionEventHandlers();
        
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
        button.addEventListener('click', async function() {
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
        toggle.addEventListener('change', async function() {
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
        const uploadId = uploadData.id;
        const salt = uploadData.salt;
        const totalParts = uploadData.total_parts || 1;
        
        console.log(`Upload initialized with ID: ${uploadId}, ${totalParts} parts`);
        updateProgressBar(0, `Preparing upload: ${formatFileSize(0)}/${formatFileSize(fileSize)}`);
        
        // Update the initialization message to show we're now uploading
        showInitializingMessage(`Uploading file: ${fileName} (${formatFileSize(fileSize)})...`);
        
        // Step 2: Create Socket.IO connection for chunk uploads
        const socketConfig = {
            ...window.socketIOConfig,
            query: { upload_id: uploadId }
        };
        
        console.log('Creating Socket.IO connection with config:', socketConfig);
        // Force the binary type to be arraybuffer
        const socket = io('/ws/upload', socketConfig);
        // Ensure binary data is sent correctly
        socket.binaryType = 'arraybuffer';
        
        // Determine if this is a large file (over 1GB)
        const isLargeFile = fileSize > 1024 * 1024 * 1024;
        
        // Chunk size and tracking variables
        const chunkSize = 5 * 1024 * 1024; // 5MB chunks for Notion API
        let completedParts = 0;
        let uploadedBytes = 0;
        let isUploading = false;
        let retryCount = 0;
        let binaryReceived = false;
        const maxRetries = 3;
        let waitTimeBeforeNextChunk = isLargeFile ? 500 : 100; // ms
        
        // Create upload queue
        const uploadQueue = [];
        for (let partNumber = 1; partNumber <= totalParts; partNumber++) {
            const start = (partNumber - 1) * chunkSize;
            const end = Math.min(start + chunkSize, fileSize);
            const isLastChunk = partNumber === totalParts;
            
            uploadQueue.push({
                partNumber,
                start,
                end,
                isLastChunk,
                size: end - start
            });
        }
        
        // Process Socket.IO messages from server
        socket.on('message', (data) => {
            try {
                const response = JSON.parse(data);
                console.log('Socket.IO message:', response);
                
                if (response.status === 'ready_for_chunk') {
                    // Server is ready for the next chunk
                    retryCount = 0; // Reset retry count on successful message
                    
                    // For very large files, add a small delay to avoid memory pressure
                    if (isLargeFile) {
                        setTimeout(sendNextChunk, waitTimeBeforeNextChunk);
                    } else {
                        sendNextChunk();
                    }
                } 
                else if (response.status === 'binary_received') {
                    // Binary data was received by the server but still being processed
                    console.log(`Binary data for part ${response.part_number} received by server`);
                    binaryReceived = true;
                }
                else if (response.status === 'chunk_received') {
                    // Chunk successfully received and processed by server
                    const partNumber = response.part_number;
                    console.log(`Part ${partNumber}/${totalParts} upload confirmed`);
                    
                    // Update progress
                    completedParts++;
                    const progress = Math.floor((completedParts / totalParts) * 100);
                    uploadedBytes += response.chunk_size || 0;
                    updateProgressBar(progress, `Uploading: ${formatFileSize(uploadedBytes)}/${formatFileSize(fileSize)}`);
                    
                    // Mark current chunk as uploaded
                    isUploading = false;
                    binaryReceived = false;
                    retryCount = 0; // Reset retry count on successful upload
                    
                    // If we have more chunks, tell server we're ready to send next
                    if (uploadQueue.length > 0) {
                        // Add a small delay for large files to prevent memory pressure
                        setTimeout(() => {
                            socket.emit('message', JSON.stringify({ 
                                action: 'ready_for_next_chunk',
                                upload_id: uploadId
                            }));
                        }, isLargeFile ? waitTimeBeforeNextChunk : 0);
                    } else {
                        // All chunks uploaded, request finalization
                        socket.emit('message', JSON.stringify({ 
                            action: 'finalize',
                            upload_id: uploadId,
                            total_parts: totalParts
                        }));
                        
                        // Show finalizing message
                        updateProgressBar(100, '');
                        showFinalizingMessage('Transferring to storage server...');
                    }
                }
                else if (response.status === 'finalized') {
                    // Upload successfully finalized
                    console.log('Upload finalized:', response);
                    updateProgressBar(100, '');
                    showStatus(`Upload completed successfully. File ID: ${response.file_id}`, 'success');
                    
                    // Close the socket
                    socket.disconnect();
                    
                    // Refresh file list
                    loadFiles();
                }
                else if (response.status === 'error') {
                    throw new Error(response.message || 'Unknown WebSocket error');
                }
            } catch (error) {
                console.error('Error processing Socket.IO message:', error);
                showStatus(`Upload error: ${error.message}`, 'error');
                socket.disconnect();
            }
        });
        
        // Handle Socket.IO events
        socket.on('connect', () => {
            console.log('Socket.IO connection established');
            // Tell server we're ready to start uploading
            socket.emit('message', JSON.stringify({ 
                action: 'start_upload',
                upload_id: uploadId,
                filename: uploadData.sanitized_filename || fileName,
                original_filename: fileName,
                total_size: fileSize,
                total_parts: totalParts,
                salt: salt
            }));
        });
        
        socket.on('connect_error', (error) => {
            console.error('Socket.IO connection error:', error);
            showStatus('Connection error: ' + error.message, 'error');
        });
        
        socket.on('disconnect', (reason) => {
            console.log('Socket.IO disconnected:', reason);
            if (completedParts < totalParts && reason !== 'io client disconnect') {
                showStatus('Connection closed unexpectedly. Please try again.', 'error');
            }
        });
        
        // Add an error event handler
        socket.on('error', (error) => {
            console.error('Socket.IO error:', error);
            showStatus('Socket error: ' + (error.message || 'Unknown error'), 'error');
        });
        
        // Function to send the next chunk in the queue
        const sendNextChunk = () => {
            if (isUploading || uploadQueue.length === 0) return;
            
            isUploading = true;
            const chunk = uploadQueue.shift();
            console.log(`Sending part ${chunk.partNumber}/${totalParts} (${formatFileSize(chunk.size)})`);
            
            // Extract blob for this chunk
            const chunkBlob = file.slice(chunk.start, chunk.end);
            
            // First send chunk metadata
            socket.emit('message', JSON.stringify({
                action: 'chunk_metadata',
                upload_id: uploadId,
                part_number: chunk.partNumber,
                is_last_chunk: chunk.isLastChunk,
                chunk_size: chunk.size
            }));
            
            // Use FileReader to read as ArrayBuffer
            const reader = new FileReader();
            reader.onload = function(e) {
                try {
                    const arrayBuffer = e.target.result;
                    console.log(`Ready to send binary data for part ${chunk.partNumber}, size: ${arrayBuffer.byteLength} bytes`);
                    
                    // For large chunks, ensure successful transmission by adding a small delay
                    const sendDelay = Math.min(chunk.size / (1024 * 1024), 2) * 100; // 100ms per MB, max 200ms
                    
                    // Send the binary data with a delay to ensure metadata is processed first
                    setTimeout(() => {
                        try {
                            // Send raw ArrayBuffer for better compatibility
                            socket.emit('binary_chunk', arrayBuffer);
                            console.log(`Binary data sent for part ${chunk.partNumber}`);
                        } catch (error) {
                            console.error('Error sending binary chunk:', error);
                            isUploading = false;
                            showStatus(`Error sending chunk: ${error.message}`, 'error');
                        }
                    }, sendDelay);
                } catch (error) {
                    console.error('Error processing chunk data:', error);
                    isUploading = false;
                    showStatus(`Error processing chunk: ${error.message}`, 'error');
                }
            };
            
            reader.onerror = function(error) {
                console.error('Error reading file chunk:', error);
                isUploading = false;
                showStatus(`Error reading file chunk: ${error.message || 'Unknown error'}`, 'error');
            };
            
            // Read the blob as ArrayBuffer
            reader.readAsArrayBuffer(chunkBlob);
        };
        
    } catch (error) {
        console.error('Upload failed:', error);
        showStatus(`Upload failed: ${error.message}`, 'error');
    }
}; 