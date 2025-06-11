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
        // Reload the page to refresh the file list
        window.location.reload();
    } catch (error) {
        console.error('Error loading files:', error);
    }
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

// Function to upload a file with chunking and proper tracking
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
        updateProgressBar(0, `Uploading: 0/${formatFileSize(fileSize)}`);
        
        // Update the initialization message to show we're now uploading
        showInitializingMessage(`Uploading file: ${fileName} (${formatFileSize(fileSize)})...`);
        
        // Step 2: Split file into chunks and upload each
        const chunkSize = 5 * 1024 * 1024; // 5MB chunks for Notion API
        
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
                uploaded: false,
                retryCount: 0,
                maxRetries: 3,
                blob: file.slice(start, end)
            });
        }
        
        // Upload chunks with max 10 concurrent uploads
        const maxConcurrent = 10;
        let activeUploads = 0;
        let completedParts = 0;
        let failedParts = 0;
        
        const uploadNextChunk = async () => {
            if (uploadQueue.length === 0) {
                return;
            }
            
            // Get next chunk to upload
            const chunk = uploadQueue.shift();
            activeUploads++;
            
            try {
                // Create form data for this chunk
                const formData = new FormData();
                formData.append('file', chunk.blob);
                formData.append('upload_id', uploadId);
                formData.append('part_number', chunk.partNumber);
                formData.append('total_size', fileSize);
                formData.append('filename', uploadData.sanitized_filename || fileName);
                formData.append('original_filename', fileName);
                formData.append('salt', salt);
                formData.append('is_last_chunk', chunk.isLastChunk ? 'true' : 'false');
                formData.append('is_multipart', uploadData.is_multipart ? 'true' : 'false');
                
                // Upload the chunk
                console.log(`Uploading part ${chunk.partNumber}/${totalParts} (${formatFileSize(chunk.blob.size)})`);
                const response = await fetch('/upload_file', {
                    method: 'POST',
                    body: formData
                });
                
                if (!response.ok) {
                    const errorData = await response.json();
                    throw new Error(`Part ${chunk.partNumber} upload failed: ${errorData.error || 'Unknown error'}`);
                }
                
                const responseData = await response.json();
                console.log(`Part ${chunk.partNumber} upload response:`, responseData);
                
                // Update progress
                completedParts++;
                const progress = Math.floor((completedParts / totalParts) * 100);
                const uploadedBytes = completedParts * chunkSize > fileSize ? fileSize : completedParts * chunkSize;
                updateProgressBar(progress, `Uploading: ${formatFileSize(uploadedBytes)}/${formatFileSize(fileSize)}`);
                
                // Mark chunk as uploaded
                chunk.uploaded = true;
                
            } catch (error) {
                console.error(`Error uploading part ${chunk.partNumber}:`, error);
                
                // Add back to queue if retries remaining
                chunk.retryCount++;
                if (chunk.retryCount <= chunk.maxRetries) {
                    console.log(`Retrying part ${chunk.partNumber} (attempt ${chunk.retryCount}/${chunk.maxRetries})`);
                    
                    // Add exponential backoff
                    const backoffTime = Math.pow(2, chunk.retryCount) * 1000;
                    await new Promise(resolve => setTimeout(resolve, backoffTime));
                    
                    // Add back to queue
                    uploadQueue.push(chunk);
                } else {
                    console.error(`Part ${chunk.partNumber} failed after ${chunk.maxRetries} attempts`);
                    failedParts++;
                    showStatus(`Upload failed, please try again`, 'error');
                }
            } finally {
                activeUploads--;
                
                // Start next upload if queue isn't empty and we're below max concurrent
                if (uploadQueue.length > 0 && activeUploads < maxConcurrent) {
                    uploadNextChunk();
                }
                
                // If all uploads finished (no active, no queue), finalize the upload
                if (activeUploads === 0 && uploadQueue.length === 0) {
                    if (failedParts === 0) {
                        console.log('All parts uploaded successfully, finalizing...');
                        // Show 100% without detailed text
                        updateProgressBar(100, '');
                        showFinalizingMessage('Preparing storage transfer...');
                        // Wait a moment to ensure all server-side processes are complete
                        setTimeout(() => finalizeUpload(uploadId, fileSize), 5000);
                    } else {
                        showStatus(`Upload failed, please try again`, 'error');
                    }
                }
            }
        };
        
        // Start initial batch of uploads
        const initialUploads = Math.min(maxConcurrent, uploadQueue.length);
        for (let i = 0; i < initialUploads; i++) {
            uploadNextChunk();
        }
        
    } catch (error) {
        console.error('Upload failed:', error);
        showStatus(`Upload failed: ${error.message}`, 'error');
    }
};

// Function to finalize the upload
const finalizeUpload = async (uploadId, fileSize) => {
    try {
        // Show 100% progress with empty text
        updateProgressBar(100, '');
        showFinalizingMessage('Transferring to storage server...');
        
        const response = await fetch('/finalize_upload', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ upload_id: uploadId })
        });
        
        if (!response.ok) {
            const errorData = await response.json();
            console.error('Finalization error:', errorData);
            
            // If the error indicates missing parts, try again after a delay
            if (errorData.error && errorData.error.includes('missing parts')) {
                showFinalizingMessage('Still transferring, please wait...');
                // Keep progress at 100% with empty text
                updateProgressBar(100, '');
                // Wait 5 seconds and try again
                setTimeout(() => finalizeUpload(uploadId, fileSize), 5000);
                return;
            }
            
            throw new Error(`Upload finalization failed: ${errorData.error || 'Unknown error'}`);
        }
        
        const finalizeData = await response.json();
        console.log('Upload finalized:', finalizeData);
        
        // Update UI to show completion with empty progress text
        updateProgressBar(100, '');
        showStatus(`Upload completed successfully. File ID: ${finalizeData.file_id}`, 'success');
        
        // Refresh file list
        loadFiles();
        
    } catch (error) {
        console.error('Finalization failed:', error);
        showStatus(`Finalization failed: ${error.message}`, 'error');
    }
}; 