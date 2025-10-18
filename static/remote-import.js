/**
 * Remote import workflow helpers
 * Extracted from legacy upload.js to coexist with streaming uploader
 */

const REMOTE_IMPORT_ENDPOINT = '/api/upload/import';
const REMOTE_IMPORT_STATUS_ENDPOINT = jobId => `/api/upload/import/${encodeURIComponent(jobId)}`;
const REMOTE_IMPORT_POLL_INTERVAL_MS = 3000;

const remoteImportState = {
    timerId: null,
    jobId: null,
    lastStatus: null,
    lastMessage: null,
    lastProgressText: null,
    isSubmitting: false
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
}

function processRemoteImportUpdate(data) {
    if (!data) {
        return;
    }

    const status = (data.status || '').toString().toLowerCase();
    const message = data.message || data.detail || data.status_message || '';
    let progressValue = null;

    if (typeof data.progress === 'number') {
        progressValue = data.progress;
    } else if (typeof data.percent === 'number') {
        progressValue = data.percent;
    } else if (typeof data.percentage === 'number') {
        progressValue = data.percentage;
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

    const combinedMessage = message || (status ? `Remote import ${status}` : 'Remote import update');
    const terminalSuccess = ['success', 'completed', 'complete', 'done', 'finished'].includes(status);
    const terminalFailure = ['failed', 'error', 'cancelled', 'canceled', 'rejected'].includes(status);

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
        const failureMessage = combinedMessage || 'Remote import failed.';
        showStatus(failureMessage, 'error');
        stopRemoteImportTracking();
        return;
    }

    if (status !== remoteImportState.lastStatus || combinedMessage !== remoteImportState.lastMessage || progressText !== remoteImportState.lastProgressText) {
        showStatus(progressText ? `${combinedMessage}${progressText}` : combinedMessage, 'info');
        remoteImportState.lastStatus = status;
        remoteImportState.lastMessage = combinedMessage;
        remoteImportState.lastProgressText = progressText;
    }
}

async function pollRemoteImportStatus(jobId) {
    try {
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
        processRemoteImportUpdate(data);
    } catch (error) {
        console.error('Error polling remote import status:', error);
        showStatus(`Unable to retrieve import status: ${error.message}`, 'error');
        stopRemoteImportTracking();
    }
}

function startRemoteImportTracking(jobId, initialPayload) {
    stopRemoteImportTracking();
    remoteImportState.jobId = jobId;

    if (initialPayload) {
        processRemoteImportUpdate(initialPayload);
    }

    pollRemoteImportStatus(jobId);
    remoteImportState.timerId = setInterval(() => {
        pollRemoteImportStatus(jobId);
    }, REMOTE_IMPORT_POLL_INTERVAL_MS);
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
        return;
    }

    const payload = {
        source_url: sourceUrl
    };

    const resolvedDestination = destinationFolder || (window.currentFolder && window.currentFolder.trim()) || '/';
    if (resolvedDestination) {
        payload.destination_folder = resolvedDestination;
    }
    if (advancedCommand) {
        payload.advanced_command = advancedCommand;
    }

    remoteImportState.isSubmitting = true;
    toggleRemoteImportFormDisabled(true);
    showStatus('Submitting remote import request...', 'info');

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

        const jobId = data && (data.job_id || data.jobId || data.id);
        if (!jobId) {
            showStatus('Import started but no job identifier was returned.', 'error');
            return;
        }

        showStatus(data && data.message ? data.message : 'Import request accepted. Monitoring progress...', 'info');
        closeRemoteImportModal();
        resetRemoteImportForm();
        startRemoteImportTracking(jobId, data);
    } catch (error) {
        console.error('Remote import submission failed:', error);
        showStatus(`Failed to start remote import: ${error.message}`, 'error');
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
    initializeRemoteImportWorkflow
};
