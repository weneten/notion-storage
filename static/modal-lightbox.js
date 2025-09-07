/**
 * Modal Lightbox System for Media Viewing
 * Provides a complete lightbox experience for videos, images, and audio
 * with iOS Safari optimization and accessibility features
 */

class MediaModalLightbox {
    constructor() {
        this.modal = null;
        this.modalContainer = null;
        this.modalClose = null;
        this.modalTitle = null;
        this.modalContent = null;
        this.modalLoading = null;
        this.modalMediaContainer = null;
        this.modalPdfError = null;
        this.modalFilename = null;
        this.modalFilesize = null;
        this.modalFiletype = null;
        this.modalNavigation = null;
        this.modalPrev = null;
        this.modalNext = null;
        
        this.currentMediaList = [];
        this.currentMediaIndex = -1;
        this.isZoomed = false;
        
        this.init();
    }
    
    init() {
        this.modal = document.getElementById('mediaModal');
        this.modalContainer = this.modal?.querySelector('.modal-container');
        this.modalClose = document.getElementById('modalClose');
        this.modalTitle = document.getElementById('modalTitle');
        this.modalContent = document.getElementById('modalContent');
        this.modalLoading = document.getElementById('modalLoading');
        this.modalMediaContainer = document.getElementById('modalMediaContainer');
        this.modalPdfError = document.getElementById('modalPdfError');
        this.modalFilename = document.getElementById('modalFilename');
        this.modalFilesize = document.getElementById('modalFilesize');
        this.modalFiletype = document.getElementById('modalFiletype');
        this.modalNavigation = document.getElementById('modalNavigation');
        this.modalPrev = document.getElementById('modalPrev');
        this.modalNext = document.getElementById('modalNext');
        
        this.setupEventListeners();
        this.setupKeyboardNavigation();
        this.setupTouchGestures();
    }
    
    setupEventListeners() {
        // Close modal events
        if (this.modalClose) {
            this.modalClose.addEventListener('click', () => this.closeModal());
        }
        
        // Click outside to close
        if (this.modal) {
            this.modal.addEventListener('click', (e) => {
                if (e.target === this.modal) {
                    this.closeModal();
                }
            });
        }
        
        // Navigation events
        if (this.modalPrev) {
            this.modalPrev.addEventListener('click', () => this.showPrevious());
        }
        
        if (this.modalNext) {
            this.modalNext.addEventListener('click', () => this.showNext());
        }
        
        // Prevent modal container clicks from closing modal
        if (this.modalContainer) {
            this.modalContainer.addEventListener('click', (e) => {
                e.stopPropagation();
            });
        }
    }
    
    setupKeyboardNavigation() {
        document.addEventListener('keydown', (e) => {
            if (!this.isModalOpen()) return;
            
            switch (e.key) {
                case 'Escape':
                    e.preventDefault();
                    this.closeModal();
                    break;
                case 'ArrowLeft':
                    e.preventDefault();
                    this.showPrevious();
                    break;
                case 'ArrowRight':
                    e.preventDefault();
                    this.showNext();
                    break;
                case ' ':
                    e.preventDefault();
                    this.togglePlayPause();
                    break;
                case 'f':
                case 'F':
                    e.preventDefault();
                    this.toggleFullscreen();
                    break;
            }
        });
    }
    
    setupTouchGestures() {
        let startX = 0;
        let startY = 0;
        let isSwipeGesture = false;
        
        if (this.modalContainer) {
            this.modalContainer.addEventListener('touchstart', (e) => {
                if (e.touches.length === 1) {
                    startX = e.touches[0].clientX;
                    startY = e.touches[0].clientY;
                    isSwipeGesture = true;
                }
            }, { passive: true });
            
            this.modalContainer.addEventListener('touchmove', (e) => {
                if (!isSwipeGesture || e.touches.length !== 1) return;
                
                const deltaX = Math.abs(e.touches[0].clientX - startX);
                const deltaY = Math.abs(e.touches[0].clientY - startY);
                
                // If vertical movement is greater than horizontal, not a swipe
                if (deltaY > deltaX) {
                    isSwipeGesture = false;
                }
            }, { passive: true });
            
            this.modalContainer.addEventListener('touchend', (e) => {
                if (!isSwipeGesture) return;
                
                const endX = e.changedTouches[0].clientX;
                const deltaX = endX - startX;
                const threshold = 50; // minimum swipe distance
                
                if (Math.abs(deltaX) > threshold) {
                    if (deltaX > 0) {
                        this.showPrevious(); // Swipe right = previous
                    } else {
                        this.showNext(); // Swipe left = next
                    }
                }
                
                isSwipeGesture = false;
            }, { passive: true });
        }
    }
    
    /**
     * Open modal with media content
     */
    async openModal(mediaUrl, filename, filesize = '', filetype = '', mediaList = [], currentIndex = 0) {
        if (!this.modal) return;
        
        // Store media list for navigation
        this.currentMediaList = mediaList;
        this.currentMediaIndex = currentIndex;
        
        // Prevent body scrolling
        document.body.style.overflow = 'hidden';
        
        // Show modal immediately with loading state
        this.modal.style.display = 'flex';
        this.modal.setAttribute('aria-hidden', 'false');
        
        // Show loading state first with filename for context
        this.showLoading(filename);
        
        // Show modal with animation immediately
        setTimeout(() => {
            if (this.modal) {
                this.modal.classList.add('show');
            }
        }, 50);
        
        // Update modal info
        if (this.modalFilename) this.modalFilename.textContent = filename;
        if (this.modalFilesize) this.modalFilesize.textContent = filesize ? `Size: ${filesize}` : '';
        if (this.modalFiletype) this.modalFiletype.textContent = filetype ? `Type: ${filetype}` : '';
        
        // Update navigation visibility
        this.updateNavigation();
        
        // Focus management for accessibility
        if (this.modalClose) {
            this.modalClose.focus();
        }
        
        // Load media content asynchronously after modal is shown
        try {
            await this.loadMediaContent(mediaUrl, filename);
        } catch (error) {
            console.error('Error loading media content:', error);
            this.showError('Failed to load media content');
        }
    }
    
    /**
     * Close modal
     */
    closeModal() {
        if (!this.modal) return;
        
        // Hide modal with animation
        this.modal.classList.remove('show');
        
        // Reset zoom state
        this.isZoomed = false;
        
        // Stop any playing media
        this.stopMedia();
        
        setTimeout(() => {
            if (this.modal) {
                this.modal.style.display = 'none';
                this.modal.setAttribute('aria-hidden', 'true');
                
                // Clear modal content
                if (this.modalMediaContainer) {
                    this.modalMediaContainer.innerHTML = '';
                    this.modalMediaContainer.classList.remove('show');
                }
                
                // Hide PDF error if visible
                if (this.modalPdfError) {
                    this.modalPdfError.style.display = 'none';
                }
                
                // Re-enable body scrolling
                document.body.style.overflow = '';
                
                // Clear media list
                this.currentMediaList = [];
                this.currentMediaIndex = -1;
            }
        }, 300);
    }
    
    /**
     * Load media content based on file type
     */
    async loadMediaContent(mediaUrl, filename) {
        console.log('=== DEBUG: loadMediaContent called for:', filename, 'URL:', mediaUrl);
        
        const fileType = this.getFileType(filename);
        const mediaContainer = this.modalMediaContainer;
        
        if (!mediaContainer) {
            console.error('=== DEBUG: mediaContainer not found!');
            return;
        }
        
        // Check if there's existing content before clearing
        const existingContent = mediaContainer.innerHTML;
        if (existingContent.trim() !== '') {
            console.log('=== DEBUG: Clearing existing content from mediaContainer. Content length:', existingContent.length);
        }
        
        // Clear previous content
        mediaContainer.innerHTML = '';
        console.log('=== DEBUG: mediaContainer cleared, innerHTML length now:', mediaContainer.innerHTML.length);
        
        // Update modal title
        if (this.modalTitle) {
            this.modalTitle.textContent = `${this.getFileTypeDisplayName(fileType)} Viewer`;
        }
        
        let mediaElement;
        
        switch (fileType) {
            case 'video':
                mediaElement = await this.createVideoElement(mediaUrl, filename);
                break;
            case 'image':
                mediaElement = await this.createImageElement(mediaUrl, filename);
                break;
            case 'audio':
                mediaElement = await this.createAudioElement(mediaUrl, filename);
                break;
            case 'pdf':
                mediaElement = await this.createPdfElement(mediaUrl, filename);
                break;
            default:
                throw new Error(`Unsupported file type: ${fileType}`);
        }
        
        if (mediaElement) {
            console.log('=== DEBUG: Appending media element to container. Element type:', mediaElement.tagName);
            console.log('=== DEBUG: mediaContainer children count before append:', mediaContainer.children.length);
            
            // Double-check container is really empty before appending
            if (mediaContainer.children.length > 0) {
                console.warn('=== DEBUG: WARNING - Container not empty before append, force clearing!');
                mediaContainer.innerHTML = '';
            }
            
            mediaContainer.appendChild(mediaElement);
            
            console.log('=== DEBUG: mediaContainer children count after append:', mediaContainer.children.length);
            console.log('=== DEBUG: All children in container:', Array.from(mediaContainer.children).map(child => child.tagName));
            
            // Additional verification that only one video exists
            const videoElements = mediaContainer.querySelectorAll('video');
            if (videoElements.length > 1) {
                console.error('=== DEBUG: CRITICAL - Multiple video elements detected after append!', videoElements.length);
                // Keep only the last (newest) video element
                for (let i = 0; i < videoElements.length - 1; i++) {
                    console.log('=== DEBUG: Removing duplicate video element', i);
                    videoElements[i].remove();
                }
            }
            
            // Smooth transition from loading to content
            setTimeout(() => {
                this.hideLoading();
                setTimeout(() => {
                    mediaContainer.classList.add('show');
                }, 150);
            }, 100);
        }
    }
    
    /**
     * Create video element with iOS Safari optimizations
     */
    async createVideoElement(videoUrl, filename) {
        console.log('=== DEBUG: Creating video element for:', filename, 'URL:', videoUrl);
        
        return new Promise((resolve, reject) => {
            const video = document.createElement('video');
            console.log('=== DEBUG: Video element created with src:', videoUrl);
            video.className = 'modal-video';
            video.controls = true;
            video.preload = 'metadata';
            video.setAttribute('playsinline', 'true');
            video.setAttribute('webkit-playsinline', 'true');
            video.crossOrigin = 'anonymous';
            
            // iOS Safari specific optimizations
            if (window.deviceInfo?.isIOSSafari) {
                video.muted = true; // Start muted for iOS autoplay policies
                video.setAttribute('muted', 'true');
            }
            
            video.addEventListener('loadedmetadata', () => {
                console.log('=== DEBUG: Video metadata loaded for:', filename);
                console.log('=== DEBUG: Resolving video element creation');
                
                // Mark this video as modal-managed to prevent external interference
                video.setAttribute('data-modal-managed', 'true');
                
                resolve(video);
            });
            
            video.addEventListener('error', (e) => {
                console.error('Video load error:', e);
                reject(new Error('Failed to load video'));
            });
            
            video.addEventListener('canplay', () => {
                console.log('Video ready to play:', filename);
            });
            
            // Set video source
            video.src = videoUrl;
            
            // Timeout fallback
            setTimeout(() => {
                if (video.readyState === 0) {
                    reject(new Error('Video load timeout'));
                }
            }, 10000);
        });
    }
    
    /**
     * Create image element with zoom functionality
     */
    async createImageElement(imageUrl, filename) {
        return new Promise((resolve, reject) => {
            const img = document.createElement('img');
            img.className = 'modal-image';
            img.alt = filename;
            
            // Add zoom functionality
            img.addEventListener('click', () => {
                this.toggleImageZoom(img);
            });
            
            img.addEventListener('load', () => {
                console.log('Image loaded:', filename);
                resolve(img);
            });
            
            img.addEventListener('error', (e) => {
                console.error('Image load error:', e);
                reject(new Error('Failed to load image'));
            });
            
            // Set image source
            img.src = imageUrl;
            
            // Timeout fallback
            setTimeout(() => {
                if (!img.complete) {
                    reject(new Error('Image load timeout'));
                }
            }, 10000);
        });
    }
    
    /**
     * Create audio element
     */
    async createAudioElement(audioUrl, filename) {
        return new Promise((resolve, reject) => {
            const audio = document.createElement('audio');
            audio.className = 'modal-audio';
            audio.controls = true;
            audio.preload = 'metadata';
            audio.crossOrigin = 'anonymous';
            
            audio.addEventListener('loadedmetadata', () => {
                console.log('Audio metadata loaded:', filename);
                resolve(audio);
            });
            
            audio.addEventListener('error', (e) => {
                console.error('Audio load error:', e);
                reject(new Error('Failed to load audio'));
            });
            
            // Set audio source
            audio.src = audioUrl;
            
            // Timeout fallback
            setTimeout(() => {
                if (audio.readyState === 0) {
                    reject(new Error('Audio load timeout'));
                }
            }, 10000);
        });
    }
    
    /**
     * Create PDF element with iframe and fallback options
     */
    async createPdfElement(pdfUrl, filename) {
        return new Promise((resolve, reject) => {
            const pdfContainer = document.createElement('div');
            pdfContainer.className = 'modal-pdf-container';
            
            // Check if device/browser supports PDF viewing
            const isMobile = window.deviceInfo?.isMobile || /Android|webOS|iPhone|iPad|iPod|BlackBerry|IEMobile|Opera Mini/i.test(navigator.userAgent);
            const isIOS = window.deviceInfo?.isIOS || /iPad|iPhone|iPod/.test(navigator.userAgent);
            
            // Create iframe for PDF viewing
            const iframe = document.createElement('iframe');
            iframe.className = 'modal-pdf-iframe';
            iframe.src = pdfUrl;
            iframe.title = `PDF Viewer - ${filename}`;
            iframe.setAttribute('loading', 'lazy');
            
            // Create fallback download button
            const fallbackContainer = document.createElement('div');
            fallbackContainer.className = 'modal-pdf-fallback';
            fallbackContainer.style.display = 'none';
            fallbackContainer.innerHTML = `
                <div class="pdf-fallback-content">
                    <i class="fas fa-file-pdf" style="font-size: 3em; color: #cf6679; margin-bottom: 15px;"></i>
                    <h4>PDF Preview Not Available</h4>
                    <p>This PDF cannot be displayed in your browser.</p>
                    <a href="${pdfUrl}" target="_blank" class="btn btn-primary">
                        <i class="fas fa-download mr-2"></i>Download PDF
                    </a>
                    <a href="${pdfUrl}" target="_blank" class="btn btn-success ml-2">
                        <i class="fas fa-external-link-alt mr-2"></i>Open in New Tab
                    </a>
                </div>
            `;
            
            // Always show download option for mobile devices
            const downloadBar = document.createElement('div');
            downloadBar.className = 'modal-pdf-download-bar';
            downloadBar.innerHTML = `
                <div class="pdf-controls">
                    <a href="${pdfUrl}" target="_blank" class="btn btn-sm btn-primary">
                        <i class="fas fa-download mr-1"></i>Download
                    </a>
                    <a href="${pdfUrl}" target="_blank" class="btn btn-sm btn-success">
                        <i class="fas fa-external-link-alt mr-1"></i>Open in New Tab
                    </a>
                </div>
            `;
            
            // Handle iframe load events
            iframe.onload = () => {
                console.log('PDF loaded successfully:', filename);
                resolve(pdfContainer);
            };
            
            iframe.onerror = () => {
                console.error('PDF load error:', filename);
                // Show fallback on error
                iframe.style.display = 'none';
                fallbackContainer.style.display = 'flex';
                resolve(pdfContainer);
            };
            
            // Handle cases where iframe doesn't support PDF
            iframe.onabort = () => {
                console.warn('PDF load aborted:', filename);
                iframe.style.display = 'none';
                fallbackContainer.style.display = 'flex';
                resolve(pdfContainer);
            };
            
            // For mobile devices, show a more limited experience
            if (isMobile || isIOS) {
                // iOS and many mobile browsers have limited PDF support
                // Show both iframe (if supported) and download options
                iframe.style.height = '60vh';
                downloadBar.style.display = 'block';
                
                // Add mobile-specific styling
                pdfContainer.innerHTML = `
                    <div class="mobile-pdf-notice">
                        <i class="fas fa-info-circle mr-2"></i>
                        <span>PDF viewing may be limited on mobile devices.</span>
                    </div>
                `;
            }
            
            // Assemble the PDF container
            pdfContainer.appendChild(downloadBar);
            pdfContainer.appendChild(iframe);
            pdfContainer.appendChild(fallbackContainer);
            
            // Set timeout for loading
            setTimeout(() => {
                if (iframe.contentDocument === null) {
                    // Iframe didn't load, show fallback
                    iframe.style.display = 'none';
                    fallbackContainer.style.display = 'flex';
                    resolve(pdfContainer);
                }
            }, 5000);
        });
    }
    
    /**
     * Toggle image zoom
     */
    toggleImageZoom(img) {
        if (this.isZoomed) {
            img.classList.remove('zoomed');
            this.isZoomed = false;
        } else {
            img.classList.add('zoomed');
            this.isZoomed = true;
        }
    }
    
    /**
     * Show/hide loading state
     */
    showLoading(filename = '') {
        if (this.modalLoading) {
            this.modalLoading.style.display = 'flex';
            
            // Update loading text based on file type
            const loadingText = this.modalLoading.querySelector('.modal-loading-text');
            const loadingSubtext = this.modalLoading.querySelector('.modal-loading-subtext');
            
            if (filename) {
                const fileType = this.getFileType(filename);
                const messages = this.getLoadingMessages(fileType);
                
                if (loadingText) loadingText.textContent = messages.main;
                if (loadingSubtext) loadingSubtext.textContent = messages.sub;
            }
        }
        if (this.modalMediaContainer) {
            this.modalMediaContainer.classList.remove('show');
        }
    }
    
    /**
     * Get loading messages based on file type
     */
    getLoadingMessages(fileType) {
        const messages = {
            video: {
                main: 'Loading video...',
                sub: 'Preparing video player and buffering content'
            },
            audio: {
                main: 'Loading audio...',
                sub: 'Setting up audio player and loading track'
            },
            image: {
                main: 'Loading image...',
                sub: 'Downloading and optimizing image for display'
            },
            pdf: {
                main: 'Loading PDF...',
                sub: 'Preparing document viewer and loading content'
            },
            unknown: {
                main: 'Loading content...',
                sub: 'Please wait while we prepare your media'
            }
        };
        
        return messages[fileType] || messages.unknown;
    }
    
    hideLoading() {
        if (this.modalLoading) {
            this.modalLoading.style.display = 'none';
        }
    }
    
    /**
     * Show error message
     */
    showError(message, pdfUrl = null) {
        this.hideLoading();
        if (this.modalPdfError && pdfUrl) {
            // Use the dedicated PDF error element for PDF files
            const downloadLink = document.getElementById('pdfDownloadLink');
            const openLink = document.getElementById('pdfOpenLink');
            
            if (downloadLink) downloadLink.href = pdfUrl;
            if (openLink) openLink.href = pdfUrl;
            
            this.modalPdfError.style.display = 'flex';
            this.modalMediaContainer.style.display = 'none';
        } else if (this.modalMediaContainer) {
            // Use the media container for other errors
            this.modalMediaContainer.innerHTML = `
                <div style="text-align: center; color: #cf6679; padding: 40px;">
                    <i class="fas fa-exclamation-triangle" style="font-size: 3em; margin-bottom: 20px;"></i>
                    <p style="font-size: 1.2em; margin: 0;">${message}</p>
                </div>
            `;
            this.modalMediaContainer.classList.add('show');
        }
    }
    
    /**
     * Navigation functions
     */
    showPrevious() {
        if (this.currentMediaIndex > 0) {
            const prevMedia = this.currentMediaList[this.currentMediaIndex - 1];
            if (prevMedia) {
                // Show loading immediately when navigating
                this.showLoading(prevMedia.filename);
                this.openModal(
                    prevMedia.url,
                    prevMedia.filename,
                    prevMedia.filesize,
                    prevMedia.filetype,
                    this.currentMediaList,
                    this.currentMediaIndex - 1
                );
            }
        }
    }
    
    showNext() {
        if (this.currentMediaIndex < this.currentMediaList.length - 1) {
            const nextMedia = this.currentMediaList[this.currentMediaIndex + 1];
            if (nextMedia) {
                // Show loading immediately when navigating
                this.showLoading(nextMedia.filename);
                this.openModal(
                    nextMedia.url,
                    nextMedia.filename,
                    nextMedia.filesize,
                    nextMedia.filetype,
                    this.currentMediaList,
                    this.currentMediaIndex + 1
                );
            }
        }
    }
    
    updateNavigation() {
        if (!this.modalNavigation || this.currentMediaList.length <= 1) {
            if (this.modalNavigation) {
                this.modalNavigation.style.display = 'none';
            }
            return;
        }
        
        this.modalNavigation.style.display = 'flex';
        
        // Update previous button
        if (this.modalPrev) {
            this.modalPrev.disabled = this.currentMediaIndex <= 0;
        }
        
        // Update next button
        if (this.modalNext) {
            this.modalNext.disabled = this.currentMediaIndex >= this.currentMediaList.length - 1;
        }
    }
    
    /**
     * Media control functions
     */
    togglePlayPause() {
        const video = this.modalMediaContainer?.querySelector('video');
        const audio = this.modalMediaContainer?.querySelector('audio');
        
        if (video) {
            if (video.paused) {
                video.play().catch(e => console.warn('Video play failed:', e));
            } else {
                video.pause();
            }
        } else if (audio) {
            if (audio.paused) {
                audio.play().catch(e => console.warn('Audio play failed:', e));
            } else {
                audio.pause();
            }
        }
    }
    
    toggleFullscreen() {
        const video = this.modalMediaContainer?.querySelector('video');
        if (video && video.requestFullscreen) {
            if (document.fullscreenElement) {
                document.exitFullscreen();
            } else {
                video.requestFullscreen().catch(e => console.warn('Fullscreen failed:', e));
            }
        }
    }
    
    stopMedia() {
        const video = this.modalMediaContainer?.querySelector('video');
        const audio = this.modalMediaContainer?.querySelector('audio');
        
        if (video && !video.paused) {
            video.pause();
        }
        
        if (audio && !audio.paused) {
            audio.pause();
        }
    }
    
    /**
     * Utility functions
     */
    isModalOpen() {
        return this.modal && this.modal.classList.contains('show');
    }
    
    getFileType(filename) {
        const ext = filename.split('.').pop().toLowerCase();
        
        const videoTypes = ['mp4', 'webm', 'avi', 'mov', 'mkv', 'wmv', 'flv', 'm4v'];
        const imageTypes = ['jpg', 'jpeg', 'png', 'gif', 'webp', 'svg', 'bmp', 'tiff'];
        const audioTypes = ['mp3', 'wav', 'ogg', 'aac', 'flac', 'm4a', 'wma'];
        const pdfTypes = ['pdf'];
        
        if (videoTypes.includes(ext)) return 'video';
        if (imageTypes.includes(ext)) return 'image';
        if (audioTypes.includes(ext)) return 'audio';
        if (pdfTypes.includes(ext)) return 'pdf';
        
        return 'unknown';
    }
    
    getFileTypeDisplayName(fileType) {
        const displayNames = {
            video: 'Video',
            image: 'Image',
            audio: 'Audio',
            pdf: 'PDF'
        };
        return displayNames[fileType] || 'Media';
    }
    
    /**
     * Format file size for display
     */
    formatFileSize(bytes) {
        if (bytes === 0) return '0 Bytes';
        const k = 1000;
        const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    }
}

// Global modal instance
let mediaModal = null;

/**
 * Initialize modal system
 */
function initializeMediaModal() {
    console.log('=== DEBUG: Initializing media modal system...');
    if (mediaModal) {
        console.warn('=== DEBUG: WARNING - mediaModal already exists, potential duplicate initialization!');
    }
    mediaModal = new MediaModalLightbox();
    
    // Set up click handlers for view buttons
    setupModalViewButtons();
    
    console.log('=== DEBUG: Media modal system initialized');
}

/**
 * Set up click handlers for existing view buttons
 */
function setupModalViewButtons() {
    console.log('=== DEBUG: Setting up modal view buttons - this should only happen once per page load');
    
    // Check if we already have listeners attached
    if (window.modalListenersAttached) {
        console.error('=== DEBUG: CRITICAL - setupModalViewButtons called multiple times! This will create duplicate listeners.');
        return; // Prevent duplicate listeners
    }
    window.modalListenersAttached = true;

    let touchHandled = false;

    const handleInteraction = (e) => {
        console.log('=== DEBUG: Click event triggered on:', e.target);

        // Ensure we have an element to work with. Some browsers may fire
        // events with a non-Element target (e.g. text nodes), which would
        // cause errors when calling .closest().
        let target = e.target;
        if (!(target instanceof Element)) {
            target = target.parentElement;
        }
        if (!target) {
            return;
        }

        const viewButton = target.closest('.modal-view-btn, a[href^="/v/"].btn-success, .modal-view-link');

        if (!viewButton || !viewButton.href || !viewButton.href.includes('/v/')) {
            return;
        }

        if (e.type === 'touchend') {
            touchHandled = true;
        } else if (e.type === 'click' && touchHandled) {
            touchHandled = false;
            return;
        }

        e.preventDefault();

        const filename = getFilenameFromButton(viewButton);
        const filesize = getFilesizeFromButton(viewButton);
        const filetype = getFiletypeFromButton(viewButton);
        const mediaUrl = viewButton.href;

        const mediaList = getMediaListFromPage();
        const currentIndex = getMediaIndexFromUrl(mediaUrl, mediaList);

        if (mediaModal) {
            mediaModal.openModal(mediaUrl, filename, filesize, filetype, mediaList, currentIndex);
        }
    };

    document.addEventListener('click', handleInteraction);
    document.addEventListener('touchend', handleInteraction);
}

/**
 * Helper functions to extract file information from buttons
 */
function getFilenameFromButton(button) {
    // First try to get from button data attributes
    if (button.dataset.filename) {
        return button.dataset.filename;
    }
    
    // Fallback to extracting from table row
    const row = button.closest('.file-row');
    if (row) {
        const filenameCell = row.querySelector('.filename-column strong');
        if (filenameCell) return filenameCell.textContent.trim();
    }
    return 'Unknown File';
}

function getFilesizeFromButton(button) {
    // First try to get from button data attributes
    if (button.dataset.filesize) {
        return button.dataset.filesize;
    }
    
    // Fallback to extracting from table row
    const row = button.closest('.file-row');
    if (row) {
        const filesizeCell = row.querySelector('.filesize-cell');
        if (filesizeCell) return filesizeCell.textContent.trim();
    }
    return '';
}

function getFiletypeFromButton(button) {
    // First try to get from button data attributes
    if (button.dataset.filetype) {
        return button.dataset.filetype;
    }
    
    // Fallback to extracting from filename
    const filename = getFilenameFromButton(button);
    const ext = filename.split('.').pop().toLowerCase();
    
    const videoTypes = ['mp4', 'webm', 'avi', 'mov', 'mkv', 'wmv', 'flv', 'm4v'];
    const imageTypes = ['jpg', 'jpeg', 'png', 'gif', 'webp', 'svg', 'bmp', 'tiff'];
    const audioTypes = ['mp3', 'wav', 'ogg', 'aac', 'flac', 'm4a', 'wma'];
    const pdfTypes = ['pdf'];
    
    if (videoTypes.includes(ext)) return 'Video';
    if (imageTypes.includes(ext)) return 'Image';
    if (audioTypes.includes(ext)) return 'Audio';
    if (pdfTypes.includes(ext)) return 'PDF';
    
    return ext.toUpperCase();
}

/**
 * Get list of all viewable media from the current page for navigation
 */
function getMediaListFromPage() {
    const mediaList = [];
    const viewButtons = document.querySelectorAll('a[href^="/v/"]');
    
    viewButtons.forEach(button => {
        if (button.href && button.href.includes('/v/')) {
            const filename = getFilenameFromButton(button);
            const fileType = getFiletypeFromButton(button);
            
            // Only include actual media files (not download links)
            if (['Video', 'Image', 'Audio', 'PDF'].includes(fileType)) {
                mediaList.push({
                    url: button.href,
                    filename: filename,
                    filesize: getFilesizeFromButton(button),
                    filetype: fileType
                });
            }
        }
    });
    
    return mediaList;
}

/**
 * Get the index of current media in the media list
 */
function getMediaIndexFromUrl(currentUrl, mediaList) {
    return mediaList.findIndex(media => media.url === currentUrl);
}

/**
 * Public API for opening modal programmatically
 */
window.openMediaModal = function(mediaUrl, filename, filesize = '', filetype = '') {
    if (mediaModal) {
        const mediaList = getMediaListFromPage();
        const currentIndex = getMediaIndexFromUrl(mediaUrl, mediaList);
        mediaModal.openModal(mediaUrl, filename, filesize, filetype, mediaList, currentIndex);
    } else {
        console.error('Media modal not initialized');
    }
};

// Initialize when DOM is ready
document.addEventListener('DOMContentLoaded', function() {
    console.log('=== DEBUG: DOMContentLoaded event fired');
    initializeMediaModal();
});

// Re-initialize when new content is added (for dynamic content)
// This is disabled as it can cause duplicate listeners even with protection
// If needed, call setupModalViewButtons() manually after adding dynamic content
/*
document.addEventListener('contentUpdated', function() {
    console.log('=== DEBUG: contentUpdated event fired - this could be causing duplicate listeners!');
    setupModalViewButtons();
});
*/