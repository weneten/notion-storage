from flask import Flask, request, jsonify, Response, stream_with_context
import os
import requests
import urllib.parse
import threading
import io
import concurrent.futures
import queue
from typing import Dict, Any, List, Optional, Tuple, Union, Iterable
from flask_socketio import SocketIO
import uuid
import time
import random
import mimetypes

class ChunkProcessor:
    def __init__(self, max_concurrent_uploads=3, max_pending_chunks=5):
        self.chunk_buffer = io.BytesIO()
        self.buffer_size = 0
        self.chunk_size = 5 * 1024 * 1024  # 5MB chunks for Notion API
        self.upload_queue = queue.Queue(maxsize=max_pending_chunks)  # Limit pending chunks
        self.lock = threading.Lock()
        self.upload_futures = []
        self.max_concurrent_uploads = max_concurrent_uploads
        self.max_pending_chunks = max_pending_chunks
        self.part_number = 1
        self.total_size = 0  # Will be set when we start processing
        self.total_parts = 0  # Will be calculated based on total_size
        self.upload_error = None

    def process_chunk(self, chunk: bytes, executor: concurrent.futures.ThreadPoolExecutor, upload_func) -> None:
        if self.upload_error:
            raise self.upload_error

        # Update total size and calculate total parts if not done yet
        if self.total_size == 0:
            # This is our first chunk, initialize tracking
            self.total_size = sum(1 for _ in chunk)  # Get size of first chunk
            self.total_parts = (self.total_size + self.chunk_size - 1) // self.chunk_size
            print(f"Calculated total parts: {self.total_parts}")

        self.chunk_buffer.write(chunk)
        self.buffer_size += len(chunk)

        # Process chunks while we have enough data
        while self.buffer_size >= self.chunk_size:
            # First check if we're at the pending chunk limit
            while len(self.upload_futures) >= self.max_pending_chunks:
                # Wait for at least one upload to complete
                self._wait_for_upload_completion()

            # Get the full chunk
            self.chunk_buffer.seek(0)
            full_chunk = self.chunk_buffer.read(self.chunk_size)
            
            # Keep remaining data
            remaining_data = self.chunk_buffer.read()
            self.chunk_buffer = io.BytesIO()
            self.chunk_buffer.write(remaining_data)
            self.buffer_size = len(remaining_data)

            # Submit chunk for upload with part number and total parts info
            current_part = self.part_number
            future = executor.submit(upload_func, current_part, full_chunk)
            self.upload_futures.append((current_part, future))
            print(f"Queuing part {current_part} of {self.total_parts}")
            self.part_number += 1

            # Check for errors in completed uploads
            self._check_upload_futures()

    def _check_upload_futures(self) -> None:
        completed = []
        for idx, (part_number, future) in enumerate(self.upload_futures):
            if future.done():
                try:
                    future.result()  # Will raise if upload failed
                    completed.append(idx)
                except Exception as e:
                    self.upload_error = e
                    raise

        # Remove completed futures
        for idx in reversed(completed):
            self.upload_futures.pop(idx)

    def complete(self, executor: concurrent.futures.ThreadPoolExecutor, upload_func) -> None:
        # Upload any remaining data in the buffer
        if self.buffer_size > 0:
            final_chunk = self.chunk_buffer.getvalue()
            current_part = self.part_number
            print(f"Uploading final part {current_part} of {self.total_parts}")
            future = executor.submit(upload_func, current_part, final_chunk)
            self.upload_futures.append((current_part, future))

        # Wait for all uploads to complete
        for part_number, future in self.upload_futures:
            try:
                print(f"Waiting for part {part_number} of {self.total_parts} to complete...")
                future.result()
            except Exception as e:
                self.upload_error = e
                raise
        
        print("All parts uploaded successfully")

    def _wait_for_upload_completion(self) -> None:
        """Wait for at least one upload to complete before continuing"""
        if not self.upload_futures:
            return

        # Wait for the first upload to complete
        completed = []
        for idx, (part_number, future) in enumerate(self.upload_futures):
            if future.done():
                try:
                    future.result()  # Will raise if upload failed
                    completed.append(idx)
                    break  # Exit after first completed upload
                except Exception as e:
                    self.upload_error = e
                    raise

        # Remove completed future
        for idx in reversed(completed):
            self.upload_futures.pop(idx)

        # If no uploads completed, wait for first one
        if not completed and self.upload_futures:
            part_number, future = self.upload_futures[0]
            try:
                future.result()  # This will block until completion
                self.upload_futures.pop(0)
            except Exception as e:
                self.upload_error = e
                raise

class NotionFileUploader:
    def __init__(self, api_token: str, socketio: SocketIO = None, notion_version: str = "2022-06-28",
                 global_file_index_db_id: str = None, notion_space_id: str = None):
        self.api_token = api_token
        self.socketio = socketio
        self.base_url = "https://api.notion.com/v1"
        self.notion_version = notion_version
        self.headers = {
            "Authorization": f"Bearer {api_token}",
            "Notion-Version": notion_version,
            "accept": "application/json"
        }
        self.global_file_index_db_id = global_file_index_db_id
        self.notion_space_id = notion_space_id or "c91485e6-ff71-811c-b300-000345011419"  # Default space ID
        
        # URL caching for performance optimization
        self.url_cache = {}  # file_page_id -> {url, expires_at, generated_at}
        self.url_cache_lock = threading.Lock()
        self.url_cache_duration = 3600  # 1 hour default cache duration
        
        # Validation configuration
        self.validation_config = {
            'enabled': True,                    # Enable/disable validation entirely
            'validation_threshold': 1800,      # Only validate URLs with less than 30 min left (seconds)
            'urgent_threshold': 300,           # Urgent validation threshold - 5 min (seconds)
            'bypass_fresh_urls': True,         # Skip validation for fresh URLs (>30 min left)
            'timeout': 10,                     # Validation request timeout (seconds)
            'accept_server_errors': True,      # Treat server errors (5xx) as valid
            'accept_auth_errors': True,        # Treat auth errors (403, 405) as valid
            'fallback_on_error': True         # Use cached URL if validation fails with error
        }

    def ensure_txt_filename(self, filename: str) -> str:
        """Ensure filename has .txt extension but do not replace spaces"""
        if not filename.lower().endswith('.txt'):
            name_without_ext = os.path.splitext(filename)[0]
            filename = f"{name_without_ext}.txt"
        return filename

    def get_mime_type(self, filename: str) -> str:
        """Always return text/plain to ensure compatibility with Notion's File Upload API"""
        return 'text/plain'

    def create_file_upload(self, content_type=None, filename=None, mode="single_part", number_of_parts=None):
        """
        Creates a new file upload in Notion
        
        Args:
            content_type (str, optional): MIME type of the file
            filename (str, optional): Name of the file
            mode (str, optional): Upload mode, either "single_part" or "multi_part"
            number_of_parts (int, optional): Number of parts for multipart upload
            
        Returns:
            Dict with upload info including ID and upload URL
            
        Raises:
            Exception if creation fails
        """
        url = f"{self.base_url}/file_uploads"
        headers = {**self.headers, "Content-Type": "application/json"}
        
        payload = {}
        if content_type:
            payload["content_type"] = content_type
        if filename:
            payload["filename"] = filename
            
        # For multipart uploads, add mode and number_of_parts
        if mode == "multi_part":
            payload["mode"] = "multi_part"
            if number_of_parts:
                payload["number_of_parts"] = number_of_parts
            else:
                raise ValueError("number_of_parts must be provided for multi_part uploads")
        else:
            # Default to single part if not specified
            payload["mode"] = "single_part"
            
        print(f"Creating file upload with payload: {payload}")
        
        response = requests.post(url, headers=headers, json=payload)
        
        if response.status_code != 200:
            print(f"ERROR: Failed to create file upload: {response.text}")
            raise Exception(f"Failed to create file upload: {response.text}")
        
        result = response.json()
        
        # CRITICAL FIX 1: ID Validation on Upload Creation
        upload_id = result.get('id')
        if not upload_id:
            error_msg = f"ID CORRUPTION: File upload creation succeeded but no ID returned. Response: {result}"
            print(f"🚨 CRITICAL ERROR: {error_msg}")
            raise Exception(error_msg)
        
        print(f"🔍 CREATE_UPLOAD: File upload created with ID: {upload_id}")
        print(f"🔍 CREATE_UPLOAD: Response keys: {list(result.keys())}")
        
        # Additional validation
        if isinstance(upload_id, str) and len(upload_id.strip()) == 0:
            error_msg = f"ID CORRUPTION: Upload ID is empty string"
            print(f"🚨 CRITICAL ERROR: {error_msg}")
            raise Exception(error_msg)
        
        return result

    def send_file_content(self, file_upload_id: str, file_stream: Union[Iterable[bytes], io.BytesIO], content_type: str, filename: str, total_size: int) -> Dict[str, Any]:
        """Step 2: Send file content to Notion (for single file uploads)"""
        url = f"{self.base_url}/file_uploads/{file_upload_id}/send"

        # Always use generic file.txt for Notion's site
        notion_filename = "file.txt"

        # Handle different stream types properly for requests.post()
        if isinstance(file_stream, io.BytesIO):
            file_stream.seek(0)  # Ensure we're at the beginning
            files = {
                'file': (notion_filename, file_stream, content_type)
            }
        else:
            # Handle other stream types by accumulating data
            buffer = io.BytesIO()
            for chunk in file_stream:
                buffer.write(chunk)
            buffer.seek(0)
            files = {
                'file': (notion_filename, buffer, content_type)
            }

        headers = {
            'Authorization': self.headers['Authorization'],
            'Notion-Version': self.headers['Notion-Version'],
        }

        print(f"Uploading file content for {filename} with content type: {content_type}...")
        response = requests.post(url, files=files, headers=headers)

        if response.status_code != 200:
            raise Exception(f"File content upload failed with status {response.status_code}: {response.text}")

        result = response.json()
        
        # CRITICAL FIX 1: ID Validation on File Content Upload
        print(f"🔍 SEND_CONTENT: File content uploaded successfully. Status: {result.get('status')}")
        print(f"🔍 SEND_CONTENT: Response keys: {list(result.keys())}")
        
        # Validate that the file upload ID is preserved in the response
        response_file_info = result.get('file', {})
        if response_file_info:
            print(f"🔍 SEND_CONTENT: File info in response: {list(response_file_info.keys())}")
        
        return result

    def create_file_block(self, page_id: str, file_upload_id: str, filename: str) -> Dict[str, Any]:
        url = f"{self.base_url}/blocks/{page_id}/children"

        # Always use generic file.txt for Notion's site
        notion_filename = "file.txt"

        payload = {
            "children": [
                {
                    "type": "file",
                    "file": {
                        "type": "file_upload",
                        "file_upload": {
                            "id": file_upload_id
                        }
                    }
                }
            ]
        }

        headers = {**self.headers, "Content-Type": "application/json"}

        print(f"Creating file block for {filename}...")
        response = requests.patch(url, json=payload, headers=headers)

        if response.status_code != 200:
            raise Exception(f"File block creation failed with status {response.status_code}: {response.text}")

        result = response.json()
        print(f"File block created successfully!")
        return result

    def get_block_info(self, block_id: str) -> Dict[str, Any]:
        """Get information about a specific block"""
        url = f"{self.base_url}/blocks/{block_id}"
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error getting block info: {response.status_code} - {response.text}")
            return {}

    def get_download_url(self, block_result: Dict[str, Any], original_filename: str) -> str:
        """
        Get the Notion download link for the uploaded file.
        This method is based on get_file_download_info from uplaod_conc.py
        """
        try:
            # Get block ID from the result
            block_id = block_result['results'][0]['id'] if block_result.get('results') else None
            if not block_id:
                return ""

            # Get block information to extract file details
            block_info = self.get_block_info(block_id)
            if not block_info:
                return ""

            # Extract file information from block
            file_info = block_info.get('file', {})
            file_url = file_info.get('file', {}).get('url', '')
            if not file_url:
                return ""

            return file_url
        except Exception as e:
            print(f"Error constructing download URL: {e}")
            return ""

    def verify_page_id(self, page_id: str) -> bool:
        """Verify that the page ID exists and is accessible"""
        url = f"{self.base_url}/pages/{page_id}"

        try:
            response = requests.get(url, headers=self.headers)
            return response.status_code == 200
        except Exception as e:
            print(f"Error verifying page ID: {e}")
            return False

    def get_download_url_for_file(self, page_id: str, original_filename: str) -> str:
        """
        Get download URL for a file, now prioritizing permanent URLs over temporary ones.
        Returns permanent Notion signed attachment URLs when available.
        """
        # This function is now deprecated and replaced by the always-fresh download link logic.
        raise NotImplementedError("get_download_url_for_file legacy method is removed. Use the new always-fresh download link logic.")



    def get_download_url_for_file(self, page_id: str, original_filename: str) -> str:
        """
        Always fetch a fresh Notion download link and cache it for 30 minutes.
        """
        with self.url_cache_lock:
            cached_entry = self.url_cache.get(page_id)
            current_time = time.time()
            if cached_entry:
                expires_at = cached_entry.get('expires_at', 0)
                cached_url = cached_entry.get('url', '')
                if current_time < expires_at and cached_url:
                    print(f"[CACHE HIT] Using cached download URL for {page_id}")
                    return cached_url
                else:
                    print(f"[CACHE EXPIRED] Cached URL expired for {page_id}")
                    del self.url_cache[page_id]
        # Always fetch a new download link from Notion
        print(f"[FETCH] Fetching new download URL for {page_id}")
        new_url, file_size, content_type = self._fetch_fresh_download_url_with_metadata(page_id, original_filename)
        if new_url:
            cache_entry = {
                'url': new_url,
                'expires_at': current_time + self.url_cache_duration,
                'generated_at': current_time,
                'file_size': file_size,
                'content_type': content_type
            }
            with self.url_cache_lock:
                self.url_cache[page_id] = cache_entry
            print(f"[CACHE STORE] New download URL cached for {page_id}")
        return new_url

    def _validate_cached_url(self, url: str, time_until_expiry: float = 0) -> bool:
        """
        Validate that a cached URL is still working using smart validation method.
        Uses small GET range request instead of HEAD for better compatibility.
        """
        try:
            print(f"🔍 VALIDATING URL: Starting validation (expires in {int(time_until_expiry)}s)")
            
            # Use small range GET request instead of HEAD for better compatibility with Notion's CDN
            headers = {
                'Range': 'bytes=0-1',  # Request just first 2 bytes
                'User-Agent': 'NotionUploader/1.0'
            }
            
            response = requests.get(
                url,
                headers=headers,
                timeout=self.validation_config['timeout'],
                allow_redirects=True
            )
            
            # Build valid status codes based on configuration
            valid_status_codes = [200, 206, 416]  # 416 = Range Not Satisfiable (but URL exists)
            
            if self.validation_config['accept_auth_errors']:
                valid_status_codes.extend([403, 405])  # Forbidden, Method Not Allowed
            
            if response.status_code in valid_status_codes:
                print(f"✅ URL VALIDATION PASSED: Status {response.status_code}")
                return True
            elif response.status_code == 404:
                print(f"🚨 URL VALIDATION FAILED: Status {response.status_code} - URL not found")
                return False
            elif response.status_code in [500, 502, 503, 504]:
                # Server errors - handle based on configuration
                if self.validation_config['accept_server_errors']:
                    print(f"⚠️ URL VALIDATION INCONCLUSIVE: Server error {response.status_code}, treating as valid per config")
                    return True
                else:
                    print(f"🚨 URL VALIDATION FAILED: Server error {response.status_code}")
                    return False
            else:
                print(f"🚨 URL VALIDATION FAILED: Unexpected status {response.status_code}")
                return False
                
        except requests.exceptions.Timeout:
            if self.validation_config['fallback_on_error']:
                print(f"⚠️ URL VALIDATION TIMEOUT: Request timed out, using cached URL per config")
                return True
            else:
                print(f"🚨 URL VALIDATION TIMEOUT: Request timed out, invalidating cached URL")
                return False
        except requests.exceptions.ConnectionError as e:
            if self.validation_config['fallback_on_error']:
                print(f"⚠️ URL VALIDATION CONNECTION ERROR: {e}, using cached URL per config")
                return True
            else:
                print(f"🚨 URL VALIDATION CONNECTION ERROR: {e}, invalidating cached URL")
                return False
        except Exception as e:
            if self.validation_config['fallback_on_error']:
                print(f"⚠️ URL VALIDATION ERROR: {e}, using cached URL per config")
                return True
            else:
                print(f"🚨 URL VALIDATION ERROR: {e}, invalidating cached URL")
                return False

    def _fetch_fresh_download_url(self, page_id: str, original_filename: str) -> str:
        """
        Fetch a fresh download URL from Notion API (legacy method for backward compatibility).
        """
        url, _, _ = self._fetch_fresh_download_url_with_metadata(page_id, original_filename)
        return url

    def _fetch_fresh_download_url_with_metadata(self, page_id: str, original_filename: str) -> tuple:
        """
        Fetch a fresh download URL from Notion API with file size and content type detection.
        
        Returns:
            tuple: (download_url, file_size, content_type)
        """
        try:
            print(f"Getting fresh download URL with metadata for file page ID: {page_id}, original filename: {original_filename}")
            
            # Get page information to extract file details
            page_info = self.get_user_by_id(page_id) # Reusing get_user_by_id as it fetches a page
            if not page_info:
                print(f"No page found with ID: {page_id}")
                return "", 0, "application/octet-stream"

            # Extract file information from the 'file' property
            # Assuming 'file' is the name of the file property in the database
            file_property = page_info.get('properties', {}).get('file', {})
            files_array = file_property.get('files', [])

            if not files_array:
                print(f"No files found in 'file' property for page ID: {page_id}")
                return "", 0, "application/octet-stream"

            # Get the first file in the array
            file_info = files_array[0]
            file_url = file_info.get('file', {}).get('url', '') # Corrected path to the URL

            if not file_url:
                print(f"No valid URL found in file property for page ID: {page_id}")
                print(f"File info: {file_info}")
                return "", 0, "application/octet-stream"

            print(f"Successfully retrieved fresh download URL for file: {original_filename}")
            
            # Get file size from Notion file properties if available
            file_size = self.get_file_size_from_notion(page_info, original_filename)
            
            # If not available from Notion, try to get it from the download URL
            if file_size == 0:
                file_size = self.get_file_size_from_url(file_url)
            
            # Determine content type
            content_type = self.get_content_type_from_filename(original_filename)
            
            print(f"📊 File metadata: size={file_size} bytes, content_type={content_type}")
            return file_url, file_size, content_type
            
        except Exception as e:
            print(f"Error constructing download URL from page property: {e}")
            import traceback
            traceback.print_exc()
            return "", 0, "application/octet-stream"

    def get_file_size_from_notion(self, page_info: Dict[str, Any], original_filename: str) -> int:
        """
        Extract file size from Notion page properties if available.
        
        Args:
            page_info: Notion page information containing file properties
            original_filename: Original filename for logging
            
        Returns:
            int: File size in bytes, or 0 if not available
        """
        try:
            # Check if there's a filesize property in the page
            file_size = page_info.get('properties', {}).get('filesize', {}).get('number', 0)
            if file_size > 0:
                print(f"📊 Got file size from Notion properties: {file_size} bytes for {original_filename}")
                return file_size
            
            print(f"📊 No file size found in Notion properties for {original_filename}")
            return 0
            
        except Exception as e:
            print(f"Error getting file size from Notion properties: {e}")
            return 0

    def get_file_size_from_url(self, download_url: str) -> int:
        """
        Get file size by making a HEAD request to the download URL.
        
        Args:
            download_url: The Notion download URL
            
        Returns:
            int: File size in bytes, or 0 if not available
        """
        try:
            print(f"📊 Attempting to get file size from URL via HEAD request")
            
            # Make HEAD request to get Content-Length without downloading the file
            response = requests.head(
                download_url,
                timeout=10,
                allow_redirects=True,
                headers={'User-Agent': 'NotionUploader/1.0'}
            )
            
            if response.status_code == 200:
                content_length = response.headers.get('Content-Length')
                if content_length:
                    file_size = int(content_length)
                    print(f"📊 Got file size from HEAD request: {file_size} bytes")
                    return file_size
                else:
                    print(f"📊 No Content-Length header in HEAD response")
            else:
                print(f"📊 HEAD request failed with status {response.status_code}")
                
        except requests.exceptions.RequestException as e:
            print(f"📊 HEAD request failed: {e}")
        except Exception as e:
            print(f"📊 Error getting file size from URL: {e}")
            
        return 0

    def get_content_type_from_filename(self, filename: str) -> str:
        """
        Determine content type from filename extension.
        
        Args:
            filename: Original filename
            
        Returns:
            str: MIME type string
        """
        try:
            content_type, _ = mimetypes.guess_type(filename)
            if content_type:
                return content_type
            
            # Fallback for common file types
            extension = filename.lower().split('.')[-1] if '.' in filename else ''
            content_types = {
                'txt': 'text/plain',
                'pdf': 'application/pdf',
                'jpg': 'image/jpeg',
                'jpeg': 'image/jpeg',
                'png': 'image/png',
                'gif': 'image/gif',
                'mp4': 'video/mp4',
                'avi': 'video/x-msvideo',
                'mov': 'video/quicktime',
                'mp3': 'audio/mpeg',
                'wav': 'audio/wav',
                'zip': 'application/x-zip-compressed',
                'doc': 'application/msword',
                'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
                'xls': 'application/vnd.ms-excel',
                'xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            }
            
            return content_types.get(extension, 'application/octet-stream')
            
        except Exception as e:
            print(f"Error determining content type: {e}")
            return 'application/octet-stream'


    def cache_file_metadata(self, page_id: str, url: str, file_size: int, content_type: str):
        """Cache file metadata including size and content type."""
        with self.url_cache_lock:
            current_time = time.time()
            cache_entry = {
                'url': url,
                'expires_at': current_time + self.url_cache_duration,
                'generated_at': current_time,
                'file_size': file_size,
                'content_type': content_type
            }
            self.url_cache[page_id] = cache_entry
            print(f"💾 CACHED: Metadata cached for {page_id} - size: {file_size} bytes, type: {content_type}")

    def get_cached_file_metadata(self, page_id: str) -> tuple:
        """
        Get cached file metadata including size and content type.
        
        Args:
            page_id: Notion page ID
            
        Returns:
            tuple: (url, file_size, content_type) or (None, 0, None) if not cached or expired
        """
        with self.url_cache_lock:
            cached_entry = self.url_cache.get(page_id)
            if cached_entry:
                current_time = time.time()
                expires_at = cached_entry.get('expires_at', 0)
                
                if current_time < expires_at:
                    url = cached_entry.get('url', '')
                    file_size = cached_entry.get('file_size', 0)
                    content_type = cached_entry.get('content_type', 'application/octet-stream')
                    print(f"📊 Retrieved cached metadata for {page_id}: size={file_size}, type={content_type}")
                    return url, file_size, content_type
                else:
                    print(f"📊 Cached metadata expired for {page_id}")
                    
            return None, 0, None

    def get_file_download_metadata(self, page_id: str, original_filename: str) -> Dict[str, Any]:
        """
        Get comprehensive file metadata for download including URL, size, and content type.
        
        Args:
            page_id: Notion page ID containing the file
            original_filename: Original filename for content type detection
            
        Returns:
            Dict containing url, file_size, content_type, and cached status
        """
        try:
            # First try to get from cache
            cached_url, cached_file_size, cached_content_type = self.get_cached_file_metadata(page_id)
            
            if cached_url:
                return {
                    'url': cached_url,
                    'file_size': cached_file_size,
                    'content_type': cached_content_type,
                    'cached': True
                }
            
            # Cache miss - fetch fresh data with metadata
            fresh_url, file_size, content_type = self._fetch_fresh_download_url_with_metadata(page_id, original_filename)
            
            if fresh_url:
                # Cache the fresh metadata
                self.cache_file_metadata(page_id, fresh_url, file_size, content_type)
                
                return {
                    'url': fresh_url,
                    'file_size': file_size,
                    'content_type': content_type,
                    'cached': False
                }
            
            # Fallback
            return {
                'url': '',
                'file_size': 0,
                'content_type': 'application/octet-stream',
                'cached': False
            }
            
        except Exception as e:
            print(f"Error getting file download metadata: {e}")
            return {
                'url': '',
                'file_size': 0,
                'content_type': 'application/octet-stream',
                'cached': False
            }

    def get_notion_file_url_from_page_property(self, page_id: str, original_filename: str) -> str:
        """
        Legacy method - now uses caching optimization.
        """
        return self.get_download_url_for_file(page_id, original_filename)

    def clear_url_cache(self, page_id: str = None):
        """
        Clear URL cache entries. If page_id is provided, clear only that entry.
        Otherwise, clear all cached URLs.
        """
        with self.url_cache_lock:
            if page_id:
                if page_id in self.url_cache:
                    del self.url_cache[page_id]
                    print(f"🗑️ CACHE CLEARED: Removed cached URL for {page_id}")
            else:
                cleared_count = len(self.url_cache)
                self.url_cache.clear()
                print(f"🗑️ CACHE CLEARED: Removed {cleared_count} cached URLs")

    def get_cache_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the URL cache.
        """
        with self.url_cache_lock:
            current_time = time.time()
            valid_entries = 0
            expired_entries = 0
            
            for page_id, entry in self.url_cache.items():
                if current_time < entry.get('expires_at', 0):
                    valid_entries += 1
                else:
                    expired_entries += 1
            
            return {
                'total_entries': len(self.url_cache),
                'valid_entries': valid_entries,
                'expired_entries': expired_entries,
                'cache_duration': self.url_cache_duration,
                'current_time': current_time
            }

    def cleanup_expired_cache_entries(self):
        """
        Remove expired entries from the URL cache.
        """
        with self.url_cache_lock:
            current_time = time.time()
            expired_keys = []
            
            for page_id, entry in self.url_cache.items():
                if current_time >= entry.get('expires_at', 0):
                    expired_keys.append(page_id)
            
            for key in expired_keys:
                del self.url_cache[key]
            
            if expired_keys:
                print(f"🧹 CACHE CLEANUP: Removed {len(expired_keys)} expired entries")

    def configure_validation(self, **kwargs) -> Dict[str, Any]:
        """
        Configure URL validation behavior.
        
        Args:
            enabled (bool): Enable/disable validation entirely
            validation_threshold (int): Only validate URLs with less than this many seconds left
            urgent_threshold (int): Urgent validation threshold in seconds
            bypass_fresh_urls (bool): Skip validation for fresh URLs
            timeout (int): Validation request timeout in seconds
            accept_server_errors (bool): Treat server errors (5xx) as valid
            accept_auth_errors (bool): Treat auth errors (403, 405) as valid
            fallback_on_error (bool): Use cached URL if validation fails with error
            
        Returns:
            Dict with current validation configuration
        """
        for key, value in kwargs.items():
            if key in self.validation_config:
                old_value = self.validation_config[key]
                self.validation_config[key] = value
                print(f"📝 VALIDATION CONFIG: {key} changed from {old_value} to {value}")
            else:
                print(f"⚠️ VALIDATION CONFIG: Unknown configuration key: {key}")
        
        return self.get_validation_config()
    
    def get_validation_config(self) -> Dict[str, Any]:
        """
        Get current validation configuration.
        
        Returns:
            Dict with current validation settings
        """
        return self.validation_config.copy()
    
    def disable_validation(self):
        """
        Disable URL validation entirely - cached URLs will be used without validation.
        """
        self.validation_config['enabled'] = False
        print("🚫 VALIDATION DISABLED: Cached URLs will be used without validation")
    
    def enable_validation(self):
        """
        Enable URL validation with smart timing.
        """
        self.validation_config['enabled'] = True
        print("✅ VALIDATION ENABLED: Smart validation timing active")
    
    def set_conservative_validation(self):
        """
        Set conservative validation settings - validate more aggressively.
        """
        self.validation_config.update({
            'enabled': True,
            'validation_threshold': 3600,  # Validate URLs with less than 1 hour left
            'urgent_threshold': 1800,      # Urgent validation for URLs with less than 30 min left
            'bypass_fresh_urls': False,    # Always validate
            'accept_server_errors': False, # Don't accept server errors as valid
            'accept_auth_errors': True,    # Still accept auth errors (403, 405)
            'fallback_on_error': False     # Don't use cached URL on validation errors
        })
        print("🛡️ CONSERVATIVE VALIDATION: More aggressive validation enabled")
    
    def set_permissive_validation(self):
        """
        Set permissive validation settings - validate less aggressively.
        """
        self.validation_config.update({
            'enabled': True,
            'validation_threshold': 900,   # Only validate URLs with less than 15 min left
            'urgent_threshold': 300,       # Urgent validation for URLs with less than 5 min left
            'bypass_fresh_urls': True,     # Skip validation for fresh URLs
            'accept_server_errors': True,  # Accept server errors as valid
            'accept_auth_errors': True,    # Accept auth errors as valid
            'fallback_on_error': True      # Use cached URL on validation errors
        })
        print("🤝 PERMISSIVE VALIDATION: Less aggressive validation enabled")
    def upload_file_stream(self, file_stream: Iterable[bytes], filename: str, user_id: str, total_size: int, existing_upload_info: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Uploads a file to Notion from a stream to the specified database.
        
        Args:
            file_stream: Iterator yielding file chunks
            filename: Original filename
            user_id: Notion user ID
            total_size: Total file size in bytes
            existing_upload_info: Optional pre-created multipart upload info
        """
        # Get or create user's database
        database_id = self.get_user_database_id(user_id)

        original_filename = filename
        # Ensure filename has .txt extension for Notion compatibility
        filename = self.ensure_txt_filename(filename)
        content_type = self.get_mime_type(filename)

        if total_size <= 5 * 1024 * 1024:  # 5 MiB limit for single file upload
            return self.upload_single_file_stream(file_stream, filename, database_id, content_type, total_size, original_filename)
        else:
            return self.upload_large_file_multipart_stream(
                file_stream, 
                filename, 
                database_id, 
                content_type, 
                total_size, 
                original_filename,
                existing_upload_info=existing_upload_info
            )

    def upload_single_file_stream(self, file_stream: Iterable[bytes], filename: str, database_id: str, content_type: str, file_size: int, original_filename: str = None) -> Dict[str, Any]:
        """Handles the upload of a single file from a stream to user's database."""
        try:
            if original_filename is None:
                original_filename = filename

            # Initial progress - keep this to show upload is starting
            if self.socketio:
                self.socketio.emit('upload_progress', {'percentage': 0, 'bytes_uploaded': 0, 'total_bytes': file_size})

            # Step 1: Create file upload object
            upload_info = self.create_file_upload(content_type, filename)
            file_upload_id = upload_info['id']

            # Initial progress after creating upload - keep this to show upload is starting
            if self.socketio:
                self.socketio.emit('upload_progress', {'percentage': 10, 'bytes_uploaded': 0, 'total_bytes': file_size})

            # Step 2: Send file content
            # Create an accumulator for the streamed data
            total_bytes_received = 0
            stream_buffer = io.BytesIO()

            # Accumulate data from the stream and track progress
            for chunk in file_stream:
                stream_buffer.write(chunk)
                total_bytes_received += len(chunk)
                
                # Comment out progress updates during streaming to prevent UI jumping
                # if self.socketio and file_size > 0:
                #     progress = min(90, int((total_bytes_received / file_size) * 80) + 10)
                #     self.socketio.emit('upload_progress', {
                #         'percentage': progress,
                #         'bytes_uploaded': total_bytes_received,
                #         'total_bytes': file_size
                #     })

            # Upload the complete file
            stream_buffer.seek(0)
            upload_result = self.send_file_content(file_upload_id, stream_buffer, content_type, filename, file_size)

            # Final progress - keep this to show upload is complete
            if self.socketio:
                self.socketio.emit('upload_progress', {'percentage': 100, 'bytes_uploaded': file_size, 'total_bytes': file_size})

            # Get the download URL
            download_url = upload_result.get('file', {}).get('url', f"https://notion.so/file/{file_upload_id}")

            # CRITICAL FIX 1: ID Validation on Upload Success
            print(f"🔍 SINGLE_UPLOAD: Success with file_upload_id: {file_upload_id}")
            print(f"🔍 SINGLE_UPLOAD: Download URL: {download_url[:50]}..." if download_url else "No download URL")
            
            result = {
                "message": "File uploaded successfully",
                "download_link": download_url,
                "original_filename": original_filename,
                "database_id": database_id,
                "file_upload_id": file_upload_id
            }
            
            # Validate result before returning
            if not result.get('file_upload_id'):
                error_msg = f"ID CORRUPTION: Result missing file_upload_id after successful upload"
                print(f"🚨 CRITICAL ERROR: {error_msg}")
                raise Exception(error_msg)
            
            return result

        except Exception as e:
            # CRITICAL FIX 4: Enhanced Error Handling
            error_msg = str(e).lower()
            if any(keyword in error_msg for keyword in ['id', 'upload_id', 'null', 'empty']):
                print(f"🚨 ID-RELATED ERROR in single file upload: {e}")
                print(f"🔍 ERROR CONTEXT: filename={filename}, file_size={file_size}")
                print(f"🔍 ERROR CONTEXT: file_upload_id={file_upload_id if 'file_upload_id' in locals() else 'N/A'}")
            
            raise Exception(f"Error uploading single file: {e}")

    def upload_large_file_multipart_stream(self, file_stream: Iterable[bytes], filename: str, database_id: str, content_type: str, file_size: int, original_filename: str, chunk_size: int = 5 * 1024 * 1024, existing_upload_info: Dict[str, Any] = None) -> Dict[str, Any]:
        """Handles the multipart upload of a large file from a stream."""
        try:
            # Always use "file.txt" for Notion's site (the original filename is preserved elsewhere)
            notion_filename = "file.txt"
            
            if not original_filename:
                original_filename = filename
            
            # Use existing upload info if provided, otherwise create new
            if existing_upload_info:
                print("Using existing multipart upload info")
                multipart_upload_info = existing_upload_info
            else:
                # Calculate number of parts based on chunk size
                number_of_parts = (file_size + chunk_size - 1) // chunk_size
                print(f"Creating new multipart upload with {number_of_parts} parts")
                print(f"Original filename: {original_filename}")
                print(f"Stored as: {notion_filename} for Notion compatibility")
                # Step 1: Create multipart upload
                multipart_upload_info = self.create_file_upload(content_type, filename, "multi_part", number_of_parts)
            
            file_upload_id = multipart_upload_info['id']
            chunk_processor = ChunkProcessor()

            def upload_chunk(part_number: int, chunk_data: bytes) -> Dict:
                try:
                    result = self.send_file_part(
                        file_upload_id,
                        part_number,
                        chunk_data,
                        notion_filename,  # Use notion_filename (file.txt) here
                        content_type,
                        part_number * chunk_size,  # Approximate bytes uploaded
                        file_size,
                        multipart_upload_info['number_of_parts'],
                        ""  # session_id
                    )

                    # Commented out to prevent server-to-Notion progress updates from being shown to the user
                    # if self.socketio:
                    #     progress = min(100, int((part_number * chunk_size / file_size) * 100))
                    #     self.socketio.emit('upload_progress', {
                    #         'percentage': progress,
                    #         'bytes_uploaded': min(part_number * chunk_size, file_size),
                    #         'total_bytes': file_size
                    #     })

                    return result
                except Exception as e:
                    print(f"Error uploading part {part_number}: {e}")
                    raise

            # Create a thread pool for concurrent uploads
            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
                # Process incoming chunks
                for chunk in file_stream:
                    chunk_processor.process_chunk(chunk, executor, upload_chunk)

                # Complete any remaining uploads
                chunk_processor.complete(executor, upload_chunk)

            # Step 3: Complete multipart upload
            complete_result = self.complete_multipart_upload(file_upload_id)

            # CRITICAL FIX: Extract the actual file ID from completion response
            actual_file_id = complete_result.get('file', {}).get('id')
            if actual_file_id:
                print(f"🔍 MULTIPART_COMPLETE: Got actual file ID from completion: {actual_file_id}")
                file_upload_id = actual_file_id  # Use the correct ID from completion response
            else:
                print(f"🔍 MULTIPART_COMPLETE: No file ID in completion response, using upload ID: {file_upload_id}")

            # Return the download URL and other info
            download_url = complete_result.get('file', {}).get('url', f"https://notion.so/file/{file_upload_id}")
            
            # CRITICAL FIX 1: ID Validation on Multipart Upload Success
            print(f"🔍 MULTIPART_UPLOAD: Success with file_upload_id: {file_upload_id}")
            print(f"🔍 MULTIPART_UPLOAD: Download URL: {download_url[:50]}..." if download_url else "No download URL")
            
            result = {
                "message": "File uploaded successfully",
                "download_link": download_url,
                "original_filename": original_filename,
                "database_id": database_id,
                "file_upload_id": file_upload_id
            }
            
            # Validate result before returning
            if not result.get('file_upload_id'):
                error_msg = f"ID CORRUPTION: Multipart result missing file_upload_id after successful upload"
                print(f"🚨 CRITICAL ERROR: {error_msg}")
                raise Exception(error_msg)
            
            return result

        except Exception as e:
            # CRITICAL FIX 4: Enhanced Error Handling for Multipart Uploads
            error_msg = str(e).lower()
            if any(keyword in error_msg for keyword in ['id', 'upload_id', 'null', 'empty', 'part']):
                print(f"🚨 ID-RELATED ERROR in multipart upload: {e}")
                print(f"🔍 ERROR CONTEXT: filename={filename}, file_size={file_size}")
                print(f"🔍 ERROR CONTEXT: file_upload_id={file_upload_id if 'file_upload_id' in locals() else 'N/A'}")
                print(f"🔍 ERROR CONTEXT: multipart_upload_info={multipart_upload_info if 'multipart_upload_info' in locals() else 'N/A'}")
            
            raise Exception(f"Error uploading large file multipart: {e}")

    def upload_part_thread(self, upload_id: str, part_number: int, upload_url: str, chunk: bytes, filename: str, total_size: int, initial_uploaded_bytes: int, lock: threading.Lock, content_type: str, total_parts: int, session_id: str = None):
        try:
            # Calculate bytes uploaded so far (this part + initial bytes)
            bytes_uploaded_so_far = initial_uploaded_bytes + len(chunk)

            # Call send_file_part with all required parameters
            self.send_file_part(upload_id, part_number, chunk, filename, content_type,
                               bytes_uploaded_so_far, total_size, total_parts, session_id or "")

            with lock:
                # Progress is now handled by send_file_part
                pass
        except Exception as e:
            print(f"Error uploading part {part_number}: {e}")

    def create_multipart_upload(self, filename: str, content_type: str, number_of_parts: int) -> Dict[str, Any]:
        """
        Create a multipart upload for large files.
        
        The Notion API requires:
        - All parts except the last one must be exactly 5 MiB
        - The last part can be any size
        - Parts must be uploaded in order
        
        Args:
            filename: Original file name (will be stored as metadata)
            content_type: Content type (always use text/plain for Notion)
            number_of_parts: Number of parts the file will be split into
            
        Returns:
            Dict containing upload info from Notion API
        """
        url = f"{self.base_url}/file_uploads"

        # ALWAYS use "file.txt" for the actual file in Notion
        notion_filename = "file.txt"
        
        # Always use text/plain for Notion API compatibility
        payload = {
            "filename": notion_filename,
            "content_type": "text/plain",
            "mode": "multi_part",
            "number_of_parts": number_of_parts
        }

        headers = {**self.headers, "Content-Type": "application/json"}

        print(f"Creating multipart upload for {filename} (stored as {notion_filename}) with {number_of_parts} parts and content type: {content_type}...")
        print(f"NOTE: All parts except the last must be EXACTLY 5 MiB as required by Notion's API")
        
        response = requests.post(url, json=payload, headers=headers)

        if response.status_code != 200:
            raise Exception(f"Multipart upload creation failed: {response.text}")

        # Map the Notion response to our expected format
        # Store the original response
        result = response.json()
        # Add number_of_parts since it's needed for chunking
        result['number_of_parts'] = number_of_parts
        return result

    def send_file_part(self, file_upload_id: str, part_number: int, chunk_data: bytes, filename: str, content_type: str, bytes_uploaded_so_far: int, total_bytes: int, total_parts: int, session_id: str) -> Dict[str, Any]:
        """Send a chunk/part of a multipart upload with enhanced resilience and circuit breaker protection"""
        # Import circuit breaker
        try:
            from .circuit_breaker import notion_api_circuit_breaker
        except ImportError:
            # Fallback if circuit breaker not available
            notion_api_circuit_breaker = None
        
        if notion_api_circuit_breaker:
            # Use circuit breaker protection
            return notion_api_circuit_breaker.call(
                self._send_file_part_with_retry,
                file_upload_id, part_number, chunk_data, filename, content_type,
                bytes_uploaded_so_far, total_bytes, total_parts, session_id
            )
        else:
            # Direct call without circuit breaker
            return self._send_file_part_with_retry(
                file_upload_id, part_number, chunk_data, filename, content_type,
                bytes_uploaded_so_far, total_bytes, total_parts, session_id
            )
    
    def _send_file_part_with_retry(self, file_upload_id: str, part_number: int, chunk_data: bytes, filename: str, content_type: str, bytes_uploaded_so_far: int, total_bytes: int, total_parts: int, session_id: str, max_retries: int = 5) -> Dict[str, Any]:
        """Enhanced send_file_part with comprehensive retry logic and resilience"""
        import random
        import time
        request_id = f"part_{part_number}_{uuid.uuid4().hex[:8]}"
        
        # Retry configuration
        retry_config = {
            'max_retries': max_retries,
            'initial_delay': 1.0,
            'max_delay': 120.0,
            'exponential_base': 2.0,
            'jitter_percent': 25,
            'retryable_status_codes': [502, 503, 504],
            'retryable_exceptions': [
                requests.exceptions.Timeout,
                requests.exceptions.ConnectionError,
                requests.exceptions.ChunkedEncodingError
            ]
        }
        
        upload_url = None
        if file_upload_id.startswith('http'):
            upload_url = f"{file_upload_id}/send"
        else:
            upload_url = f"{self.base_url}/file_uploads/{file_upload_id}/send"
            
        headers = self.headers.copy()
        if "Content-Type" in headers:
            del headers["Content-Type"]
            
        # Calculate progress percentage
        progress_percentage = 0
        if total_bytes > 0:
            progress_percentage = (bytes_uploaded_so_far + len(chunk_data)) / total_bytes * 100
            
        print(f"Uploading part {part_number} of {total_parts} ({len(chunk_data)/(1024*1024):.3f} MiB)")
        
        # Log memory snapshot for high part numbers
            
        # Prepare the multipart/form-data request
        files = {
            'file': ('file.txt', chunk_data, 'text/plain'),
            'part_number': (None, str(part_number))
        }
        
        last_exception = None
        
        for attempt in range(retry_config['max_retries']):
            try:
                # Log request start
                
                # Multi-tier timeout strategy: (connect_timeout, read_timeout)
                timeout_config = (30, 300)  # 30s connect, 5min read
                
                response = requests.post(
                    upload_url,
                    headers=headers,
                    files=files,
                    timeout=timeout_config
                )
                
                # Log successful request
                
                # Check for retryable status codes
                if response.status_code in retry_config['retryable_status_codes']:
                    if attempt < retry_config['max_retries'] - 1:
                        delay = self._calculate_retry_delay(attempt, retry_config)
                        print(f"Part {part_number} got {response.status_code}, retrying in {delay:.2f}s (attempt {attempt + 1}/{retry_config['max_retries']})")
                        time.sleep(delay)
                        continue
                    else:
                        raise Exception(f"Part {part_number} failed with {response.status_code} after {retry_config['max_retries']} attempts: {response.text}")
                
                if response.status_code != 200:
                    print(f"ERROR uploading part {part_number}: {response.text}")
                    raise Exception(f"Failed to upload part {part_number}: {response.text}")
                    
                # Parse response
                try:
                    response_data = response.json()
                    print(f"Response for part {part_number}: {response_data}")
                    
                    # Handle ETag extraction
                    etag = None
                    if 'etag' in response_data:
                        etag = response_data['etag']
                        print(f"Found ETag for part {part_number}: {etag}")
                    elif 'part' in response_data and 'etag' in response_data['part']:
                        etag = response_data['part']['etag']
                        print(f"Found ETag in 'part' for part {part_number}: {etag}")
                    else:
                        print(f"No ETag found in response for part {part_number}")
                    
                    if etag:
                        response_data['etag'] = etag
                    
                    print(f"Successfully uploaded part {part_number} of {total_parts}")
                    return response_data
                    
                except ValueError as e:
                    print(f"WARNING: Could not parse JSON response for part {part_number}: {e}")
                    response_text = response.text
                    return {
                        "part_number": part_number,
                        "success": True,
                        "response_text": response_text[:100] + "..." if len(response_text) > 100 else response_text
                    }
                    
            except tuple(retry_config['retryable_exceptions']) as e:
                last_exception = e
                
                # Enhanced error logging for timeout-related issues
                error_msg = str(e).lower()
                if any(keyword in error_msg for keyword in ['timeout', '504', 'gateway', 'connection', 'reset']):
                    print(f"🚨 TIMEOUT_ERROR_DETECTED: Part {part_number} failed with timeout-related error: {e}")
                
                if attempt < retry_config['max_retries'] - 1:
                    delay = self._calculate_retry_delay(attempt, retry_config)
                    print(f"Part {part_number} failed ({type(e).__name__}), retrying in {delay:.2f}s (attempt {attempt + 1}/{retry_config['max_retries']})")
                    time.sleep(delay)
                    continue
                else:
                    # Log failed request
                    raise Exception(f"Part {part_number} failed permanently after {retry_config['max_retries']} attempts: {str(e)}")
                    
            except Exception as e:
                last_exception = e
                # Log failed request
                print(f"ERROR in send_file_part for part {part_number}: {str(e)}")
                raise
        
        # Should never reach here, but just in case
        raise Exception(f"Part {part_number} failed after all retry attempts: {str(last_exception)}")
    
    def _calculate_retry_delay(self, attempt: int, retry_config: dict) -> float:
        """Calculate exponential backoff delay with jitter"""
        base_delay = retry_config['initial_delay'] * (retry_config['exponential_base'] ** attempt)
        max_delay = min(base_delay, retry_config['max_delay'])
        
        # Add jitter to prevent thundering herd
        jitter_range = max_delay * (retry_config['jitter_percent'] / 100)
        jitter = random.uniform(-jitter_range, jitter_range)
        
        return max(0.1, max_delay + jitter)  # Minimum 0.1 second delay

    def complete_multipart_upload(self, file_upload_id: str, parts: List[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Complete the multipart upload
        
        Args:
            file_upload_id: The ID of the multipart upload to complete
            parts: Optional list of part objects with part_number and etag
            
        Returns:
            Dict with the completion response
            
        Raises:
            Exception if completion fails
        """
        url = None
        if file_upload_id.startswith('http'):
            # If file_upload_id is a full URL, use it directly
            url = f"{file_upload_id}/complete"
        else:
            # Otherwise construct the URL from base
            url = f"{self.base_url}/file_uploads/{file_upload_id}/complete"

        headers = {**self.headers, "Content-Type": "application/json"}
        
        # According to Notion API docs, we just need to call the complete endpoint
        # without any parts information - Notion tracks the parts internally
        # https://developers.notion.com/docs/sending-larger-files
        
        print(f"Completing multipart upload for ID: {file_upload_id}...")
        
        # Try multiple times if needed
        max_retries = 3
        retry_count = 0
        last_error = None
        
        while retry_count < max_retries:
            try:
                response = requests.post(url, headers=headers, json={})
                
                if response.status_code != 200:
                    error_msg = f"Failed to complete multipart upload: {response.text}"
                    print(f"ERROR (attempt {retry_count+1}/{max_retries}): {error_msg}")
                    last_error = Exception(error_msg)
                    
                    # Check if the error suggests missing parts
                    if "Expected" in response.text and "parts" in response.text:
                        import re
                        match = re.search(r'Send part number (\d+) next', response.text)
                        if match:
                            expected_part = int(match.group(1))
                            print(f"Notion API expects part {expected_part} next. Upload cannot be completed yet.")
                            raise Exception(f"Missing part {expected_part} in upload")
                    
                    # Wait before retrying
                    retry_count += 1
                    if retry_count < max_retries:
                        wait_time = 2 ** retry_count  # Exponential backoff
                        print(f"Waiting {wait_time} seconds before retry...")
                        import time
                        time.sleep(wait_time)
                        continue
                    else:
                        raise last_error
                else:
                    print("Multipart upload completed successfully")
                    return response.json()
                    
            except Exception as e:
                print(f"ERROR (attempt {retry_count+1}/{max_retries}): {str(e)}")
                last_error = e
                retry_count += 1
                
                if retry_count < max_retries:
                    wait_time = 2 ** retry_count  # Exponential backoff
                    print(f"Waiting {wait_time} seconds before retry...")
                    import time
                    time.sleep(wait_time)
                else:
                    break
        
        # If we get here, all retries failed
        error_msg = f"Failed to complete multipart upload after {max_retries} attempts: {str(last_error)}"
        print(f"ERROR: {error_msg}")
        raise Exception(error_msg)

    # Removed upload_single_file and upload_large_file_multipart as they are no longer needed.
    # The new upload_file_stream method handles both cases.

    def query_user_database_by_username(self, database_id: str, username: str) -> Dict[str, Any]:
        """Query the Notion user database for a specific username"""
        url = f"{self.base_url}/databases/{database_id}/query"

        payload = {
            "filter": {
                "property": "Name",
                "title": {
                    "equals": username
                }
            }
        }

        headers = {**self.headers, "Content-Type": "application/json"}

        response = requests.post(url, json=payload, headers=headers)

        if response.status_code != 200:
            raise Exception(f"User database query failed: {response.text}")

        return response.json()

    def create_user(self, database_id: str, username: str, password_hash: str) -> Dict[str, Any]:
        """Create a new user in the Notion database"""
        url = f"{self.base_url}/pages"

        # First verify the database exists and is accessible
        try:
            db_url = f"{self.base_url}/databases/{database_id}"
            db_response = requests.get(db_url, headers=self.headers)
            if db_response.status_code != 200:
                raise Exception(f"Database verification failed: {db_response.text}")
        except Exception as e:
            raise Exception(f"Failed to verify database: {str(e)}")

        # Prepare minimal required properties
        payload = {
            "parent": {"database_id": database_id},
            "properties": {
                "Name": {
                    "title": [{"text": {"content": username}}]
                },
                "Password-Hash": {
                    "rich_text": [{"text": {"content": password_hash}}]
                }
            }
        }

        headers = {
            **self.headers,
            "Content-Type": "application/json"
        }

        try:
            print(f"Creating user with payload: {payload}")  # Debug logging
            response = requests.post(url, json=payload, headers=headers)
            
            if response.status_code != 200:
                error_msg = f"Status: {response.status_code}\nResponse: {response.text}"
                print(f"User creation error: {error_msg}")  # Debug logging
                raise Exception(error_msg)
                
            return response.json()
        except Exception as e:
            raise Exception(f"User creation failed: {str(e)}")

    def ensure_database_property(self, database_id: str, property_name: str, property_type: str, property_config: Optional[Dict[str, Any]] = None) -> bool:
        """Ensure a property exists in a database, creating it if needed"""
        try:
            # First get the database schema
            url = f"{self.base_url}/databases/{database_id}"
            response = requests.get(url, headers=self.headers)
            
            if response.status_code != 200:
                raise Exception(f"Failed to get database schema: {response.text}")
                
            schema = response.json().get('properties', {})
            
            # If property already exists, return
            if property_name in schema:
                return True
                
            # Create the property with proper configuration based on type
            url = f"{self.base_url}/databases/{database_id}"
            
            # Handle relation properties specially
            if property_type == "relation":
                if not property_config or "database_id" not in property_config:
                    raise ValueError("Relation properties require a database_id in property_config")
                
                payload = {
                    "properties": {
                        property_name: {
                            "type": "relation",
                            "relation": property_config
                        }
                    }
                }
            else:
                payload = {
                    "properties": {
                        property_name: {
                            property_type: property_config if property_config is not None else {}
                        }
                    }
                }
            
            response = requests.patch(url, json=payload, headers=self.headers)
            
            if response.status_code != 200:
                raise Exception(f"Failed to create property: {response.text}")
            
            # Re-fetch the database schema to confirm the property is now present
            response = requests.get(url, headers=self.headers)
            if response.status_code != 200:
                raise Exception(f"Failed to re-fetch database schema after property creation: {response.text}")
            
            updated_schema = response.json().get('properties', {})
            if property_name in updated_schema:
                return True
            else:
                raise Exception(f"Property '{property_name}' was not found in database schema after creation attempt.")
                
        except Exception as e:
            print(f"Error ensuring database property: {e}")
            return False

    def _get_database_properties_map(self, database_id: str) -> Dict[str, Any]:
        """Helper to get a map of property names to their full property objects (including IDs)"""
        url = f"{self.base_url}/databases/{database_id}"
        response = requests.get(url, headers=self.headers)
        if response.status_code != 200:
            raise Exception(f"Failed to get database schema for properties map: {response.text}")
        return response.json().get('properties', {})

    def update_user_properties(self, user_id: str, properties: Dict[str, Any]) -> Dict[str, Any]:
        """Update multiple properties of a user's Notion page."""
        url = f"{self.base_url}/pages/{user_id}"

        try:
            # First get the current page and schema
            page = self.get_user_by_id(user_id)
            if not page:
                raise Exception("User page not found")
            
            schema = page.get('properties', {})
            print(f"Debug - Current schema: {schema}")  # Debug logging
            
            # Prepare payload with validated properties
            payload_properties = {}
            
            for prop_name, prop_value in properties.items():
                if prop_name not in schema:
                    print(f"Warning: Property {prop_name} not found in schema")
                    continue
                    
                prop_schema = schema[prop_name]
                prop_type = prop_schema.get('type')
                
                # Special handling for User Database property (can be URL or relation)
                if prop_name == 'User Database':
                    if isinstance(prop_value, dict) and 'url' in prop_value:
                        payload_properties[prop_name] = {
                            "type": "url",
                            "url": prop_value['url']
                        }
                    elif isinstance(prop_value, dict) and 'relation' in prop_value:
                        payload_properties[prop_name] = {
                            "type": "relation",
                            "relation": prop_value['relation']
                        }
                    else:
                        raise ValueError("User Database must include either url or relation data")
                else:
                    # For non-relation properties, use the value as-is
                    payload_properties[prop_name] = prop_value

            if not payload_properties:
                raise Exception("No valid properties to update")

            payload = {
                "properties": payload_properties
            }
            
            print(f"Debug - Update payload: {payload}")  # Debug logging

            headers = {
                **self.headers,
                "Content-Type": "application/json"
            }

            response = requests.patch(url, json=payload, headers=headers)
            
            if response.status_code != 200:
                error_msg = f"Status: {response.status_code}\nResponse: {response.text}"
                print(f"Debug - Update error: {error_msg}")  # Debug logging
                raise Exception(error_msg)
                
            return response.json()
            
        except Exception as e:
            error_details = f"{str(e)}"
            if hasattr(e, 'response') and e.response:
                error_details += f"\nResponse: {e.response.text}"
            raise Exception(f"Failed to update user properties: {error_details}")

    def update_user_password(self, user_id: str, new_password_hash: str) -> Dict[str, Any]:
        """Update the password hash for a user in the Notion database"""
        url = f"{self.base_url}/pages/{user_id}"

        payload = {
            "properties": {
                "Password-Hash": {
                    "type": "rich_text",
                    "rich_text": [
                        {
                            "type": "text",
                            "text": {
                                "content": new_password_hash
                            }
                        }
                    ]
                }
            }
        }

        headers = {**self.headers, "Content-Type": "application/json"}

        response = requests.patch(url, json=payload, headers=headers)

        if response.status_code != 200:
            raise Exception(f"Failed to update user password: {response.text}")

        return response.json()

    def get_user_by_id(self, user_id: str) -> Dict[str, Any]:
        """Get a user page by its ID with retry logic for temporary failures"""
        import time
        
        url = f"{self.base_url}/pages/{user_id}"
        headers = {**self.headers, "Content-Type": "application/json"}
        
        print(f"DEBUG: Making request to Notion API: {url}")
        print(f"DEBUG: Request headers: {headers}")

        max_retries = 3
        base_delay = 1  # Start with 1 second delay
        
        for attempt in range(max_retries + 1):
            try:
                response = requests.get(url, headers=headers, timeout=30)
                print(f"DEBUG: Attempt {attempt + 1}: Response status: {response.status_code}")
                print(f"DEBUG: Response headers: {dict(response.headers)}")
                
                if response.status_code == 200:
                    return response.json()
                elif response.status_code in [502, 503, 504] and attempt < max_retries:
                    # Temporary server errors - retry with exponential backoff
                    delay = base_delay * (2 ** attempt)
                    print(f"DEBUG: Server error {response.status_code}, retrying in {delay} seconds...")
                    time.sleep(delay)
                    continue
                else:
                    print(f"DEBUG: Response text: {response.text[:500]}")  # Limit response text for readability
                    raise Exception(f"Failed to get user by ID: HTTP {response.status_code} - {response.text}")
                    
            except requests.exceptions.RequestException as e:
                if attempt < max_retries:
                    delay = base_delay * (2 ** attempt)
                    print(f"DEBUG: Network error on attempt {attempt + 1}: {str(e)}, retrying in {delay} seconds...")
                    time.sleep(delay)
                    continue
                else:
                    print(f"DEBUG: Request exception after {max_retries + 1} attempts: {str(e)}")
                    raise Exception(f"Failed to get user by ID - Network error: {str(e)}")
            except Exception as e:
                print(f"DEBUG: Unexpected error: {str(e)}")
                raise
        
        # Should not reach here, but just in case
        raise Exception("Failed to get user by ID after all retry attempts")

    def get_user_database_id(self, user_id: str) -> Optional[str]:
        """
        Retrieves the user's dedicated Notion database ID from their user page.
        """
        try:
            user_data = self.get_user_by_id(user_id)
            if not user_data:
                print(f"User data not found for ID: {user_id}")
                return None

            # The 'User Database' property is expected to be a URL type
            user_db_property = user_data.get('properties', {}).get('User Database', {})
            user_db_url = user_db_property.get('url')

            if not user_db_url:
                print(f"User Database URL not found for user ID: {user_id}")
                return None

            # Extract the database ID from the URL
            # Notion database URLs are typically like: https://www.notion.so/DATABASE_ID_WITHOUT_HYPHENS
            # We need to convert it back to the hyphenated format.
            parsed_url = urllib.parse.urlparse(user_db_url)
            path_segments = [s for s in parsed_url.path.split('/') if s]
            
            if not path_segments:
                print(f"Could not parse database ID from URL: {user_db_url}")
                return None
            
            # The database ID is the last segment, without hyphens
            db_id_without_hyphens = path_segments[-1]
            
            # Convert to hyphenated format (8-4-4-4-12)
            hyphenated_db_id = (
                f"{db_id_without_hyphens[0:8]}-"
                f"{db_id_without_hyphens[8:12]}-"
                f"{db_id_without_hyphens[12:16]}-"
                f"{db_id_without_hyphens[16:20]}-"
                f"{db_id_without_hyphens[20:]}"
            )
            return hyphenated_db_id

        except Exception as e:
            print(f"Error getting user database ID for user {user_id}: {e}")
            return None

    # New methods for user-specific file storage and Notion database
    def create_user_database(self, parent_id: str, username: str) -> Dict[str, Any]:
        """Create a Notion database for a user to store file information"""
        url = f"{self.base_url}/databases"

        # Define the database schema with all required properties, only use file_data for file storage
        database_schema = {
            "parent": {"page_id": parent_id},
            "title": [
                {
                    "text": {
                        "content": f"{username}'s File Database"
                    }
                }
            ],
            "properties": {
                "filename": {
                    "title": {}
                },
                "filesize": {
                    "number": {
                        "format": "number"
                    }
                },
                "filehash": {
                    "rich_text": {}
                },
                "is_public": {
                    "checkbox": {}
                },
                "salt": {
                    "rich_text": {}
                },
                "is_visible": {
                    "checkbox": {}
                },
                "file_data": {
                    "files": {}
                }
            },
            "is_inline": True
        }

        headers = {**self.headers, "Content-Type": "application/json"}

        # Create the database and return URL
        response = requests.post(url, json=database_schema, headers=headers)
        if response.status_code != 200:
            raise Exception(f"Failed to create user database: {response.text}")

        result = response.json()
        # Ensure the URL is generated correctly for the database
        # Notion database URLs are typically like: https://www.notion.so/DATABASE_ID_WITHOUT_HYPHENS
        result['url'] = f"https://www.notion.so/{result['id'].replace('-', '')}"
        return result

    def get_files_from_user_database(self, database_id: str) -> Dict[str, Any]:
        """Queries a user's Notion database for all file entries."""
        url = f"{self.base_url}/databases/{database_id}/query"
        headers = {**self.headers, "Content-Type": "application/json"}
        
        try:
            response = requests.post(url, headers=headers)
            if response.status_code != 200:
                raise Exception(f"Failed to query user database: {response.text}")
            return response.json()
        except Exception as e:
            print(f"Error querying user database {database_id}: {e}")
            raise

    def delete_file_from_db(self, file_page_id: str) -> Dict[str, Any]:
        """Deletes a file entry (page) from a Notion database and the Global File Index."""
        url = f"{self.base_url}/pages/{file_page_id}"
        headers = {**self.headers, "Content-Type": "application/json"}
        
        try:
            # First, retrieve the file's hash before archiving it
            file_details = self.get_user_by_id(file_page_id)
            if not file_details:
                raise Exception(f"File details not found for page ID: {file_page_id}")
            
            salted_sha512_hash = file_details.get('properties', {}).get('filehash', {}).get('rich_text', [{}])[0].get('text', {}).get('content', '')
            
            if not salted_sha512_hash:
                print(f"Warning: Could not retrieve salted_sha512_hash for file_page_id: {file_page_id}. Skipping Global File Index deletion.")

            # Notion's delete is an archive operation
            payload = {"archived": True}
            response = requests.patch(url, json=payload, headers=headers)
            
            if response.status_code != 200:
                raise Exception(f"Failed to delete (archive) file from user database: {response.text}")
            
            # If hash was found, delete from Global File Index as well
            if salted_sha512_hash:
                self.delete_file_from_global_index(salted_sha512_hash)
                
            return response.json()
        except Exception as e:
            print(f"Error deleting file from database {file_page_id}: {e}")
            raise

    def delete_file_from_global_index(self, salted_sha512_hash: str) -> Optional[Dict[str, Any]]:
        """Deletes an entry from the Global File Index database based on the salted SHA512 hash."""
        global_index_db_id = self.global_file_index_db_id
        if not global_index_db_id:
            print("GLOBAL_FILE_INDEX_DB_ID not set. Cannot delete from Global File Index.")
            return None

        # 1. Query the Global File Index for the hash to get the page ID
        index_query_url = f"{self.base_url}/databases/{global_index_db_id}/query"
        index_payload = {
            "filter": {
                "property": "Salted SHA512 Hash",
                "rich_text": {
                    "equals": salted_sha512_hash
                }
            }
        }
        headers = {**self.headers, "Content-Type": "application/json"}

        try:
            index_response = requests.post(index_query_url, json=index_payload, headers=headers)
            if index_response.status_code != 200:
                print(f"Failed to query Global File Index for deletion: {index_response.text}")
                return None

            index_results = index_response.json().get('results', [])
            if not index_results:
                print(f"No entry found in Global File Index for hash: {salted_sha512_hash}. Nothing to delete.")
                return None

            # Get the page ID of the entry to be deleted
            index_entry_id = index_results[0]['id']
            
            # 2. Archive (delete) the page in the Global File Index
            delete_url = f"{self.base_url}/pages/{index_entry_id}"
            delete_payload = {"archived": True}
            delete_response = requests.patch(delete_url, json=delete_payload, headers=headers)

            if delete_response.status_code != 200:
                raise Exception(f"Failed to delete (archive) entry from Global File Index: {delete_response.text}")
            
            print(f"Successfully deleted entry from Global File Index for hash: {salted_sha512_hash}")
            return delete_response.json()

        except Exception as e:
            print(f"Error deleting file from Global File Index by hash {salted_sha512_hash}: {e}")
            raise

    def add_file_to_user_database(self, database_id: str, filename: str, file_size: int, file_hash: str, file_upload_id: str, is_public: bool = False, salt: str = "", original_filename: str = None, file_url: str = None) -> Dict[str, Any]:
        """Add a file entry to a user's Notion database with enhanced ID validation"""
        import traceback
        url = f"{self.base_url}/pages"

        # CRITICAL FIX 1: Enhanced ID Validation and Logging
        print(f"🔍 ADD_FILE_TO_DB: Starting with file_upload_id: {file_upload_id}")
        print(f"🔍 ADD_FILE_TO_DB: Parameter types - file_upload_id: {type(file_upload_id)}, database_id: {type(database_id)}")
        print(f"🔍 ADD_FILE_TO_DB: IMPORTANT - This file_upload_id should be used for Notion file operations, NOT database operations")

        # EXTRA LOGGING: Log filename, file_size, and call stack for every DB entry creation
        print(f"[EXTRA LOGGING] Creating DB entry: filename={filename}, file_size={file_size}, original_filename={original_filename}")
        print(f"[EXTRA LOGGING] Call stack:")
        traceback.print_stack()

        if not file_upload_id:
            error_msg = f"ID VALIDATION FAILED: file_upload_id is required but was null or empty. Received: {repr(file_upload_id)}"
            print(f"🚨 CRITICAL ERROR: {error_msg}")
            raise Exception(error_msg)

        # Additional validation for string content
        if isinstance(file_upload_id, str):
            if len(file_upload_id.strip()) == 0:
                error_msg = f"ID VALIDATION FAILED: file_upload_id is empty string. Length: {len(file_upload_id)}"
                print(f"🚨 CRITICAL ERROR: {error_msg}")
                raise Exception(error_msg)
            if file_upload_id.lower() in ['null', 'none', 'undefined', 'nan']:
                error_msg = f"ID VALIDATION FAILED: file_upload_id contains invalid value: {file_upload_id}"
                print(f"🚨 CRITICAL ERROR: {error_msg}")
                raise Exception(error_msg)

        print(f"🔍 ID VALIDATION PASSED: file_upload_id '{file_upload_id}' is valid for file operations")
        print(f"🔍 ID PURPOSE: This ID will be used in the file property for Notion file attachment")

        # Use original_filename if provided, otherwise fall back to filename
        display_filename = original_filename if original_filename else filename

        # Create a new page in the database with file information, only use file_data for file storage
        payload = {
            "parent": {"database_id": database_id},
            "properties": {
                "filename": {
                    "title": [
                        {
                            "text": {
                                "content": display_filename
                            }
                        }
                    ]
                },
                "filesize": {
                    "number": file_size
                },
                "filehash": {
                    "rich_text": [
                        {
                            "text": {
                                "content": file_hash
                            }
                        }
                    ]
                },
                "file_data": {
                    "files": [
                        {
                            "name": "file.txt",
                            "type": "file_upload",
                            "file_upload": {
                                "id": file_upload_id
                            }
                        }
                    ]
                },
                "is_public": {
                    "checkbox": is_public
                },
                "salt": {
                    "rich_text": [
                        {
                            "text": {
                                "content": salt
                            }
                        }
                    ]
                }
            }
        }

        headers = {**self.headers, "Content-Type": "application/json"}

        print(f"🔍 DATABASE OPERATION: Adding file to user database with upload ID: {file_upload_id}")
        print(f"🔍 DATABASE OPERATION: Original filename: {display_filename}")
        print(f"🔍 DATABASE OPERATION: Stored as: file.txt")
        print(f"🔍 DATABASE OPERATION: Database ID: {database_id}")
        print(f"🔍 DATABASE OPERATION: Payload file_upload_id: {payload['properties']['file_data']['files'][0]['file_upload']['id']}")
        # Verify payload integrity before sending
        payload_file_id = payload['properties']['file_data']['files'][0]['file_upload']['id']
        if payload_file_id != file_upload_id:
            error_msg = f"ID CORRUPTION: Payload file_upload_id '{payload_file_id}' does not match parameter '{file_upload_id}'"
            print(f"🚨 CRITICAL ERROR: {error_msg}")
            raise Exception(error_msg)

        response = requests.post(url, json=payload, headers=headers)

        if response.status_code != 200:
            error_msg = f"Failed to add file to user database: {response.text}"
            print(f"🚨 DATABASE ERROR: {error_msg}")
            print(f"🔍 DATABASE ERROR: Request payload: {payload}")
            raise Exception(error_msg)

        result = response.json()
        result_id = result.get('id')
        print(f"🔍 DATABASE SUCCESS: File added with database page ID: {result_id}")
        print(f"🔍 DATABASE SUCCESS: Response keys: {list(result.keys())}")
        print(f"🔍 ID SEPARATION COMPLETE:")
        print(f"  - File Upload ID (for Notion file operations): {file_upload_id}")
        print(f"  - Database Page ID (for database operations): {result_id}")
        print(f"  - These IDs serve different purposes and should never be confused")
        return result

    def get_file_by_salted_sha512_hash(self, salted_sha512_hash: str) -> Optional[Dict[str, Any]]:
        """
        Queries the Global File Index database for a file by its salted SHA512 hash,
        then fetches the full file details from the user's specific database.
        """
        global_index_db_id = self.global_file_index_db_id
        if not global_index_db_id:
            print("GLOBAL_FILE_INDEX_DB_ID not set in NotionFileUploader instance. Cannot query file index.")
            return None

        # 1. Query the Global File Index for the hash
        index_query_url = f"{self.base_url}/databases/{global_index_db_id}/query"
        index_payload = {
            "filter": {
                "property": "Salted SHA512 Hash",
                "rich_text": {
                    "equals": salted_sha512_hash
                }
            }
        }
        headers = {**self.headers, "Content-Type": "application/json"}

        try:
            index_response = requests.post(index_query_url, json=index_payload, headers=headers)
            if index_response.status_code != 200:
                print(f"Failed to query Global File Index: {index_response.text}")
                return None

            index_results = index_response.json().get('results', [])
            if not index_results:
                return None # Hash not found in index

            # Get the first matching entry from the index
            index_entry = index_results[0]
            properties = index_entry.get('properties', {})

            file_page_id = properties.get('File Page ID', {}).get('rich_text', [{}])[0].get('text', {}).get('content', '')
            user_database_id = properties.get('User Database ID', {}).get('rich_text', [{}])[0].get('text', {}).get('content', '')
            
            if not file_page_id or not user_database_id:
                print("Missing File Page ID or User Database ID in Global File Index entry.")
                return None

            # Instead of fetching the full file details, return the index entry itself
            # The caller (app.py) will then extract file_page_id and user_database_id
            return index_entry

        except Exception as e:
            print(f"Error getting file by salted SHA512 hash from index: {e}")
            return None

    def update_file_public_status(self, file_id: str, is_public: bool, salted_sha512_hash: str = None) -> Dict[str, Any]:
        """
        Updates the 'is_public' property of a file entry in the user's Notion database
        and optionally in the Global File Index.
        """
        url = f"{self.base_url}/pages/{file_id}"
        payload = {
            "properties": {
                "is_public": {
                    "checkbox": is_public
                }
            }
        }
        headers = {**self.headers, "Content-Type": "application/json"}

        try:
            response = requests.patch(url, json=payload, headers=headers)
            if response.status_code != 200:
                raise Exception(f"Failed to update file public status in user database: {response.text}")
            
            # Also update in Global File Index if hash is provided
            if salted_sha512_hash:
                global_index_db_id = self.global_file_index_db_id
                if global_index_db_id:
                    # Find the entry in the global index by hash
                    index_query_url = f"{self.base_url}/databases/{global_index_db_id}/query"
                    index_payload = {
                        "filter": {
                            "property": "Salted SHA512 Hash",
                            "rich_text": {
                                "equals": salted_sha512_hash
                            }
                        }
                    }
                    index_response = requests.post(index_query_url, json=index_payload, headers=headers)
                    if index_response.status_code == 200 and index_response.json().get('results'):
                        index_entry_id = index_response.json()['results'][0]['id']
                        index_update_url = f"{self.base_url}/pages/{index_entry_id}"
                        index_update_payload = {
                            "properties": {
                                "Is Public": {
                                    "checkbox": is_public
                                }
                            }
                        }
                        requests.patch(index_update_url, json=index_update_payload, headers=headers)
                        print(f"Updated is_public status in Global File Index for hash: {salted_sha512_hash}")
                    else:
                        print(f"Warning: Could not find entry in Global File Index for hash: {salted_sha512_hash} to update public status.")
                else:
                    print("Warning: GLOBAL_FILE_INDEX_DB_ID not set in NotionFileUploader instance. Cannot update public status in Global File Index.")

            return response.json()
        except Exception as e:
            print(f"Error updating file public status: {e}")
            raise

    def stream_file_from_notion(self, notion_download_url: str) -> Iterable[bytes]:
        """
        Streams file content from a Notion signed download URL.
        """
        try:
            with requests.get(notion_download_url, stream=True) as r:
                r.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
                for chunk in r.iter_content(chunk_size=8192): # 8KB chunks
                    yield chunk
        except requests.exceptions.RequestException as e:
            print(f"Error streaming file from Notion: {e}")
            raise Exception(f"Failed to stream file from Notion: {e}")

    def stream_file_from_notion_range(self, notion_download_url: str, start: int, end: int) -> Iterable[bytes]:
        """
        Streams a specific byte range of file content from a Notion signed download URL.
        
        Args:
            notion_download_url: The Notion signed download URL
            start: Starting byte position (inclusive)
            end: Ending byte position (inclusive)
            
        Returns:
            Iterator yielding file content chunks for the requested range
        """
        # Stream a specific byte range from a Notion signed download URL
        headers = {
            'Range': f'bytes={start}-{end}'
        }
        print(f"Requesting bytes {start}-{end} from Notion download URL: {notion_download_url}")
        try:
            with requests.get(notion_download_url, headers=headers, stream=True) as r:
                # Handle both 200 (full content) and 206 (partial content) responses
                if r.status_code == 206:
                    print(f"Received partial content response (206) for range {start}-{end}")
                elif r.status_code == 200:
                    print(f"Server doesn't support range requests, streaming full content and extracting range {start}-{end}")
                else:
                    r.raise_for_status()

                bytes_read = 0
                target_bytes = end - start + 1

                # If we got a 200 response, we need to skip bytes until we reach the start position
                if r.status_code == 200:
                    skip_bytes = start
                    for chunk in r.iter_content(chunk_size=8192):
                        if skip_bytes > 0:
                            if len(chunk) <= skip_bytes:
                                skip_bytes -= len(chunk)
                                continue
                            else:
                                chunk = chunk[skip_bytes:]
                                skip_bytes = 0
                        to_yield = min(len(chunk), target_bytes - bytes_read)
                        if to_yield <= 0:
                            break
                        yield chunk[:to_yield]
                        bytes_read += to_yield
                        if bytes_read >= target_bytes:
                            break
                else:
                    # 206 Partial Content or other range-supporting response
                    for chunk in r.iter_content(chunk_size=8192):
                        to_yield = min(len(chunk), target_bytes - bytes_read)
                        if to_yield <= 0:
                            break
                        yield chunk[:to_yield]
                        bytes_read += to_yield
                        if bytes_read >= target_bytes:
                            break
        except requests.exceptions.RequestException as e:
            print(f"Error streaming byte range from Notion: {e}")
            raise Exception(f"Failed to stream byte range from Notion: {e}")
    def add_file_to_index(self, salted_sha512_hash: str, file_page_id: str, user_database_id: str, original_filename: str, is_public: bool) -> Dict[str, Any]:
        """Adds an entry to the Global File Index database."""
        global_index_db_id = self.global_file_index_db_id
        if not global_index_db_id:
            raise Exception("GLOBAL_FILE_INDEX_DB_ID not set in NotionFileUploader instance. Global File Index database must be created manually.")

        url = f"{self.base_url}/pages"

        payload = {
            "parent": {"database_id": global_index_db_id},
            "properties": {
                "Salted SHA512 Hash": {
                    "rich_text": [{"text": {"content": salted_sha512_hash}}]
                },
                "File Page ID": {
                    "rich_text": [{"text": {"content": file_page_id}}]
                },
                "User Database ID": {
                    "rich_text": [{"text": {"content": user_database_id}}]
                },
                "Original Filename": {
                    "title": [{"text": {"content": original_filename}}]
                },
                "Is Public": {
                    "checkbox": is_public
                }
            }
        }

        headers = {**self.headers, "Content-Type": "application/json"}

        try:
            response = requests.post(url, json=payload, headers=headers)
            if response.status_code != 200:
                raise Exception(f"Failed to add file to Global File Index: {response.text}")
            print(f"✅ Added file to Global Index for {original_filename}")
            return response.json()
        except Exception as e:
            print(f"Error adding file to Global File Index: {e}")
            raise

    def handle_streaming_upload(self, stream: Iterable[bytes], total_size: int, upload_info: Dict[str, Any]) -> Dict[str, Any]:
        """Handle streaming upload by accumulating chunks efficiently with concurrent uploading"""
        chunk_size = 5 * 1024 * 1024  # 5MB chunks for Notion API
        current_chunk = io.BytesIO()
        current_size = 0
        part_number = 1
        pending_uploads = []

        # Create thread pool for concurrent uploads
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            try:
                for chunk in stream:
                    current_chunk.write(chunk)
                    current_size += len(chunk)

                    # When we have accumulated enough data for a full chunk
                    if current_size >= chunk_size:
                        # Upload the chunk in a separate thread
                        chunk_data = current_chunk.getvalue()
                        future = executor.submit(
                            self._upload_part,
                            upload_info['upload_url'],
                            upload_info['upload_id'],
                            part_number,
                            chunk_data
                        )
                        pending_uploads.append((part_number, future))
                        part_number += 1

                        # Reset buffer
                        current_chunk = io.BytesIO()
                        current_size = 0
                        
                        print(f"Queued part {part_number-1} for upload ({len(chunk_data)/1024/1024:.2f} MB)")

                # Upload any remaining data
                if current_size > 0:
                    chunk_data = current_chunk.getvalue()
                    future = executor.submit(
                        self._upload_part,
                        upload_info['upload_url'],
                        upload_info['upload_id'],
                        part_number,
                        chunk_data
                    )
                    pending_uploads.append((part_number, future))
                    print(f"Queued final part {part_number} for upload ({current_size/1024/1024:.2f} MB)")

                # Wait for all uploads to complete
                for pnum, future in pending_uploads:
                    try:
                        future.result()  # Just check for errors, don't store the result
                        print(f"Completed upload of part {pnum}")
                    except Exception as e:
                        print(f"Error uploading part {pnum}: {e}")
                        raise

                # Complete the multipart upload
                complete_result = self._complete_multipart_upload(
                    upload_info['upload_url'],
                    upload_info['upload_id'],
                    []  # Empty parts list as Notion handles this internally
                )

                return {
                    'file_upload_id': upload_info['upload_id'],  # Use the upload_id as the file_upload_id
                    'message': 'File uploaded successfully'
                }

            except Exception as e:
                print(f"Error in streaming upload: {e}")
                # Attempt to abort the multipart upload
                self._abort_multipart_upload(upload_info['upload_url'], upload_info['upload_id'])
                raise

    def upload_file_stream(self, stream: Iterable[bytes], filename: str, user_id: str, total_size: int, 
                          existing_upload_info: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Handle file upload with proper streaming support"""
        try:
            if existing_upload_info:
                # Use existing multipart upload info
                print("Using existing multipart upload info")
                return self.handle_streaming_upload(stream, total_size, existing_upload_info)
            else:
                # Create new upload for small files
                print(f"Creating single-part upload for {filename}")
                upload_info = self.create_file_upload(self.get_mime_type(filename))
                return self.handle_streaming_upload(stream, total_size, upload_info)
        except Exception as e:
            print(f"Error in upload_file_stream: {e}")
            raise

    def _upload_part(self, upload_url: str, upload_id: str, part_number: int, chunk_data: bytes) -> requests.Response:
        """Upload a single part of a multipart upload"""
        url = f"{upload_url}/send"
        headers = {
            'Authorization': self.headers['Authorization'],
            'Notion-Version': self.headers['Notion-Version']
        }
        files = {
            'file': ('file.txt', chunk_data, 'text/plain'),
            'part_number': (None, str(part_number))  # Make sure part number is correct
        }
        
        # Calculate total parts from the total size and chunk size
        chunk_size = 5 * 1024 * 1024  # 5MB chunk size
        total_parts = len(self.upload_futures) + 1  # Current queued parts plus this one
        chunk_size_mb = len(chunk_data) / (1024*1024)
        print(f"Uploading part {part_number} of {total_parts} ({chunk_size_mb:.2f} MB)...")
        response = requests.post(url, headers=headers, files=files)
        if response.status_code != 200:
            raise Exception(f"Part {part_number} upload failed: {response.text}")
            
        # No need to store ETags or part numbers, Notion handles this internally
        return response

    def _complete_multipart_upload(self, upload_url: str, upload_id: str, parts: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Complete a multipart upload"""
        url = f"{upload_url}/complete"  # Base URL is already properly formatted
        headers = {**self.headers, "Content-Type": "application/json"}
        
        print("Completing multipart upload...")
        response = requests.post(url, headers=headers, json={})  # Notion API expects an empty body
        if response.status_code != 200:
            raise Exception(f"Failed to complete multipart upload: {response.text}")
        return response.json()

    def _abort_multipart_upload(self, upload_url: str, upload_id: str):
        """Abort a multipart upload in case of failure"""
        try:
            url = f"{upload_url}/cancel"  # Base URL is already properly formatted
            headers = {**self.headers, "Content-Type": "application/json"}
            requests.post(url, headers=headers)
            print(f"Successfully aborted multipart upload {upload_id}")
        except Exception as e:
            print(f"Failed to abort multipart upload {upload_id}: {e}")