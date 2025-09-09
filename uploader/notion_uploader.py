from flask import Flask, request, jsonify, Response, stream_with_context
import os
import requests
import urllib.parse
import threading
import io
import concurrent.futures
import queue
import tempfile
from typing import Dict, Any, List, Optional, Tuple, Union, Iterable
from flask_socketio import SocketIO
import uuid
import time
import random
import mimetypes
from .s3_downloader import (
    download_file_from_url,
    stream_file_from_url,
    stream_file_range_from_url,
)
import os


def _fetch_json(url: str):
    """Retrieve JSON from a URL, using the S3 downloader when applicable."""
    import json
    if 'amazonaws.com' in url:
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp_path = tmp.name
        try:
            download_file_from_url(url, tmp_path)
            with open(tmp_path, 'rb') as f:
                return json.load(f)
        finally:
            os.remove(tmp_path)
    else:
        resp = requests.get(url)
        resp.raise_for_status()
        return resp.json() if resp.headers.get('content-type','').startswith('application/json') else json.loads(resp.content)


# Default chunk size for downloading files from Notion (1 MiB by default).
# Controlled via the DOWNLOAD_CHUNK_SIZE env var (supports 1MiB, 5MB, etc.).
def _parse_size(value: str, default_bytes: int) -> int:
    if not value:
        return default_bytes
    s = value.strip().lower()
    if s.isdigit():
        try:
            return int(s)
        except Exception:
            return default_bytes
    num = ""
    unit = ""
    for ch in s:
        if ch.isdigit() or ch == ".":
            num += ch
        else:
            unit += ch
    try:
        fnum = float(num) if "." in num else int(num)
    except Exception:
        return default_bytes
    unit = unit.strip()
    units = {
        "b": 1,
        "kb": 1000,
        "kib": 1024,
        "mb": 1000 * 1000,
        "mib": 1024 * 1024,
        "gb": 1000 * 1000 * 1000,
        "gib": 1024 * 1024 * 1024,
        "m": 1000 * 1000,
        "mi": 1024 * 1024,
        "g": 1000 * 1000 * 1000,
    }
    mult = units.get(unit)
    if mult is None:
        return default_bytes
    return int(fnum * mult)

def _clamp(val: int, min_v: int, max_v: int) -> int:
    return max(min_v, min(max_v, val))

DOWNLOAD_CHUNK_SIZE = _parse_size(os.getenv("DOWNLOAD_CHUNK_SIZE", str(1 * 1024 * 1024)), 1 * 1024 * 1024)

# Define a shared default multipart chunk size (in bytes).
# Notion's S3-compatible multipart uploads typically require parts of at least 5 MiB.
# Keep this consistent with ChunkProcessor's logic and allow overrides via env.
_MIN_MP = 5 * 1024 * 1024
_MAX_MP = 20 * 1024 * 1024
MULTIPART_CHUNK_SIZE_BYTES = _clamp(
    _parse_size(os.getenv("NOTION_MULTIPART_CHUNK_SIZE", str(_MIN_MP)), _MIN_MP),
    _MIN_MP,
    _MAX_MP,
)

class ChunkProcessor:
    def __init__(self, max_concurrent_uploads=3, max_pending_chunks=5):
        self.chunk_buffer = io.BytesIO()
        self.buffer_size = 0
        _MIN_MP = 5 * 1024 * 1024
        _MAX_MP = 20 * 1024 * 1024
        self.chunk_size = _clamp(_parse_size(os.getenv("NOTION_MULTIPART_CHUNK_SIZE", str(_MIN_MP)), _MIN_MP), _MIN_MP, _MAX_MP)
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
    def delete_file_from_user_database(self, file_page_id: str) -> Dict[str, Any]:
        """Alias for delete_file_from_db for compatibility with streaming uploader."""
        return self.delete_file_from_db(file_page_id)

    def delete_file_from_index(self, file_page_id: str) -> Optional[Dict[str, Any]]:
        """Alias for delete_file_from_global_index for compatibility with streaming uploader. Accepts file_page_id, retrieves hash, and deletes from global index if possible."""
        # Try to get the hash from the file_page_id
        file_details = self.get_user_by_id(file_page_id)
        if not file_details:
            print(f"[delete_file_from_index] File details not found for page ID: {file_page_id}")
            return None
        salted_sha512_hash = file_details.get('properties', {}).get('filehash', {}).get('rich_text', [{}])[0].get('text', {}).get('content', '')
        if not salted_sha512_hash:
            print(f"[delete_file_from_index] Could not retrieve salted_sha512_hash for file_page_id: {file_page_id}. Skipping Global File Index deletion.")
            return None
        return self.delete_file_from_global_index(salted_sha512_hash)
    def stream_multi_part_file(self, manifest_page_id: str, start: int = 0, end: Optional[int] = None) -> Iterable[bytes]:
        """
        Streams a multi-part file described by a manifest JSON stored in Notion.
        Supports optional byte range streaming so clients can request only
        specific portions of the file.

        Args:
            manifest_page_id: The Notion page ID containing the manifest JSON.
            start: Starting byte position (inclusive).
            end: Ending byte position (inclusive). If None, stream until the end.

        Yields:
            File bytes in the requested range.
        """
        import json
        # 1. Fetch the manifest JSON from Notion (as a file attachment)
        manifest_page = self.get_user_by_id(manifest_page_id)
        if not manifest_page:
            raise Exception(f"Manifest page not found: {manifest_page_id}")
        file_property = manifest_page.get('properties', {}).get('file_data', {})
        files_array = file_property.get('files', [])
        if not files_array:
            raise Exception(f"No files found in 'file_data' for manifest page {manifest_page_id}")
        # Assume the first file is the manifest JSON
        manifest_file = files_array[0]
        manifest_url = manifest_file.get('file', {}).get('url', '')
        if not manifest_url:
            raise Exception(f"No valid URL for manifest file on page {manifest_page_id}")
        # Download the manifest JSON
        manifest = _fetch_json(manifest_url)
        parts = manifest.get('parts', [])
        if not parts:
            raise Exception(f"Manifest JSON does not contain 'parts' array")

        total_size = sum(part.get('size', 0) for part in parts)
        if end is None or end >= total_size:
            end = total_size - 1
        if start < 0 or start > end:
            raise Exception("Invalid byte range requested")

        # The original implementation resolved *all* part metadata before
        # starting to stream any bytes.  Large files can consist of hundreds of
        # parts which led to significant startup delays.  Instead we lazily
        # resolve part information and prefetch only a small window of upcoming
        # parts.  This allows the first bytes to be sent to the client as soon
        # as the first part is located.

        parts = sorted(parts, key=lambda p: p.get('part_number', 0))
        part_index = 0
        current_pos = 0

        def resolve_next_part() -> Optional[Dict[str, Any]]:
            nonlocal part_index, current_pos
            while part_index < len(parts):
                part = parts[part_index]
                part_size = part.get('size', 0)
                part_end_pos = current_pos + part_size - 1
                if part_end_pos < start:
                    current_pos += part_size
                    part_index += 1
                    continue

                file_hash = part.get('file_hash')
                part_filename = part.get('filename')
                if not file_hash:
                    raise Exception(f"Part missing file_hash: {part}")

                index_entry = self.get_file_by_salted_sha512_hash(file_hash)
                if not index_entry:
                    raise Exception(f"File part with hash {file_hash} not found in global index")
                part_page_id = index_entry.get('properties', {}).get('File Page ID', {}).get('rich_text', [{}])[0].get('text', {}).get('content', '')
                if not part_page_id:
                    raise Exception(f"No File Page ID for part hash {file_hash}")

                part_start = 0
                part_end = part_size - 1
                if start > current_pos:
                    part_start = start - current_pos
                if end < part_end_pos:
                    part_end = end - current_pos

                current_pos += part_size
                part_index += 1

                return {
                    'page_id': part_page_id,
                    'filename': part_filename,
                    'size': part_size,
                    'start': part_start,
                    'end': part_end,
                }
            return None

        prefetch_count = int(os.getenv("STREAM_PREFETCH_COUNT", "1"))
        prefetch_count = max(1, prefetch_count)
        streams: List[Tuple[queue.Queue, threading.Thread]] = []

        def start_prefetch(part_info: Dict[str, Any]) -> None:
            q: queue.Queue = queue.Queue(maxsize=4)  # small buffer per part

            def worker() -> None:
                try:
                    if part_info['start'] > 0 or part_info['end'] < part_info['size'] - 1:
                        iterator = self.stream_file_from_notion_range(
                            part_info['page_id'],
                            part_info['filename'],
                            part_info['start'],
                            part_info['end'],
                        )
                    else:
                        iterator = self.stream_file_from_notion(
                            part_info['page_id'], part_info['filename']
                        )
                    for chunk in iterator:
                        q.put(chunk)
                    q.put(None)  # Signal completion
                except Exception as e:  # Propagate errors to consumer
                    q.put(e)

            t = threading.Thread(target=worker, daemon=True)
            t.start()
            streams.append((q, t))

        # Prime the prefetch window
        for _ in range(prefetch_count):
            part_info = resolve_next_part()
            if not part_info:
                break
            start_prefetch(part_info)

        while streams:
            q, t = streams[0]
            while True:
                chunk = q.get()
                if chunk is None:
                    break
                if isinstance(chunk, Exception):
                    raise chunk
                yield chunk

            streams.pop(0)
            t.join()

            next_part = resolve_next_part()
            if next_part:
                start_prefetch(next_part)
    def __init__(self, api_token: str, socketio: SocketIO = None, notion_version: str = "2022-06-28",
                 global_file_index_db_id: str = None):
        self.api_token = api_token
        self.socketio = socketio
        self.base_url = "https://api.notion.com/v1"
        self.notion_version = notion_version
        self.headers = {
            "Authorization": f"Bearer {api_token}",
            "Notion-Version": notion_version,
            "accept": "application/json"
        }
        self.session = requests.Session()
        self.session.headers.update({**self.headers, "Connection": "keep-alive"})
        self.global_file_index_db_id = global_file_index_db_id
        
        # Validation configuration
        self.validation_config = {
            'enabled': True,                    # Enable/disable validation entirely
            'validation_threshold': 1800,      # Only validate URLs with less than 30 min left (seconds)
            'urgent_threshold': 300,           # Urgent validation threshold - 5 min (seconds)
            'bypass_fresh_urls': True,         # Skip validation for fresh URLs (>30 min left)
            'timeout': 10,                     # Validation request timeout (seconds)
            'accept_server_errors': True,      # Treat server errors (5xx) as valid
            'accept_auth_errors': True,        # Treat auth errors (403, 405) as valid
            'fallback_on_error': True          # Deprecated; caching disabled
        }

        # Cache for Global File Index lookups to avoid repeated queries for the
        # same file part. Entries are cached for 30 minutes similar to the
        # previous download URL cache.
        self.index_cache: Dict[str, Dict[str, Any]] = {}
        self.index_cache_lock = threading.Lock()
        self.index_cache_ttl = 1800  # 30 minutes
        self._start_index_cache_cleaner()

    def _start_index_cache_cleaner(self) -> None:
        """Start background thread to remove expired Global File Index cache entries."""
        thread = threading.Thread(target=self._index_cache_cleaner, daemon=True)
        thread.start()

    def _index_cache_cleaner(self) -> None:
        """Background worker that clears expired entries from the Global File Index cache."""
        while True:
            now = time.time()
            with self.index_cache_lock:
                keys_to_remove = [k for k, v in self.index_cache.items() if v['expires_at'] <= now]
                for key in keys_to_remove:
                    del self.index_cache[key]
            time.sleep(min(self.index_cache_ttl, 300))

    def invalidate_index_cache(self, salted_sha512_hash: str) -> None:
        """Remove a specific entry from the Global File Index cache."""
        with self.index_cache_lock:
            if salted_sha512_hash in self.index_cache:
                del self.index_cache[salted_sha512_hash]

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
        headers = {**self.headers, "Content-Type": "application/json", "Connection": "keep-alive"}
        
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
        
        response = self.session.post(url, headers=headers, json=payload)
        
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
            'Connection': 'keep-alive'
        }

        print(f"Uploading file content for {filename} with content type: {content_type}...")
        response = self.session.post(url, files=files, headers=headers)

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
        """Return a fresh signed download URL for the given file."""
        metadata = self.get_file_download_metadata(page_id, original_filename)
        return metadata.get('url', '')

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

            # Extract file information from the 'file_data' property (new name)
            file_property = page_info.get('properties', {}).get('file_data', {})
            files_array = file_property.get('files', [])

            if not files_array:
                print(f"No files found in 'file_data' property for page ID: {page_id}")
                return "", 0, "application/octet-stream"

            # Try to match by original filename if possible
            file_info = None
            for entry in files_array:
                if entry.get('name') == original_filename or entry.get('name') == 'file.txt':
                    file_info = entry
                    break
            if not file_info:
                file_info = files_array[0]  # fallback to first if not found

            file_url = file_info.get('file', {}).get('url', '')

            if not file_url:
                print(f"No valid URL found in file_data property for page ID: {page_id}")
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


    def get_file_download_metadata(self, page_id: str, original_filename: str,
                                    force_refresh: bool = False) -> Dict[str, Any]:
        """Fetch a fresh signed URL and related metadata for the given file."""
        try:
            fresh_url, file_size, content_type = self._fetch_fresh_download_url_with_metadata(
                page_id, original_filename)

            return {
                'url': fresh_url or '',
                'file_size': file_size,
                'content_type': content_type or 'application/octet-stream',
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
        Legacy wrapper that returns a fresh download URL for the given file.
        """
        return self.get_download_url_for_file(page_id, original_filename)

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

        single_threshold = _parse_size(os.getenv("NOTION_SINGLE_PART_THRESHOLD", str(20 * 1024 * 1024)), 20 * 1024 * 1024)
        if total_size <= single_threshold:  # Single-part limit
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

    def upload_large_file_multipart_stream(self, file_stream: Iterable[bytes], filename: str, database_id: str, content_type: str, file_size: int, original_filename: str, chunk_size: int = MULTIPART_CHUNK_SIZE_BYTES, existing_upload_info: Dict[str, Any] = None) -> Dict[str, Any]:
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

    def ensure_database_property(
        self,
        database_id: str,
        property_name: str,
        property_type: str,
        property_config: Optional[Dict[str, Any]] = None,
        default_value: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Ensure a property exists in a database, creating it if needed.

        If ``default_value`` is provided and the property is newly created, the
        value will be applied to all existing pages in the database. This helps
        when adding new checkbox fields like ``is_visible`` which should default
        to ``True`` for existing entries.
        """
        try:
            # First get the database schema
            url = f"{self.base_url}/databases/{database_id}"
            response = requests.get(url, headers=self.headers)

            if response.status_code != 200:
                raise Exception(f"Failed to get database schema: {response.text}")

            schema = response.json().get('properties', {})

            # If property already exists, nothing else to do
            if property_name in schema:
                return True

            # Create the property with proper configuration based on type
            if property_type == "relation":
                if not property_config or "database_id" not in property_config:
                    raise ValueError("Relation properties require a database_id in property_config")

                payload = {
                    "properties": {
                        property_name: {
                            "type": "relation",
                            "relation": property_config,
                        }
                    }
                }
            else:
                payload = {
                    "properties": {
                        property_name: {
                            property_type: property_config if property_config is not None else {},
                        }
                    }
                }

            response = requests.patch(url, json=payload, headers=self.headers)

            if response.status_code != 200:
                raise Exception(f"Failed to create property: {response.text}")

            # Re-fetch the database schema to confirm the property is now present
            response = requests.get(url, headers=self.headers)
            if response.status_code != 200:
                raise Exception(
                    f"Failed to re-fetch database schema after property creation: {response.text}"
                )

            updated_schema = response.json().get('properties', {})
            if property_name in updated_schema:
                if default_value is not None:
                    self._backfill_database_property(
                        database_id, property_name, default_value
                    )
                return True
            else:
                raise Exception(
                    f"Property '{property_name}' was not found in database schema after creation attempt."
                )

        except Exception as e:
            print(f"Error ensuring database property: {e}")
            return False

    def _backfill_database_property(
        self, database_id: str, property_name: str, property_value: Dict[str, Any]
    ) -> None:
        """Set a property's value for all existing pages in a database."""
        query_url = f"{self.base_url}/databases/{database_id}/query"
        headers = {**self.headers, "Content-Type": "application/json"}
        payload: Dict[str, Any] = {}

        try:
            while True:
                resp = requests.post(query_url, json=payload, headers=headers)
                if resp.status_code != 200:
                    print(
                        f"Error querying database for backfill of '{property_name}': {resp.text}"
                    )
                    return
                data = resp.json()
                for page in data.get("results", []):
                    page_id = page.get("id")
                    update_url = f"{self.base_url}/pages/{page_id}"
                    update_payload = {
                        "properties": {property_name: property_value}
                    }
                    upd_resp = requests.patch(
                        update_url, json=update_payload, headers=headers
                    )
                    if upd_resp.status_code != 200:
                        print(
                            f"Error updating page {page_id} for backfill of '{property_name}': {upd_resp.text}"
                        )
                if not data.get("has_more"):
                    break
                payload["start_cursor"] = data.get("next_cursor")
        except Exception as e:
            print(f"Error backfilling property '{property_name}': {e}")

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

    def update_user_username(self, user_id: str, new_username: str) -> Dict[str, Any]:
        """Update the username for a user in the Notion database"""
        url = f"{self.base_url}/pages/{user_id}"

        payload = {
            "properties": {
                "Name": {
                    "title": [
                        {
                            "text": {"content": new_username}
                        }
                    ]
                }
            }
        }

        headers = {**self.headers, "Content-Type": "application/json"}

        response = requests.patch(url, json=payload, headers=headers)

        if response.status_code != 200:
            raise Exception(f"Failed to update username: {response.text}")

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
                "is_folder": {
                    "checkbox": {}
                },
                "folder_path": {
                    "rich_text": {}
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
        """Queries a user's Notion database for all file entries.

        The Notion API returns results in pages (maximum 100 per request).
        Previously this method only fetched the first page which meant users
        with more than 100 files couldn't see their entire collection. This
        method now follows pagination cursors until all results have been
        retrieved and returns a single combined response.
        """
        url = f"{self.base_url}/databases/{database_id}/query"
        headers = {**self.headers, "Content-Type": "application/json"}
        all_results: List[Dict[str, Any]] = []
        payload: Dict[str, Any] = {}

        try:
            while True:
                response = requests.post(url, headers=headers, json=payload)
                if response.status_code != 200:
                    raise Exception(f"Failed to query user database: {response.text}")

                data = response.json()
                all_results.extend(data.get('results', []))

                if not data.get('has_more'):
                    break

                # Prepare next request with start_cursor
                payload['start_cursor'] = data.get('next_cursor')

            return {
                "object": "list",
                "results": all_results,
                "next_cursor": None,
                "has_more": False
            }
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

    def add_file_to_user_database(self, database_id: str, filename: str, file_size: int, file_hash: str, file_upload_id: str, is_public: bool = False, salt: str = "", original_filename: str = None, file_url: str = None, is_manifest: bool = False, folder_path: str = "/") -> Dict[str, Any]:
        """Add a file entry to a user's Notion database with enhanced ID validation"""
        url = f"{self.base_url}/pages"

        # Ensure the is_folder property exists for this database
        self.ensure_database_property(database_id, "is_folder", "checkbox")
        self.ensure_database_property(database_id, "password_hash", "rich_text")
        self.ensure_database_property(database_id, "expires_at", "date")

        # CRITICAL FIX 1: Enhanced ID Validation and Logging
        print(f"🔍 ADD_FILE_TO_DB: Starting with file_upload_id: {file_upload_id}")
        print(f"🔍 ADD_FILE_TO_DB: Parameter types - file_upload_id: {type(file_upload_id)}, database_id: {type(database_id)}")
        print(f"🔍 ADD_FILE_TO_DB: IMPORTANT - This file_upload_id should be used for Notion file operations, NOT database operations")

        # EXTRA LOGGING: Log filename, file_size, and call stack for every DB entry creation
        print(f"[EXTRA LOGGING] Creating DB entry: filename={filename}, file_size={file_size}, original_filename={original_filename}")
        print(f"[EXTRA LOGGING] Call stack:")

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
        properties = {
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
            },
            "folder_path": {
                "rich_text": [
                    {
                        "text": {
                            "content": folder_path
                        }
                    }
                ]
            },
            "is_folder": {
                "checkbox": False
            }
        }
        # Add is_manifest property if this is a manifest entry
        if is_manifest:
            properties["is_manifest"] = {"checkbox": True}

        payload = {
            "parent": {"database_id": database_id},
            "properties": properties
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

    def get_file_by_salted_sha512_hash(self, salted_sha512_hash: str, force_refresh: bool = False) -> Optional[Dict[str, Any]]:
        """
        Queries the Global File Index database for a file by its salted SHA512 hash,
        then fetches the full file details from the user's specific database.

        Args:
            salted_sha512_hash: The salted hash identifying the file.
            force_refresh: If ``True``, bypass the in-memory cache and fetch
                the latest data from Notion.  This ensures public/private
                status checks always use fresh data.
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
            now = time.time()
            with self.index_cache_lock:
                cached = None if force_refresh else self.index_cache.get(salted_sha512_hash)
                if cached and cached['expires_at'] > now:
                    return cached['entry']

            index_response = self.session.post(index_query_url, json=index_payload, headers=headers)
            if index_response.status_code != 200:
                print(f"Failed to query Global File Index: {index_response.text}")
                return None

            index_results = index_response.json().get('results', [])
            if not index_results:
                return None  # Hash not found in index

            index_entry = index_results[0]
            properties = index_entry.get('properties', {})

            file_page_id = properties.get('File Page ID', {}).get('rich_text', [{}])[0].get('text', {}).get('content', '')
            user_database_id = properties.get('User Database ID', {}).get('rich_text', [{}])[0].get('text', {}).get('content', '')

            if not file_page_id or not user_database_id:
                print("Missing File Page ID or User Database ID in Global File Index entry.")
                return None

            # Fetch security properties from the user's database
            try:
                file_page = self.get_user_by_id(file_page_id)
                file_props = file_page.get('properties', {})
                password_prop = file_props.get('password_hash', {})
                expires_prop = file_props.get('expires_at', {})
                index_entry['properties']['password_hash'] = password_prop
                index_entry['properties']['expires_at'] = expires_prop
            except Exception as e:
                print(f"Warning: Failed to fetch security properties for file page {file_page_id}: {e}")

            with self.index_cache_lock:
                self.index_cache[salted_sha512_hash] = {
                    'entry': index_entry,
                    'expires_at': now + self.index_cache_ttl
                }

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
            if salted_sha512_hash:
                self.invalidate_index_cache(salted_sha512_hash)

            return response.json()
        except Exception as e:
            print(f"Error updating file public status: {e}")
            raise

    def update_file_metadata(self, file_id: str, filename: str = None, folder_path: str = None) -> Dict[str, Any]:
        """Update filename or folder path for a file entry."""
        url = f"{self.base_url}/pages/{file_id}"
        properties = {}

        if filename is not None:
            properties["filename"] = {
                "title": [{"text": {"content": filename}}]
            }

        if folder_path is not None:
            properties["folder_path"] = {
                "rich_text": [{"text": {"content": folder_path}}]
            }

        if not properties:
            return {}

        payload = {"properties": properties}
        headers = {**self.headers, "Content-Type": "application/json"}

        response = requests.patch(url, json=payload, headers=headers)
        if response.status_code != 200:
            raise Exception(f"Failed to update file metadata: {response.text}")
        return response.json()

    def update_file_security_settings(
        self,
        file_id: str,
        password_hash: str = None,
        expires_at: str = None,
        salted_sha512_hash: str = None,
    ) -> Dict[str, Any]:
        """Update security-related properties for a file entry.

        Optionally invalidate the cached index entry corresponding to
        ``salted_sha512_hash`` so that subsequent lookups reflect the
        new security settings immediately.
        """
        url = f"{self.base_url}/pages/{file_id}"
        properties: Dict[str, Any] = {}

        if password_hash is not None:
            properties["password_hash"] = {
                "rich_text": [{"text": {"content": password_hash}}]
            }
        if expires_at is not None:
            properties["expires_at"] = {
                "date": {"start": expires_at}
            }

        if not properties:
            return {}

        payload = {"properties": properties}
        headers = {**self.headers, "Content-Type": "application/json"}

        response = requests.patch(url, json=payload, headers=headers)
        if response.status_code != 200:
            raise Exception(f"Failed to update file security settings: {response.text}")

        if salted_sha512_hash:
            self.invalidate_index_cache(salted_sha512_hash)

        return response.json()

    def create_folder(self, database_id: str, folder_name: str, parent_path: str = "/") -> Dict[str, Any]:
        """Create a folder entry in the user's database."""
        url = f"{self.base_url}/pages"

        properties = {
            "filename": {
                "title": [
                    {"text": {"content": folder_name}}
                ]
            },
            "filesize": {"number": 0},
            "filehash": {"rich_text": [{"text": {"content": ""}}]},
            "file_data": {"files": []},
            "is_public": {"checkbox": False},
            "salt": {"rich_text": [{"text": {"content": ""}}]},
            "folder_path": {"rich_text": [{"text": {"content": parent_path}}]},
            "is_folder": {"checkbox": True},
            "is_visible": {"checkbox": True}
        }

        payload = {"parent": {"database_id": database_id}, "properties": properties}
        headers = {**self.headers, "Content-Type": "application/json"}

        response = requests.post(url, json=payload, headers=headers)
        if response.status_code != 200:
            raise Exception(f"Failed to create folder: {response.text}")
        return response.json()

    def stream_file_from_notion(
        self,
        page_id: str,
        original_filename: str,
        download_url: Optional[str] = None,
        chunk_size: int = DOWNLOAD_CHUNK_SIZE,
    ) -> Iterable[bytes]:
        """Stream file content from Notion.

        Args:
            page_id: The Notion page ID where the file is stored.
            original_filename: The original filename to match in the file property.
            download_url: Optional pre-fetched download URL. If provided, the
                method will skip fetching a new URL from Notion.
            chunk_size: Size of chunks to download in bytes. Defaults to
                ``DOWNLOAD_CHUNK_SIZE`` (1 MiB).

        Yields:
            Iterator of file content chunks.
        """
        notion_download_url = download_url or self.get_notion_file_url_from_page_property(page_id, original_filename)
        if not notion_download_url:
            raise Exception(
                f"Could not find AWS S3 signed URL for file '{original_filename}' on page '{page_id}'"
            )
        yield from stream_file_from_url(notion_download_url, chunk_size=chunk_size)

    def stream_file_from_notion_range(
        self,
        page_id: str,
        original_filename: str,
        start: int,
        end: int,
        download_url: Optional[str] = None,
        chunk_size: int = DOWNLOAD_CHUNK_SIZE,
    ) -> Iterable[bytes]:
        """Stream a specific byte range of file content from Notion.

        Args:
            page_id: The Notion page ID where the file is stored.
            original_filename: The original filename to match in the file property.
            start: Starting byte position (inclusive).
            end: Ending byte position (inclusive).
            download_url: Optional pre-fetched download URL.
            chunk_size: Size of chunks to download in bytes. Defaults to
                ``DOWNLOAD_CHUNK_SIZE`` (1 MiB).

        Yields:
            Iterator of file content chunks for the requested range.
        """
        notion_download_url = download_url or self.get_notion_file_url_from_page_property(page_id, original_filename)
        if not notion_download_url:
            raise Exception(
                f"Could not find AWS S3 signed URL for file '{original_filename}' on page '{page_id}'"
            )
        yield from stream_file_range_from_url(
            notion_download_url, start, end, chunk_size=chunk_size
        )
    def add_file_to_index(self, salted_sha512_hash: str, file_page_id: str, user_database_id: str, original_filename: str, is_public: bool, password_hash: str = None, expires_at: str = None) -> Dict[str, Any]:
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

        if password_hash is not None:
            payload["properties"]["Password Hash"] = {
                "rich_text": [{"text": {"content": password_hash}}]
            }
        if expires_at is not None:
            payload["properties"]["Expires At"] = {
                "date": {"start": expires_at}
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
        chunk_size = self.chunk_size  # Configured Notion multipart chunk size
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
            'Notion-Version': self.headers['Notion-Version'],
            'Connection': 'keep-alive'
        }
        files = {
            'file': ('file.txt', chunk_data, 'text/plain'),
            'part_number': (None, str(part_number))  # Make sure part number is correct
        }
        
        # Calculate total parts from the total size and chunk size
        chunk_size = self.chunk_size  # Configured Notion multipart chunk size
        total_parts = len(self.upload_futures) + 1  # Current queued parts plus this one
        chunk_size_mb = len(chunk_data) / (1024*1024)
        print(f"Uploading part {part_number} of {total_parts} ({chunk_size_mb:.2f} MB)...")
        response = self.session.post(url, headers=headers, files=files)
        if response.status_code != 200:
            raise Exception(f"Part {part_number} upload failed: {response.text}")
            
        # No need to store ETags or part numbers, Notion handles this internally
        return response

    def _complete_multipart_upload(self, upload_url: str, upload_id: str, parts: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Complete a multipart upload"""
        url = f"{upload_url}/complete"  # Base URL is already properly formatted
        headers = {**self.headers, "Content-Type": "application/json", "Connection": "keep-alive"}
        
        print("Completing multipart upload...")
        response = self.session.post(url, headers=headers, json={})  # Notion API expects an empty body
        if response.status_code != 200:
            raise Exception(f"Failed to complete multipart upload: {response.text}")
        return response.json()

    def _abort_multipart_upload(self, upload_url: str, upload_id: str):
        """Abort a multipart upload in case of failure"""
        try:
            url = f"{upload_url}/cancel"  # Base URL is already properly formatted
            headers = {**self.headers, "Content-Type": "application/json", "Connection": "keep-alive"}
            self.session.post(url, headers=headers)
            print(f"Successfully aborted multipart upload {upload_id}")
        except Exception as e:
            print(f"Failed to abort multipart upload {upload_id}: {e}")
