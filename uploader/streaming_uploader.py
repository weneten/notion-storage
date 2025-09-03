"""
Streaming File Uploader for Notion API
Implements continuous streaming upload with proper Notion API compliance
"""

import os
import io
import tempfile
import threading
import time
import hashlib
import uuid
import secrets
from typing import Optional, Callable, Dict, Any, List
import concurrent.futures
import queue
import requests
import base64
from flask import Response
from flask_socketio import SocketIO
from .notion_uploader import NotionFileUploader
from .parallel_processor import ParallelChunkProcessor, generate_salt, calculate_salted_hash
from .s3_downloader import download_file_from_url
from .crypto_utils import generate_key, encrypt_stream
import gc


def _fetch_text(url: str) -> str:
    """Download text content from a URL, using S3 helper for S3 links."""
    if 'amazonaws.com' in url:
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp_path = tmp.name
        try:
            download_file_from_url(url, tmp_path)
            with open(tmp_path, 'rb') as f:
                return f.read().decode('utf-8')
        finally:
            os.remove(tmp_path)
    else:
        resp = requests.get(url)
        resp.raise_for_status()
        return resp.content.decode('utf-8')


class NotionStreamingUploader:
    def delete_file_entry(self, file_db_id: str, user_database_id: str) -> None:
        """
        Deletes a file entry from Notion. Handles both single files and manifests (with parts),
        and deletes from both user and global index as needed.
        Args:
            file_db_id: The Notion database page ID of the file or manifest.
            user_database_id: The user's Notion database ID.
        """
        import json
        import requests
        entry = self.notion_uploader.get_user_by_id(file_db_id)
        if not entry:
            print(f"[DELETE] Entry not found: {file_db_id}. Aborting delete.")
            return

        props = entry.get('properties', {})
        filename = props.get('Name', {}).get('title', [{}])[0].get('plain_text', '') if 'Name' in props else ''
        is_manifest = False
        manifest_reason = None

        # Check for is_manifest property (Notion checkbox) or .file.json filename
        if 'is_manifest' in props and props['is_manifest'].get('checkbox'):
            is_manifest = True
            manifest_reason = 'is_manifest property'
        if filename.endswith('.file.json'):
            is_manifest = True
            if manifest_reason:
                manifest_reason += ' + filename .file.json'
            else:
                manifest_reason = 'filename .file.json'

        print(f"[DEBUG] is_manifest={is_manifest} for {file_db_id} (reason: {manifest_reason})")

        parts = []
        if is_manifest:
            print(f"[DELETE] Entry {file_db_id} is a manifest. Attempting to delete manifest and all parts.")
            # Try to get the file_data property (should contain the JSON metadata)
            file_data = None
            # Try file property first
            if 'file_data' in props and 'files' in props['file_data'] and props['file_data']['files']:
                file_url = props['file_data']['files'][0].get('file', {}).get('url') or props['file_data']['files'][0].get('external', {}).get('url')
                if file_url:
                    try:
                        file_data = _fetch_text(file_url)
                        print(f"[DELETE] Manifest JSON loaded from file property (url): {file_url}")
                    except Exception as e:
                        print(f"[DELETE] Failed to fetch manifest JSON from Notion: {e}")
            # Fallback: try to get file_data as plain text
            if not file_data and 'file_data' in props and 'rich_text' in props['file_data'] and props['file_data']['rich_text']:
                file_data = props['file_data']['rich_text'][0].get('plain_text')
                print(f"[DELETE] Manifest JSON loaded from rich_text property.")
            # Extra fallback: try to fetch the page content directly via Notion API if above fails
            if not file_data:
                print(f"[DELETE] Could not retrieve manifest JSON from file property, trying Notion API fallback...")
                try:
                    manifest_api_url = f"https://api.notion.com/v1/pages/{file_db_id}"
                    headers = {
                        "Authorization": f"Bearer {self.api_token}",
                        "Notion-Version": "2022-06-28",
                        "accept": "application/json",
                        "Content-Type": "application/json"
                    }
                    resp = requests.get(manifest_api_url, headers=headers)
                    if resp.status_code == 200:
                        manifest_api_data = resp.json()
                        # Try to extract file_data from API response
                        api_props = manifest_api_data.get('properties', {})
                        if 'file_data' in api_props and 'rich_text' in api_props['file_data'] and api_props['file_data']['rich_text']:
                            file_data = api_props['file_data']['rich_text'][0].get('plain_text')
                            print(f"[DELETE] Manifest JSON loaded from Notion API fallback.")
                    else:
                        print(f"[DELETE] Notion API fallback failed with status {resp.status_code}: {resp.text}")
                except Exception as e:
                    print(f"[DELETE] Notion API fallback failed: {e}")
            if not file_data:
                print(f"[DELETE] ABORT: Could not retrieve manifest JSON for manifest {file_db_id}. No files will be deleted.")
            else:
                try:
                    print(f"[DELETE] Raw manifest JSON string: {file_data}")
                    manifest_json = json.loads(file_data)
                    print(f"[DELETE] Loaded manifest JSON: {manifest_json}")
                    parts = manifest_json.get('parts', [])
                    print(f"[DELETE] Parts found in manifest: {parts}")
                except Exception as e:
                    print(f"[DELETE] ABORT: Failed to parse manifest JSON: {e}. No files will be deleted.")
                    print(f"[DELETE] Raw manifest JSON for debugging: {file_data}")
                    parts = []
            if not parts:
                print(f"[DELETE] WARNING: No parts found in manifest JSON for {file_db_id}. No part files will be deleted.")
                print(f"[DELETE] Raw manifest JSON for diagnosis: {file_data}")

        deleted_count = 0

        def _delete_part(part: Dict[str, Any]) -> bool:
            part_id = part.get('file_id')
            part_filename = part.get('filename')
            if not part_id:
                print(f"[DELETE] Skipping part with missing file_id: {part}")
                return False
            print(f"[DELETE] Deleting part: {part_filename} (id={part_id})")
            try:
                self.notion_uploader.delete_file_from_user_database(part_id)
                print(f"[DELETE] Deleted part {part_id} from user DB.")
                if self.notion_uploader.global_file_index_db_id:
                    try:
                        self.notion_uploader.delete_file_from_index(part_id)
                        print(f"[DELETE] Deleted part {part_id} from global index.")
                    except Exception as e:
                        print(f"[DELETE] Failed to delete part {part_id} from global index: {e}")
                return True
            except Exception as e:
                print(f"[DELETE] Failed to delete part {part_id} from user DB: {e}")
                return False

        if parts:
            max_workers = min(10, len(parts))
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(_delete_part, part) for part in parts]
                for future in concurrent.futures.as_completed(futures):
                    try:
                        if future.result():
                            deleted_count += 1
                    except Exception:
                        pass

        if deleted_count == 0 and is_manifest:
            print(f"[DELETE] WARNING: No part files were deleted for manifest {file_db_id}.")

        # Delete the main entry (single file or manifest itself)
        print(f"[DELETE] Deleting main entry: {file_db_id}")
        try:
            self.notion_uploader.delete_file_from_user_database(file_db_id)
            print(f"[DELETE] Deleted main entry {file_db_id} from user DB.")
        except Exception as e:
            print(f"[DELETE] Failed to delete main entry {file_db_id} from user DB: {e}")
        if self.notion_uploader.global_file_index_db_id:
            try:
                self.notion_uploader.delete_file_from_index(file_db_id)
                print(f"[DELETE] Deleted main entry {file_db_id} from global index.")
            except Exception as e:
                print(f"[DELETE] Failed to delete main entry {file_db_id} from global index: {e}")

    def delete_manifest_and_parts(self, manifest_db_id: str, user_database_id: str) -> None:
        """
        Delete the manifest (metadata JSON) and all associated part files from the user's database and global index.
        Args:
            manifest_db_id: The Notion database page ID of the manifest (.file.json) entry.
            user_database_id: The user's Notion database ID.
        """
        import json
        import requests
        print(f"[DELETE] Deleting manifest and parts for manifest_db_id={manifest_db_id}")

        # Fetch manifest entry and manifest JSON BEFORE any deletion
        manifest_entry = self.notion_uploader.get_user_by_id(manifest_db_id)
        if not manifest_entry:
            print(f"[DELETE] Manifest entry not found: {manifest_db_id}. Aborting delete.")
            return


        # Try to get the file_data property (should contain the JSON metadata)
        file_data = None
        props = manifest_entry.get('properties', {})
        # Try file property first
        if 'file_data' in props and 'files' in props['file_data'] and props['file_data']['files']:
            file_url = props['file_data']['files'][0].get('file', {}).get('url') or props['file_data']['files'][0].get('external', {}).get('url')
            if file_url:
                try:
                    file_data = _fetch_text(file_url)
                    print(f"[DELETE] Manifest JSON loaded from file property (url): {file_url}")
                except Exception as e:
                    print(f"[DELETE] Failed to fetch manifest JSON from Notion: {e}")

        # Fallback: try to get file_data as plain text
        if not file_data and 'file_data' in props and 'rich_text' in props['file_data'] and props['file_data']['rich_text']:
            file_data = props['file_data']['rich_text'][0].get('plain_text')
            print(f"[DELETE] Manifest JSON loaded from rich_text property.")

        # Extra fallback: try to fetch the page content directly via Notion API if above fails
        if not file_data:
            print(f"[DELETE] Could not retrieve manifest JSON from file property, trying Notion API fallback...")
            try:
                manifest_api_url = f"https://api.notion.com/v1/pages/{manifest_db_id}"
                headers = {
                    "Authorization": f"Bearer {self.api_token}",
                    "Notion-Version": "2022-06-28",
                    "accept": "application/json",
                    "Content-Type": "application/json"
                }
                resp = requests.get(manifest_api_url, headers=headers)
                if resp.status_code == 200:
                    manifest_api_data = resp.json()
                    # Try to extract file_data from API response
                    api_props = manifest_api_data.get('properties', {})
                    if 'file_data' in api_props and 'rich_text' in api_props['file_data'] and api_props['file_data']['rich_text']:
                        file_data = api_props['file_data']['rich_text'][0].get('plain_text')
                        print(f"[DELETE] Manifest JSON loaded from Notion API fallback.")
                else:
                    print(f"[DELETE] Notion API fallback failed with status {resp.status_code}: {resp.text}")
            except Exception as e:
                print(f"[DELETE] Notion API fallback failed: {e}")

        if not file_data:
            print(f"[DELETE] ABORT: Could not retrieve manifest JSON for manifest {manifest_db_id}. No files will be deleted.")
            return

        # Parse JSON and get parts
        try:
            print(f"[DELETE] Raw manifest JSON string: {file_data}")
            manifest_json = json.loads(file_data)
            print(f"[DELETE] Loaded manifest JSON: {manifest_json}")
            parts = manifest_json.get('parts', [])
            print(f"[DELETE] Parts found in manifest: {parts}")
        except Exception as e:
            print(f"[DELETE] ABORT: Failed to parse manifest JSON: {e}. No files will be deleted.")
            print(f"[DELETE] Raw manifest JSON for debugging: {file_data}")
            return

        if not parts:
            print(f"[DELETE] WARNING: No parts found in manifest JSON for {manifest_db_id}. No part files will be deleted.")
            print(f"[DELETE] Raw manifest JSON for diagnosis: {file_data}")

        deleted_count = 0

        def _delete_part(part: Dict[str, Any]) -> bool:
            part_id = part.get('file_id')
            part_filename = part.get('filename')
            if not part_id:
                print(f"[DELETE] Skipping part with missing file_id: {part}")
                return False
            print(f"[DELETE] Deleting part: {part_filename} (id={part_id})")
            try:
                self.notion_uploader.delete_file_from_user_database(part_id)
                print(f"[DELETE] Deleted part {part_id} from user DB.")
                if self.notion_uploader.global_file_index_db_id:
                    try:
                        self.notion_uploader.delete_file_from_index(part_id)
                        print(f"[DELETE] Deleted part {part_id} from global index.")
                    except Exception as e:
                        print(f"[DELETE] Failed to delete part {part_id} from global index: {e}")
                return True
            except Exception as e:
                print(f"[DELETE] Failed to delete part {part_id} from user DB: {e}")
                return False

        if parts:
            max_workers = min(10, len(parts))
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(_delete_part, part) for part in parts]
                for future in concurrent.futures.as_completed(futures):
                    try:
                        if future.result():
                            deleted_count += 1
                    except Exception:
                        pass

        if deleted_count == 0:
            print(f"[DELETE] WARNING: No part files were deleted for manifest {manifest_db_id}.")

        # Delete the manifest itself from user DB and global index
        print(f"[DELETE] Deleting manifest entry: {manifest_db_id}")
        try:
            self.notion_uploader.delete_file_from_user_database(manifest_db_id)
            print(f"[DELETE] Deleted manifest from user DB.")
        except Exception as e:
            print(f"[DELETE] Failed to delete manifest from user DB: {e}")
        if self.notion_uploader.global_file_index_db_id:
            try:
                self.notion_uploader.delete_file_from_index(manifest_db_id)
                print(f"[DELETE] Deleted manifest from global index.")
            except Exception as e:
                print(f"[DELETE] Failed to delete manifest from global index: {e}")
    """
    Handles streaming file uploads with automatic single-part/multi-part decision making
    based on Notion API specifications.
    """
      # Notion API constants
    SINGLE_PART_THRESHOLD = 20 * 1024 * 1024  # 20 MiB
    MULTIPART_CHUNK_SIZE = 5 * 1024 * 1024    # 5 MiB for multipart uploads
    SPLIT_THRESHOLD = 5 * 1024 * 1024       # 5 MiB

    class _PartStream:
        """Iterator that yields exactly ``part_size`` bytes from ``stream_iter``.
        It also updates provided hashers and preserves any excess bytes for the
        next part."""

        def __init__(self, stream_iter, part_size, leftover, overall_hasher):
            self.stream_iter = stream_iter
            self.part_size = part_size
            self.leftover = leftover or b""
            self.overall_hasher = overall_hasher
            self.bytes_sent = 0
            self.part_hasher = hashlib.sha512()

        def __iter__(self):
            return self

        def __next__(self):
            if self.bytes_sent >= self.part_size:
                raise StopIteration

            if self.leftover:
                data = self.leftover
                self.leftover = b""
            else:
                data = next(self.stream_iter)

            if not data:
                raise StopIteration

            remaining = self.part_size - self.bytes_sent
            chunk = data[:remaining]
            if len(data) > remaining:
                self.leftover = data[remaining:]

            self.bytes_sent += len(chunk)
            self.part_hasher.update(chunk)
            if self.overall_hasher is not None:
                self.overall_hasher.update(chunk)

            return chunk

        def get_leftover(self):
            return self.leftover

        def get_part_hash(self):
            return self.part_hasher.hexdigest()

        def get_bytes_sent(self):
            return self.bytes_sent
    
    def __init__(self, api_token: str, socketio: Optional[SocketIO] = None, notion_uploader: Optional[NotionFileUploader] = None):
        self.api_token = api_token
        self.socketio = socketio
        self.notion_uploader = notion_uploader  # Use existing Notion uploader for actual API calls
        
    def create_upload_session(self, filename: str, file_size: int, user_database_id: str,
                            progress_callback: Optional[Callable] = None, folder_path: str = "/") -> Dict[str, Any]:
        """
        Create an upload session and determine upload strategy based on file size
        """
        upload_id = str(uuid.uuid4())
        is_multipart = file_size > self.SINGLE_PART_THRESHOLD
        
        session_data = {
            'upload_id': upload_id,
            'filename': filename,
            'file_size': file_size,
            'user_database_id': user_database_id,
            'folder_path': folder_path,
            'is_multipart': is_multipart,
            'progress_callback': progress_callback,
            'created_at': time.time(),
            'status': 'initialized',
            'bytes_uploaded': 0,
            'last_activity': time.time(),
            'hasher': hashlib.sha512()  # For file integrity
        }

        # Generate encryption material for this upload session
        key = generate_key()
        iv = os.urandom(16)
        fingerprint = hashlib.sha256(key).hexdigest()
        session_data['encryption_meta'] = {
            'alg': 'AES-CTR',
            'key': key,
            'iv': iv,
            'iv_b64': base64.b64encode(iv).decode('utf-8'),
            'key_b64': base64.b64encode(key).decode('utf-8'),
            'key_fingerprint': fingerprint,
        }
        
        if is_multipart:
            # Initialize multipart upload with Notion
            session_data.update(self._init_multipart_upload(filename, file_size, user_database_id))
        
        return session_data
    
    def _init_multipart_upload(self, filename: str, file_size: int, user_database_id: str) -> Dict[str, Any]:
        """
        Initialize a multipart upload with Notion API
        """
        # Calculate total parts needed
        total_parts = (file_size + self.MULTIPART_CHUNK_SIZE - 1) // self.MULTIPART_CHUNK_SIZE
        
        # For now, return basic multipart session data
        # In a real implementation, you would call Notion's multipart upload initiation API
        return {
            'total_parts': total_parts,
            'completed_parts': set(),
            'upload_urls': {},  # Would be populated by Notion API response
            'upload_key': None,  # Would be populated by Notion API response
            'part_etags': {}     # Store ETags from completed parts
        }
    
    def complete_upload_with_database_integration(self, upload_session, notion_result,
                                                  precomputed_hash: str = None,
                                                  precomputed_salt: str = None):
        """Complete upload and integrate with databases.

        This method can accept a precomputed file hash and salt.  Providing
        these allows the caller to return a response to the client immediately
        after the file bytes finish uploading while database work continues in
        a background thread.
        """
        try:
            print(f"DEBUG: Starting database integration for upload {upload_session['upload_id']}")

            # Generate salt and hash if not provided by caller
            salt = precomputed_salt or generate_salt()

            file_hash = precomputed_hash or upload_session['hasher'].hexdigest()
            salted_hash = calculate_salted_hash(file_hash, salt)
            
            print(f"DEBUG: File hash calculated: {file_hash[:16]}...")
            print(f"DEBUG: Salted hash: {salted_hash[:16]}...")
            
            # CRITICAL FIX 1 & 2: ID Validation and Consistent Key Usage
            # First try to extract the actual file ID from completion response
            file_upload_id = (notion_result.get('file', {}).get('id') or
                             notion_result.get('file_upload_id') or
                             notion_result.get('file_id'))
            print(f"🔍 STREAMING_UPLOADER: Database integration starting")
            print(f"🔍 STREAMING_UPLOADER: notion_result keys: {list(notion_result.keys())}")
            print(f"🔍 STREAMING_UPLOADER: Extracted file_upload_id: {file_upload_id}")
            
            if not file_upload_id:
                error_msg = f"ID EXTRACTION FAILED: No valid file_upload_id found in notion_result. Available keys: {list(notion_result.keys())}"
                print(f"🚨 STREAMING_UPLOADER ERROR: {error_msg}")
                raise Exception(error_msg)
            
            # Only create DB entry for the main/original filename if NOT splitting (handled in non-split branch below)
            # For split uploads, DB entries are created for .partN and .file.json only.
            if upload_session['file_size'] <= self.SPLIT_THRESHOLD:
                # Add to user database with validated ID (non-split only)
                user_db_result = self.notion_uploader.add_file_to_user_database(
                    database_id=upload_session['user_database_id'],
                    filename=upload_session['filename'],
                    file_size=upload_session['file_size'],
                    file_hash=salted_hash,
                    file_upload_id=file_upload_id,
                    original_filename=upload_session['filename'],
                    salt=salt,
                    folder_path=upload_session.get('folder_path', '/'),
                    encryption_meta=upload_session.get('encryption_meta'),
                )
                database_page_id = user_db_result['id']  # This is the DATABASE PAGE ID - different from file upload ID
                print(f"DEBUG: Added to user database with database page ID: {database_page_id}")
                print(f"🔍 STREAMING ID SEPARATION: File Upload ID: {file_upload_id} | Database Page ID: {database_page_id}")
                # Add to global index - use DATABASE PAGE ID here, not file upload ID
                if self.notion_uploader.global_file_index_db_id:
                    self.notion_uploader.add_file_to_index(
                        salted_sha512_hash=salted_hash,
                        file_page_id=database_page_id,  # Use database page ID for database operations
                        user_database_id=upload_session['user_database_id'],
                        original_filename=upload_session['filename'],
                        is_public=False
                    )
                    print(f"DEBUG: Added to global file index with database page ID: {database_page_id}")
                else:
                    print(f"WARNING: Global file index DB ID not configured, skipping global index")
                return {
                    'file_id': database_page_id,  # FIXED: Return database page ID for database operations
                    'notion_file_upload_id': file_upload_id,  # FIXED: Keep file upload ID separate
                    'file_hash': salted_hash,
                    'status': 'completed',
                    'filename': upload_session['filename'],
                    'bytes_uploaded': upload_session['file_size']
                }
        except Exception as e:
            print(f"ERROR: Database integration failed: {e}")
            # Cleanup on failure
            self._cleanup_failed_upload(upload_session, notion_result)
            raise Exception(f"Database integration failed: {str(e)}")

    def _cleanup_failed_upload(self, upload_session, notion_result):
        """Clean up Notion upload if database operations fail"""
        try:
            if notion_result and 'file_upload_id' in notion_result:
                print(f"WARNING: Database integration failed, but Notion file uploaded with ID: {notion_result['file_upload_id']}")
                print(f"WARNING: Manual cleanup may be required for file: {upload_session['filename']}")
                # Note: Notion API doesn't provide direct file deletion
        except Exception as cleanup_error:
            print(f"Warning: Failed to cleanup Notion upload: {cleanup_error}")

    def _start_database_integration_async(self, upload_session, notion_result,
                                          file_hash: str, salt: str) -> None:
        """Kick off database integration in a background thread.

        Args:
            upload_session: Active upload session metadata
            notion_result:  Response from Notion upload API
            file_hash:      Precomputed SHA512 hash of file contents
            salt:           Salt to use when storing in database
        """

        def _bg_task():
            try:
                final_result = self.complete_upload_with_database_integration(
                    upload_session,
                    notion_result,
                    precomputed_hash=file_hash,
                    precomputed_salt=salt,
                )

                # Patch visibility metadata now that the DB entry exists
                try:
                    db_entry = self.notion_uploader.get_user_by_id(final_result['file_id'])
                    self.notion_uploader.update_user_properties(final_result['file_id'], {
                        "is_visible": {"checkbox": True},
                        "file_data": db_entry.get('properties', {}).get('file_data', {})
                    })
                except Exception as patch_err:
                    print(f"WARNING: Failed to patch visibility properties: {patch_err}")

                upload_session.update({
                    'status': 'completed',
                    'file_hash': final_result['file_hash'],
                    'notion_file_id': final_result['file_id'],
                    'completed_at': time.time()
                })
                if self.socketio:
                    self.socketio.emit('upload_progress', {
                        'upload_id': upload_session['upload_id'],
                        'status': 'completed',
                        'progress': 100,
                        'bytes_uploaded': upload_session['file_size'],
                        'total_size': upload_session['file_size'],
                        'file_id': final_result['file_id']
                    })
            except Exception as e:
                upload_session.update({
                    'status': 'failed',
                    'error': str(e),
                    'failed_at': time.time()
                })
                if self.socketio:
                    self.socketio.emit('upload_progress', {
                        'upload_id': upload_session['upload_id'],
                        'status': 'failed',
                        'error': str(e)
                    })

        threading.Thread(target=_bg_task, daemon=True).start()

    def _store_part_in_database(
        self,
        user_database_id: str,
        original_filename: str,
        part_filename: str,
        part_size: int,
        part_salted_hash: str,
        part_salt: str,
        notion_file_upload_id: str,
        file_url: Optional[str],
        encryption_meta: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Persist a single part's metadata in Notion databases.

        This method is intended to run in a background thread so the main
        upload loop can continue accepting data without waiting for database
        operations to complete.
        """
        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            db_entry = self.notion_uploader.add_file_to_user_database(
                database_id=user_database_id,
                filename=part_filename,
                file_size=part_size,
                file_hash=part_salted_hash,
                file_upload_id=notion_file_upload_id,
                is_public=False,
                salt=part_salt,
                original_filename=original_filename,
                file_url=file_url,
                # Store part entries at root to avoid orphaned parts when moving folders
                folder_path='/',
                encryption_meta=encryption_meta,
            )

            if self._validate_file_attachment(db_entry, notion_file_upload_id):
                break

            if attempt == max_attempts:
                raise Exception(
                    f"Failed to attach part {part_filename} after {max_attempts} attempts"
                )
            print(
                f"WARNING: Validation failed for part {part_filename} (attempt {attempt}/{max_attempts}), retrying..."
            )
            time.sleep(1)

        if self.notion_uploader.global_file_index_db_id:
            self.notion_uploader.add_file_to_index(
                salted_sha512_hash=part_salted_hash,
                file_page_id=db_entry['id'],
                user_database_id=user_database_id,
                original_filename=part_filename,
                is_public=False,
            )

        self.notion_uploader.update_user_properties(db_entry['id'], {
            "is_visible": {"checkbox": False},
            "file_data": db_entry.get('properties', {}).get('file_data', {})
        })

        return db_entry

    def _validate_file_attachment(self, db_entry: Dict[str, Any], upload_id: str) -> bool:
        """Ensure the Notion page has a valid file attachment."""
        try:
            files = (
                db_entry.get("properties", {})
                .get("file_data", {})
                .get("files", [])
            )
            if not files:
                return False
            file_info = files[0]
            if file_info.get("type") == "file":
                return bool(file_info.get("file", {}).get("url"))
            if file_info.get("type") == "file_upload":
                return file_info.get("file_upload", {}).get("id") == upload_id
            return False
        except Exception:
            return False

    def _upload_part_worker(
        self,
        part_session: Dict[str, Any],
        chunk_queue: queue.Queue,
        part_size: int,
    ) -> Dict[str, str]:
        """Upload a part by streaming bytes from ``chunk_queue``.

        The main thread feeds incoming bytes into ``chunk_queue`` while this
        worker consumes them and uploads to Notion. It returns the Notion file
        upload ID and resulting file URL so the caller can persist metadata
        once the hash is known."""

        def file_generator():
            while True:
                chunk = chunk_queue.get()
                if chunk is None:
                    break
                yield chunk

        if part_size > self.SINGLE_PART_THRESHOLD:
            part_result = self._process_multipart_stream(part_session, file_generator(), db_integration=False)
        else:
            part_result = self._process_single_part_stream(part_session, file_generator(), db_integration=False)

        file_url = None
        if part_result and part_result.get('result'):
            file_url = part_result['result'].get('download_link') or part_result['result'].get('file', {}).get('url')

        return {
            "file_upload_id": part_result.get("notion_file_upload_id", part_result.get("file_id")),
            "file_url": file_url,
        }

    def process_stream(self, upload_session: Dict[str, Any], stream_generator, resume_from: int = 0) -> Dict[str, Any]:
        """
        Process the incoming file stream and handle upload based on file size and splitting plan
        """
        try:
            print(f"DEBUG: Starting stream processing for upload {upload_session['upload_id']}")
            print(f"DEBUG: File: {upload_session['filename']}, Size: {upload_session['file_size']}, Multipart: {upload_session['is_multipart']}")
            upload_session.setdefault('uploaded_parts', [])
            upload_session['last_activity'] = time.time()

            file_size = upload_session['file_size']
            filename = upload_session['filename']
            user_database_id = upload_session['user_database_id']

            if file_size > self.SPLIT_THRESHOLD:
                # --- Stream large files in parts without buffering the entire part ---
                print(f"INFO: File size > 500 MiB, splitting and uploading in parts...")

                total_parts = (file_size + self.SPLIT_THRESHOLD - 1) // self.SPLIT_THRESHOLD
                last_part_size = file_size - self.SPLIT_THRESHOLD * (total_parts - 1)
                part_sizes = [self.SPLIT_THRESHOLD] * (total_parts - 1)
                if last_part_size:
                    part_sizes.append(last_part_size)

                stream_iter = iter(stream_generator)
                leftover = b""
                parts_metadata = []
                total_uploaded = 0

                # Upload parts in background threads so incoming data can keep
                # flowing without waiting for Notion or database operations to
                # finish for each part.
                with concurrent.futures.ThreadPoolExecutor(max_workers=3) as upload_executor:
                    part_futures: List[concurrent.futures.Future] = []
                    parts_lock = threading.Lock()

                    def make_callback(idx, part_filename, part_size, part_salted_hash, part_salt):
                        def _callback(fut: concurrent.futures.Future) -> None:
                            try:
                                upload_result = fut.result()
                                db_entry = self._store_part_in_database(
                                    user_database_id,
                                    filename,
                                    part_filename,
                                    part_size,
                                    part_salted_hash,
                                    part_salt,
                                    upload_result['file_upload_id'],
                                    upload_result['file_url'],
                                    upload_session.get('encryption_meta'),
                                )
                                with parts_lock:
                                    upload_session['uploaded_parts'].append(db_entry['id'])
                                    parts_metadata.append({
                                        "part_number": idx,
                                        "filename": part_filename,
                                        "file_id": db_entry['id'],
                                        "file_hash": part_salted_hash,
                                        "size": part_size,
                                    })
                            except Exception as e:
                                print(f"ERROR: Failed to store part {idx} metadata: {e}")
                        return _callback

                    for idx, part_size in enumerate(part_sizes, start=1):
                        part_filename = f"{filename}.part{idx}"
                        part_session = self.create_upload_session(part_filename, part_size, user_database_id)
                        # Use same encryption metadata for all parts
                        part_session['encryption_meta'] = upload_session.get('encryption_meta')
                        part_stream = self._PartStream(stream_iter, part_size, leftover, upload_session['hasher'])

                        chunk_queue: queue.Queue = queue.Queue(maxsize=4)
                        future = upload_executor.submit(
                            self._upload_part_worker,
                            part_session,
                            chunk_queue,
                            part_size,
                        )

                        for chunk in part_stream:
                            chunk_queue.put(chunk)

                        chunk_queue.put(None)
                        leftover = part_stream.get_leftover()
                        part_hash = part_stream.get_part_hash()
                        bytes_sent = part_stream.get_bytes_sent()

                        if bytes_sent < part_size:
                            future.cancel()
                            raise ValueError(
                                f"Incomplete upload: expected {part_size} bytes for part {idx}, received {bytes_sent}"
                            )

                        total_uploaded += bytes_sent

                        part_salt = generate_salt()
                        part_salted_hash = calculate_salted_hash(part_hash, part_salt)

                        future.add_done_callback(
                            make_callback(idx, part_filename, part_size, part_salted_hash, part_salt)
                        )
                        part_futures.append(future)
                        upload_session['last_activity'] = time.time()

                    concurrent.futures.wait(part_futures)
                    parts_metadata.sort(key=lambda x: x["part_number"])

                    if total_uploaded != file_size:
                        raise ValueError(
                            f"Incomplete upload: expected {file_size} bytes, received {total_uploaded}"
                        )

                import json
                metadata_json = json.dumps({
                    "original_filename": filename,
                    "total_size": file_size,
                    "parts": parts_metadata
                }, indent=2)
                metadata_bytes = metadata_json.encode("utf-8")
                metadata_filename = f"{filename}.file.json"

                metadata_result = self._upload_to_notion_single_part(
                    user_database_id,
                    metadata_filename,
                    io.BytesIO(metadata_bytes),
                    len(metadata_bytes),
                    upload_session.get('encryption_meta')
                )

                metadata_file_url = None
                if metadata_result and metadata_result.get('result'):
                    metadata_file_url = metadata_result['result'].get('download_link') or metadata_result['result'].get('file', {}).get('url')

                self.notion_uploader.ensure_database_property(user_database_id, "is_manifest", "checkbox")

                manifest_json_hash = hashlib.sha512(metadata_bytes).hexdigest()
                manifest_salt = generate_salt()
                manifest_salted_hash = calculate_salted_hash(manifest_json_hash, manifest_salt)

                # Store the original file's size so the manifest entry reflects
                # the combined size of all parts rather than the JSON manifest
                # payload itself.
                def _manifest_db_task():
                    try:
                        metadata_db_entry = self.notion_uploader.add_file_to_user_database(
                            database_id=user_database_id,
                            filename=metadata_filename,
                            file_size=file_size,
                            file_hash=manifest_salted_hash,
                            file_upload_id=metadata_result.get('file_upload_id'),
                            is_public=False,
                            salt=manifest_salt,
                            original_filename=filename,
                            file_url=metadata_file_url,
                            is_manifest=True,
                            folder_path=upload_session.get('folder_path', '/'),
                            encryption_meta=upload_session.get('encryption_meta'),
                        )

                        self.notion_uploader.update_user_properties(metadata_db_entry['id'], {
                            "is_visible": {"checkbox": True},
                            "file_data": metadata_db_entry.get('properties', {}).get('file_data', {})
                        })

                        if self.notion_uploader.global_file_index_db_id:
                            self.notion_uploader.add_file_to_index(
                                salted_sha512_hash=manifest_salted_hash,
                                file_page_id=metadata_db_entry['id'],
                                user_database_id=user_database_id,
                                original_filename=metadata_filename,
                                is_public=False
                            )

                        upload_session.update({
                            'status': 'completed',
                            'file_hash': manifest_salted_hash,
                            'notion_file_id': metadata_db_entry['id'],
                            'completed_at': time.time()
                        })
                        if self.socketio:
                            self.socketio.emit('upload_progress', {
                                'upload_id': upload_session['upload_id'],
                                'status': 'completed',
                                'progress': 100,
                                'bytes_uploaded': file_size,
                                'total_size': file_size,
                                'file_id': metadata_db_entry['id']
                            })
                    except Exception as e:
                        upload_session.update({
                            'status': 'failed',
                            'error': str(e),
                            'failed_at': time.time()
                        })
                        if self.socketio:
                            self.socketio.emit('upload_progress', {
                                'upload_id': upload_session['upload_id'],
                                'status': 'failed',
                                'error': str(e)
                            })

                threading.Thread(target=_manifest_db_task, daemon=True).start()

                print(f"INFO: File split and uploaded in {len(parts_metadata)} parts + metadata JSON.")

                upload_session.update({
                    'status': 'finalizing',
                    'bytes_uploaded': file_size,
                    'file_hash': manifest_salted_hash
                })

                upload_session.pop('uploaded_parts', None)
                return {
                    "status": "finalizing",
                    "split": True,
                    "parts": parts_metadata,
                    "metadata_file_id": None,
                    "metadata_filename": metadata_filename,
                    "file_id": None,
                    "bytes_uploaded": file_size,
                    "file_hash": manifest_salted_hash,
                    "filename": metadata_filename,
                    "original_filename": filename
                }

            else:
                # --- No split needed, use existing logic ---
                if file_size > self.SINGLE_PART_THRESHOLD:
                    result = self._process_multipart_stream(upload_session, stream_generator, resume_from=resume_from)
                else:
                    if resume_from > 0:
                        raise ValueError("Cannot resume single-part upload")
                    result = self._process_single_part_stream(upload_session, stream_generator)
                # Patch DB entry: is_visible checked, file_data set
                # (Assume complete_upload_with_database_integration returns DB page id)
                db_page_id = result.get('file_id')
                if db_page_id:
                    # Fetch DB entry to get file property
                    db_entry = self.notion_uploader.get_user_by_id(db_page_id)
                    self.notion_uploader.update_user_properties(db_page_id, {
                        "is_visible": {"checkbox": True},
                        "file_data": db_entry.get('properties', {}).get('file_data', {})
                    })
                print(f"INFO: File uploaded as single DB entry (no split).")
                upload_session.pop('uploaded_parts', None)
                return {
                    **result,
                    "split": False
                }
        except Exception as e:
            print(f"ERROR: Stream processing failed for upload {upload_session['upload_id']}: {str(e)}")
            print(f"ERROR: Exception type: {type(e).__name__}")
            upload_session.update({
                'status': 'failed',
                'error': str(e),
                'error_type': type(e).__name__,
                'failed_at': time.time()
            })
            raise
    
    def _process_single_part_stream(self, upload_session: Dict[str, Any], stream_generator, db_integration: bool = True) -> Dict[str, Any]:
        """
        Handle single-part upload (≤ 20 MiB files)
        """
        upload_session['status'] = 'uploading'
        buffer = io.BytesIO()
        bytes_received = 0
        try:
            # Collect all data first for single-part upload
            for chunk in stream_generator:
                if not chunk:
                    break
                buffer.write(chunk)
                bytes_received += len(chunk)
                upload_session['hasher'].update(chunk)
                # Calculate progress unconditionally
                progress = (bytes_received / upload_session['file_size']) * 100
                # Update progress callback if available
                if upload_session['progress_callback']:
                    upload_session['progress_callback'](progress, bytes_received)
                # Emit progress via SocketIO if available
                if self.socketio:
                    self.socketio.emit('upload_progress', {
                        'upload_id': upload_session['upload_id'],
                        'bytes_uploaded': bytes_received,
                        'total_size': upload_session['file_size'],
                        'progress': progress
                    })
            if bytes_received != upload_session['file_size']:
                raise ValueError(
                    f"Incomplete upload: expected {upload_session['file_size']} bytes, received {bytes_received}"
                )

            # Upload the complete file to Notion
            buffer.seek(0)
            notion_result = self._upload_to_notion_single_part(
                upload_session['user_database_id'],
                upload_session['filename'],
                buffer,
                bytes_received,
                upload_session.get('encryption_meta')
            )
            if db_integration:
                # Start database integration in background and return immediately
                file_hash = upload_session['hasher'].hexdigest()
                salt = generate_salt()
                salted_hash = calculate_salted_hash(file_hash, salt)

                if self.socketio:
                    self.socketio.emit('upload_progress', {
                        'upload_id': upload_session['upload_id'],
                        'status': 'finalizing',
                        'progress': 100
                    })

                self._start_database_integration_async(
                    upload_session,
                    notion_result['result'],
                    file_hash,
                    salt
                )

                upload_session.update({
                    'status': 'finalizing',
                    'bytes_uploaded': bytes_received,
                    'file_hash': salted_hash
                })
                return {
                    'notion_file_upload_id': notion_result.get('file_upload_id'),
                    'file_id': None,
                    'status': 'finalizing',
                    'filename': upload_session['filename'],
                    'bytes_uploaded': bytes_received,
                    'file_hash': salted_hash
                }
            else:
                # Only return Notion upload result, skip DB integration
                upload_session.update({
                    'status': 'completed',
                    'bytes_uploaded': bytes_received,
                    'completed_at': time.time()
                })
                return {
                    'notion_file_upload_id': notion_result.get('file_upload_id'),
                    'file_id': notion_result.get('file_upload_id'),
                    'status': 'success',
                    'filename': upload_session['filename'],
                    'bytes_uploaded': bytes_received
                }
        except Exception as e:
            upload_session.update({
                'status': 'failed',
                'error': str(e),
                'failed_at': time.time()
            })
            raise

    def _process_multipart_stream(self, upload_session: Dict[str, Any], stream_generator, resume_from: int = 0, db_integration: bool = True) -> Dict[str, Any]:
        """
        Handle multipart upload (> 20 MiB files) with parallel 5 MiB chunks
        """
        upload_session['status'] = 'uploading'
        
        try:
            print(f"DEBUG: Starting multipart upload with parallel processing")
            # Encrypt stream if required while preserving original hash
            encryption_meta = upload_session.get('encryption_meta')
            original_hasher = upload_session.get('hasher')
            encryption_active = False
            if encryption_meta:
                key = encryption_meta.get('key')
                iv = encryption_meta.get('iv')
                if key and iv:
                    encryption_active = True

                    class _HashPassthrough:
                        def __init__(self, real):
                            self._real = real
                        def update(self, data):
                            pass
                        def hexdigest(self):
                            return self._real.hexdigest()

                    upload_session['hasher'] = _HashPassthrough(original_hasher)

                    def _hashing_stream(gen):
                        for chunk in gen:
                            original_hasher.update(chunk)
                            yield chunk

                    stream_generator = encrypt_stream(key, iv, _hashing_stream(stream_generator))

            # Use parallel chunk processor
            parallel_processor = ParallelChunkProcessor(
                max_workers=10,
                notion_uploader=self.notion_uploader,
                upload_session=upload_session,
                socketio=self.socketio
            )

            # Process stream with parallel uploads
            notion_result = parallel_processor.process_stream(stream_generator, resume_from=resume_from)

            if encryption_active:
                upload_session['hasher'] = original_hasher

            print(f"DEBUG: Parallel upload completed, starting database integration")

            if db_integration:
                file_hash = upload_session['hasher'].hexdigest()
                salt = generate_salt()
                salted_hash = calculate_salted_hash(file_hash, salt)

                if self.socketio:
                    self.socketio.emit('upload_progress', {
                        'upload_id': upload_session['upload_id'],
                        'status': 'finalizing',
                        'progress': 100
                    })

                self._start_database_integration_async(
                    upload_session,
                    notion_result,
                    file_hash,
                    salt
                )

                upload_session.update({
                    'status': 'finalizing',
                    'bytes_uploaded': upload_session['file_size'],
                    'file_hash': salted_hash
                })
                return {
                    'notion_file_upload_id': notion_result.get('file_upload_id'),
                    'file_id': None,
                    'status': 'finalizing',
                    'filename': upload_session['filename'],
                    'bytes_uploaded': upload_session['file_size'],
                    'file_hash': salted_hash
                }
            else:
                upload_session.update({
                    'status': 'completed',
                    'bytes_uploaded': upload_session['file_size'],
                    'completed_at': time.time()
                })
                return {
                    'notion_file_upload_id': notion_result.get('file_upload_id'),
                    'file_id': notion_result.get('file_upload_id'),
                    'status': 'success',
                    'filename': upload_session['filename'],
                    'bytes_uploaded': upload_session['file_size']
                }
        except Exception as e:
            print(f"ERROR: Multipart upload failed: {e}")
            upload_session.update({
                'status': 'failed',
                'error': str(e),
                'failed_at': time.time()
            })
            raise

    def _upload_to_notion_single_part(self, user_database_id: str, filename: str,
                                    file_buffer: io.BytesIO, file_size: int,
                                    encryption_meta: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Upload file to Notion using single-part upload
        """
        print(f"DEBUG: _upload_to_notion_single_part called for {filename}, size: {file_size}")
        
        if self.notion_uploader:
            print(f"DEBUG: Using notion_uploader to upload file")
            try:                # Create a generator from the buffer
                def buffer_generator():
                    file_buffer.seek(0)
                    chunk_size = 64 * 1024  # 64KB chunks
                    while True:
                        chunk = file_buffer.read(chunk_size)
                        if not chunk:
                            break
                        yield chunk
                
                # Use file.txt for Notion API, but preserve original filename for database
                notion_filename = "file.txt"  # Always use file.txt for Notion API
                print(f"DEBUG: Using '{notion_filename}' for Notion API, real filename: {filename}")
                print(f"DEBUG: Using content-type 'text/plain' (required by Notion API)")
                
                # Use existing uploader's stream method
                result = self.notion_uploader.upload_single_file_stream(
                    file_stream=buffer_generator(),
                    filename=notion_filename,  # Use file.txt for API
                    database_id=user_database_id,
                    content_type='text/plain',  # Required by Notion API for all files
                    file_size=file_size,
                    original_filename=filename,  # Keep original filename for database
                    encryption_meta=encryption_meta,
                )
                
                # CRITICAL FIX 1: ID Validation for single-part uploads
                file_upload_id = result.get('file_upload_id')
                if not file_upload_id:
                    # Try alternative keys if file_upload_id is missing
                    file_upload_id = result.get('id') or result.get('upload_id')
                    print(f"🔍 SINGLE_PART: file_upload_id missing, using alternative: {file_upload_id}")
                
                if not file_upload_id:
                    print(f"🚨 SINGLE_PART ERROR: No valid ID found in result: {result}")
                    file_upload_id = str(uuid.uuid4())  # Generate fallback ID
                    print(f"🔍 SINGLE_PART: Generated fallback ID: {file_upload_id}")
                
                print(f"🔍 SINGLE_PART: Final file_upload_id: {file_upload_id}")
                
                return {
                    'file_upload_id': file_upload_id,
                    'status': 'success',
                    'size': file_size,
                    'result': result
                }
            except Exception as e:
                print(f"Error uploading to Notion: {e}")
                raise
        else:
            # Fallback placeholder implementation
            time.sleep(0.1)
            return {
                'file_id': str(uuid.uuid4()),
                'status': 'success',
                'size': file_size            }



class StreamingUploadManager:
    """
    Manages multiple concurrent streaming uploads
    """
    
    def __init__(self, api_token: str, socketio: Optional[SocketIO] = None, notion_uploader: Optional[NotionFileUploader] = None):
        self.uploader = NotionStreamingUploader(api_token, socketio, notion_uploader)
        self.active_uploads: Dict[str, Dict[str, Any]] = {}
        
        # CRITICAL FIX 3: Enhanced Thread Synchronization
        self.upload_lock = threading.Lock()          # Master lock for upload operations
        self.session_locks: Dict[str, threading.Lock] = {}  # Per-session locks
        self.id_tracking_lock = threading.Lock()     # Lock for ID tracking operations
        
        print("🔒 THREAD SAFETY: StreamingUploadManager initialized with enhanced synchronization")
    
    def create_upload_session(self, filename: str, file_size: int, user_database_id: str,
                            progress_callback: Optional[Callable] = None, folder_path: str = "/") -> str:
        """
        Create a new upload session and return the upload ID
        """
        session = self.uploader.create_upload_session(
            filename, file_size, user_database_id, progress_callback, folder_path
        )
        
        upload_id = session['upload_id']
        
        with self.upload_lock:
            self.active_uploads[upload_id] = session
        
        return upload_id
    
    def process_upload_stream(self, upload_id: str, stream_generator, resume_from: int = 0) -> Dict[str, Any]:
        """
        Process an upload stream for the given upload ID with enhanced thread safety
        """
        # CRITICAL FIX 3: Thread-Safe Session Management
        print(f"🔒 THREAD SAFETY: Starting process_upload_stream for {upload_id}")
        
        # Get or create session-specific lock
        with self.upload_lock:
            if upload_id not in self.active_uploads:
                raise ValueError(f"Upload session {upload_id} not found")
            
            # Create per-session lock if it doesn't exist
            if upload_id not in self.session_locks:
                self.session_locks[upload_id] = threading.Lock()
                print(f"🔒 THREAD SAFETY: Created session lock for {upload_id}")
            
            upload_session = self.active_uploads[upload_id]
            session_lock = self.session_locks[upload_id]

        resume_from = upload_session.get('bytes_uploaded', 0)
        
        # Process with session-specific lock to prevent concurrent processing of same upload
        with session_lock:
            print(f"🔒 THREAD SAFETY: Acquired session lock for {upload_id}")

            # Additional check to prevent race conditions
            current_status = upload_session.get('status')
            if current_status == 'processing':
                raise ValueError(f"Upload session {upload_id} is already being processed")

            # Reject attempts to re-process uploads in terminal states
            if current_status in {'finalizing', 'completed', 'failed'}:
                raise ValueError(
                    f"Upload session {upload_id} is {current_status} and cannot be resumed"
                )

            upload_session['status'] = 'processing'
            upload_session['processing_thread'] = threading.current_thread().ident
            
            try:
                result = self.uploader.process_stream(upload_session, stream_generator, resume_from=resume_from)
                print(f"🔒 THREAD SAFETY: Processing completed successfully for {upload_id}")
                return result
            except Exception as e:
                print(f"🔒 THREAD SAFETY: Processing failed for {upload_id}: {e}")
                upload_session['status'] = 'failed'
                raise
            finally:
                # Clean up completed/failed uploads
                with self.upload_lock:
                    if upload_id in self.active_uploads:
                        session_status = self.active_uploads[upload_id]['status']
                        if session_status in ['completed', 'failed']:
                            # Keep for a short time for status queries, but could be cleaned up
                            print(f"🔒 THREAD SAFETY: Session {upload_id} marked as {session_status}")
                            
                        # Clean up session lock for completed uploads
                        if session_status == 'completed' and upload_id in self.session_locks:
                            del self.session_locks[upload_id]
                            print(f"🔒 THREAD SAFETY: Cleaned up session lock for {upload_id}")
    
    def get_upload_status(self, upload_id: str) -> Optional[Dict[str, Any]]:
        """
        Get the status of an upload session
        """
        with self.upload_lock:
            return self.active_uploads.get(upload_id)

    def resume_upload_stream(self, upload_id: str, stream_generator) -> Dict[str, Any]:
        """Resume a previously started upload"""
        status = self.get_upload_status(upload_id)
        if not status:
            raise ValueError(f"Upload session {upload_id} not found")

        current_status = status.get('status')
        if current_status in {'finalizing', 'completed', 'failed'}:
            raise ValueError(
                f"Upload session {upload_id} is {current_status} and cannot be resumed"
            )

        resume_from = status.get('bytes_uploaded', 0)
        return self.process_upload_stream(upload_id, stream_generator, resume_from=resume_from)
    
    def cleanup_old_sessions(self, max_age_seconds: int = 3600) -> None:
        """
        Clean up old upload sessions
        """
        current_time = time.time()

        with self.upload_lock:
            expired_sessions = []
            for upload_id, session in self.active_uploads.items():
                status = session.get('status')
                last_activity = session.get('last_activity', session.get('created_at', 0))
                if status == 'completed':
                    last_activity = session.get('completed_at', last_activity)
                elif status == 'failed':
                    last_activity = session.get('failed_at', last_activity)
                elif status == 'aborted':
                    last_activity = session.get('aborted_at', last_activity)
                if current_time - last_activity > max_age_seconds:
                    expired_sessions.append(upload_id)

            for upload_id in expired_sessions:
                del self.active_uploads[upload_id]
                self.session_locks.pop(upload_id, None)
                print(f"Cleaned up expired upload session: {upload_id}")

        gc.collect()
