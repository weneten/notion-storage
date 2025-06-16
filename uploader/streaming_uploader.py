"""
Streaming File Uploader for Notion API
Implements continuous streaming upload with proper Notion API compliance
"""

import os
import io
import threading
import time
import hashlib
import uuid
import secrets
from typing import Optional, Callable, Dict, Any, List
import requests
from flask import Response
from flask_socketio import SocketIO
from .notion_uploader import NotionFileUploader
from .parallel_processor import ParallelChunkProcessor, generate_salt, calculate_salted_hash


class NotionStreamingUploader:
    """
    Handles streaming file uploads with automatic single-part/multi-part decision making
    based on Notion API specifications.
    """
      # Notion API constants
    SINGLE_PART_THRESHOLD = 20 * 1024 * 1024  # 20 MiB
    MULTIPART_CHUNK_SIZE = 5 * 1024 * 1024    # 5 MiB for multipart uploads
    
    def __init__(self, api_token: str, socketio: Optional[SocketIO] = None, notion_uploader: Optional[NotionFileUploader] = None):
        self.api_token = api_token
        self.socketio = socketio
        self.notion_uploader = notion_uploader  # Use existing Notion uploader for actual API calls
        
    def create_upload_session(self, filename: str, file_size: int, user_database_id: str, 
                            progress_callback: Optional[Callable] = None) -> Dict[str, Any]:
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
            'is_multipart': is_multipart,
            'progress_callback': progress_callback,
            'created_at': time.time(),
            'status': 'initialized',
            'bytes_uploaded': 0,
            'hasher': hashlib.sha512()  # For file integrity
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
    
    def complete_upload_with_database_integration(self, upload_session, notion_result):
        """Complete upload and integrate with databases"""
        try:
            print(f"DEBUG: Starting database integration for upload {upload_session['upload_id']}")
            
            # Generate salt for public access
            salt = generate_salt()
            
            # Calculate file hash
            file_hash = upload_session['hasher'].hexdigest()
            salted_hash = calculate_salted_hash(file_hash, salt)
            
            print(f"DEBUG: File hash calculated: {file_hash[:16]}...")
            print(f"DEBUG: Salted hash: {salted_hash[:16]}...")
            
            # Add to user database
            user_db_result = self.notion_uploader.add_file_to_user_database(
                database_id=upload_session['user_database_id'],
                filename=upload_session['filename'],
                file_size=upload_session['file_size'],
                file_hash=salted_hash,
                file_upload_id=notion_result.get('file_upload_id'),
                original_filename=upload_session['filename'],
                salt=salt
            )
            
            print(f"DEBUG: Added to user database with ID: {user_db_result['id']}")
            
            # Add to global index
            if self.notion_uploader.global_file_index_db_id:
                self.notion_uploader.add_file_to_index(
                    salted_sha512_hash=salted_hash,
                    file_page_id=user_db_result['id'],
                    user_database_id=upload_session['user_database_id'],
                    original_filename=upload_session['filename'],
                    is_public=False
                )
                print(f"DEBUG: Added to global file index")
            else:
                print(f"WARNING: Global file index DB ID not configured, skipping global index")
            
            return {
                'file_id': user_db_result['id'],
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
    
    def process_stream(self, upload_session: Dict[str, Any], stream_generator) -> Dict[str, Any]:
        """
        Process the incoming file stream and handle upload based on file size
        """
        try:
            print(f"DEBUG: Starting stream processing for upload {upload_session['upload_id']}")
            print(f"DEBUG: File: {upload_session['filename']}, Size: {upload_session['file_size']}, Multipart: {upload_session['is_multipart']}")
            
            if upload_session['is_multipart']:
                result = self._process_multipart_stream(upload_session, stream_generator)
            else:
                result = self._process_single_part_stream(upload_session, stream_generator)
            
            print(f"DEBUG: Stream processing completed successfully for upload {upload_session['upload_id']}")
            return result
            
        except Exception as e:
            print(f"ERROR: Stream processing failed for upload {upload_session['upload_id']}: {str(e)}")
            print(f"ERROR: Exception type: {type(e).__name__}")
            
            # Update upload session with error info
            upload_session.update({
                'status': 'failed',
                'error': str(e),
                'error_type': type(e).__name__,
                'failed_at': time.time()
            })
            
            raise
    
    def _process_single_part_stream(self, upload_session: Dict[str, Any], stream_generator) -> Dict[str, Any]:
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
            
            # Upload the complete file to Notion
            buffer.seek(0)
            notion_result = self._upload_to_notion_single_part(
                upload_session['user_database_id'],
                upload_session['filename'],
                buffer,
                bytes_received
            )
            
            # Database integration
            if self.socketio:
                self.socketio.emit('upload_progress', {
                    'upload_id': upload_session['upload_id'],
                    'status': 'finalizing',
                    'progress': 95
                })
                
            final_result = self.complete_upload_with_database_integration(
                upload_session, notion_result['result']
            )
            
            upload_session.update({
                'status': 'completed',
                'bytes_uploaded': bytes_received,
                'file_hash': final_result['file_hash'],
                'notion_file_id': final_result['file_id'],
                'completed_at': time.time()
            })
            
            return final_result
            
        except Exception as e:
            upload_session.update({
                'status': 'failed',
                'error': str(e),
                'failed_at': time.time()
            })
            raise
    
    def _process_multipart_stream(self, upload_session: Dict[str, Any], stream_generator) -> Dict[str, Any]:
        """
        Handle multipart upload (> 20 MiB files) with parallel 5 MiB chunks
        """
        upload_session['status'] = 'uploading'
        
        try:
            print(f"DEBUG: Starting multipart upload with parallel processing")
            
            # Use parallel chunk processor
            parallel_processor = ParallelChunkProcessor(
                max_workers=4,
                notion_uploader=self.notion_uploader,
                upload_session=upload_session,
                socketio=self.socketio
            )
            
            # Process stream with parallel uploads
            notion_result = parallel_processor.process_stream(stream_generator)
            
            print(f"DEBUG: Parallel upload completed, starting database integration")
            
            # Database integration
            if self.socketio:
                self.socketio.emit('upload_progress', {
                    'upload_id': upload_session['upload_id'],
                    'status': 'finalizing',
                    'progress': 95
                })
                
            final_result = self.complete_upload_with_database_integration(
                upload_session, notion_result
            )
            
            upload_session.update({
                'status': 'completed',
                'bytes_uploaded': upload_session['file_size'],
                'file_hash': final_result['file_hash'],
                'notion_file_id': final_result['file_id'],
                'completed_at': time.time()
            })
            
            print(f"DEBUG: Multipart upload with database integration completed successfully")
            
            return final_result
            
        except Exception as e:
            print(f"ERROR: Multipart upload failed: {e}")
            upload_session.update({
                'status': 'failed',
                'error': str(e),
                'failed_at': time.time()
            })
            raise

    def _upload_to_notion_single_part(self, user_database_id: str, filename: str, 
                                    file_buffer: io.BytesIO, file_size: int) -> Dict[str, Any]:
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
                    original_filename=filename  # Keep original filename for database
                )
                
                return {
                    'file_upload_id': result.get('file_upload_id', str(uuid.uuid4())),
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
        self.upload_lock = threading.Lock()
    
    def create_upload_session(self, filename: str, file_size: int, user_database_id: str,
                            progress_callback: Optional[Callable] = None) -> str:
        """
        Create a new upload session and return the upload ID
        """
        session = self.uploader.create_upload_session(
            filename, file_size, user_database_id, progress_callback
        )
        
        upload_id = session['upload_id']
        
        with self.upload_lock:
            self.active_uploads[upload_id] = session
        
        return upload_id
    
    def process_upload_stream(self, upload_id: str, stream_generator) -> Dict[str, Any]:
        """
        Process an upload stream for the given upload ID
        """
        with self.upload_lock:
            if upload_id not in self.active_uploads:
                raise ValueError(f"Upload session {upload_id} not found")
            
            upload_session = self.active_uploads[upload_id]
        
        try:
            result = self.uploader.process_stream(upload_session, stream_generator)
            return result
        finally:
            # Clean up completed/failed uploads
            with self.upload_lock:
                if upload_id in self.active_uploads:
                    session_status = self.active_uploads[upload_id]['status']
                    if session_status in ['completed', 'failed']:
                        # Keep for a short time for status queries, but could be cleaned up
                        pass
    
    def get_upload_status(self, upload_id: str) -> Optional[Dict[str, Any]]:
        """
        Get the status of an upload session
        """
        with self.upload_lock:
            return self.active_uploads.get(upload_id)
    
    def cleanup_old_sessions(self, max_age_seconds: int = 3600) -> None:
        """
        Clean up old upload sessions
        """
        current_time = time.time()
        
        with self.upload_lock:
            expired_sessions = [
                upload_id for upload_id, session in self.active_uploads.items()
                if current_time - session['created_at'] > max_age_seconds
            ]
            
            for upload_id in expired_sessions:
                del self.active_uploads[upload_id]
                print(f"Cleaned up expired upload session: {upload_id}")
