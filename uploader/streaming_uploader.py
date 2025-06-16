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
from typing import Optional, Callable, Dict, Any, List
import requests
from flask import Response
from flask_socketio import SocketIO
from .notion_uploader import NotionFileUploader


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
                
                # Update progress
                if upload_session['progress_callback']:
                    progress = (bytes_received / upload_session['file_size']) * 100
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
            result = self._upload_to_notion_single_part(
                upload_session['user_database_id'],
                upload_session['filename'],
                buffer,
                bytes_received
            )
            
            upload_session.update({
                'status': 'completed',
                'bytes_uploaded': bytes_received,
                'file_hash': upload_session['hasher'].hexdigest(),
                'notion_file_id': result.get('file_id'),
                'completed_at': time.time()
            })
            
            return upload_session
            
        except Exception as e:
            upload_session.update({
                'status': 'failed',
                'error': str(e),
                'failed_at': time.time()
            })
            raise
    
    def _process_multipart_stream(self, upload_session: Dict[str, Any], stream_generator) -> Dict[str, Any]:
        """
        Handle multipart upload (> 20 MiB files) with 5 MiB chunks
        """
        upload_session['status'] = 'uploading'
        
        buffer = io.BytesIO()
        buffer_size = 0
        part_number = 1
        bytes_received = 0
        
        try:
            for chunk in stream_generator:
                if not chunk:
                    break
                
                buffer.write(chunk)
                buffer_size += len(chunk)
                bytes_received += len(chunk)
                upload_session['hasher'].update(chunk)
                
                # Process complete 5MB chunks
                while buffer_size >= self.MULTIPART_CHUNK_SIZE:
                    # Extract exactly 5MB
                    buffer.seek(0)
                    part_data = buffer.read(self.MULTIPART_CHUNK_SIZE)
                    
                    # Keep remaining data
                    remaining_data = buffer.read()
                    buffer = io.BytesIO()
                    buffer.write(remaining_data)
                    buffer_size = len(remaining_data)
                      # Upload this part
                    self._upload_multipart_chunk(upload_session, part_number, part_data)
                    part_number += 1
                    
                    # Update progress
                    progress = (bytes_received / upload_session['file_size']) * 100
                    if upload_session['progress_callback']:
                        upload_session['progress_callback'](progress, bytes_received)
                    
                    # Emit progress via SocketIO if available
                    if self.socketio:
                        self.socketio.emit('upload_progress', {
                            'upload_id': upload_session['upload_id'],
                            'bytes_uploaded': bytes_received,
                            'total_size': upload_session['file_size'],
                            'progress': progress,
                            'current_part': part_number - 1,
                            'total_parts': upload_session['total_parts']
                        })
            
            # Upload final chunk if any data remains
            if buffer_size > 0:
                buffer.seek(0)
                final_data = buffer.read()
                self._upload_multipart_chunk(upload_session, part_number, final_data, is_final=True)
              # Complete multipart upload
            self._complete_multipart_upload(upload_session)
            
            upload_session.update({
                'status': 'completed',
                'bytes_uploaded': bytes_received,
                'file_hash': upload_session['hasher'].hexdigest(),
                'completed_at': time.time()
            })
            
            return upload_session
            
        except Exception as e:
            # Abort multipart upload on error
            self._abort_multipart_upload(upload_session)
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
                    'file_id': result.get('id', str(uuid.uuid4())),
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

    def _upload_multipart_chunk(self, upload_session: Dict[str, Any], part_number: int, 
                              chunk_data: bytes, is_final: bool = False) -> Dict[str, Any]:
        """
        Upload a single chunk immediately to Notion API (true streaming)
        """
        print(f"DEBUG: Immediately uploading part {part_number}, size: {len(chunk_data)} bytes, is_final: {is_final}")
        
        if self.notion_uploader:
            try:                # Initialize multipart upload if this is the first chunk
                if 'multipart_upload_id' not in upload_session:
                    print(f"DEBUG: Initializing multipart upload for {upload_session['filename']}")                    
                    # Calculate total parts
                    total_parts = upload_session['total_parts']
                    
                    # Create multipart upload with file.txt (required by Notion API)
                    notion_filename = "file.txt"  # Always use file.txt for Notion API
                    print(f"DEBUG: Using '{notion_filename}' for Notion API, real filename: {upload_session['filename']}")
                    
                    multipart_info = self.notion_uploader.create_file_upload(
                        content_type='text/plain',  # Required by Notion API for all files
                        filename=notion_filename,  # Use file.txt for API
                        mode='multi_part',
                        number_of_parts=total_parts                    )
                    
                    print(f"DEBUG: Created multipart upload with filename='{notion_filename}' for Notion API")
                    print(f"DEBUG: Using content-type 'text/plain' (required by Notion API)")
                    print(f"DEBUG: Original filename '{upload_session['filename']}' will be stored in database")
                    
                    upload_session['multipart_upload_id'] = multipart_info['id']
                    upload_session['multipart_info'] = multipart_info
                    upload_session['notion_filename'] = notion_filename  # Store for later use
                    print(f"DEBUG: Multipart upload initialized with ID: {multipart_info['id']}")
                
                # Upload this chunk immediately to Notion
                print(f"DEBUG: Sending part {part_number} to Notion API...")
                
                result = self.notion_uploader.send_file_part(
                    file_upload_id=upload_session['multipart_upload_id'],
                    part_number=part_number,
                    chunk_data=chunk_data,
                    filename=upload_session.get('notion_filename', 'file.txt'),  # Use file.txt for API
                    content_type='text/plain',  # Required by Notion API for all files
                    bytes_uploaded_so_far=part_number * self.MULTIPART_CHUNK_SIZE,
                    total_bytes=upload_session['file_size'],
                    total_parts=upload_session['total_parts'],
                    session_id=""
                )
                
                # Mark this part as completed
                upload_session['completed_parts'].add(part_number)
                upload_session['part_etags'][part_number] = result.get('etag', f"etag-{part_number}")
                
                print(f"DEBUG: Part {part_number} uploaded successfully to Notion! Memory freed. Total parts completed: {len(upload_session['completed_parts'])}")
                
                # Chunk data is automatically freed from memory after this function returns
                # No need to store it anywhere - true streaming!
                
                return {
                    'part_number': part_number,
                    'etag': result.get('etag', f"etag-{part_number}"),
                    'status': 'success',
                    'result': result
                }
                
            except Exception as e:
                print(f"ERROR: Failed to upload part {part_number} to Notion: {e}")
                raise
        else:
            print("ERROR: No notion_uploader available")
            raise Exception("NotionUploader not available")

    def _complete_multipart_upload(self, upload_session: Dict[str, Any]) -> Dict[str, Any]:
        """
        Complete the multipart upload - all chunks have already been uploaded to Notion
        """
        print(f"DEBUG: Completing multipart upload for {upload_session['filename']}")
        
        if 'multipart_upload_id' not in upload_session:
            raise Exception("No multipart upload ID found - upload was never initialized")
        
        multipart_upload_id = upload_session['multipart_upload_id']
        
        print(f"DEBUG: All {len(upload_session['completed_parts'])} parts already uploaded to Notion")
        print(f"DEBUG: Finalizing multipart upload with ID: {multipart_upload_id}")
        
        if self.notion_uploader:
            try:
                # Complete the multipart upload in Notion
                result = self.notion_uploader.complete_multipart_upload(multipart_upload_id)
                
                upload_session['notion_file_id'] = result.get('id', multipart_upload_id)
                print(f"DEBUG: Multipart upload completed successfully! File ID: {upload_session['notion_file_id']}")
                
                return {'status': 'completed', 'file_id': upload_session['notion_file_id'], 'result': result}
                
            except Exception as e:
                print(f"ERROR: Failed to complete multipart upload: {e}")                # Try to abort the upload
                try:
                    multipart_info = upload_session.get('multipart_info', {})
                    upload_url = multipart_info.get('upload_url', '')
                    self.notion_uploader._abort_multipart_upload(upload_url, multipart_upload_id)
                    print(f"DEBUG: Aborted multipart upload {multipart_upload_id}")
                except Exception as abort_error:
                    print(f"DEBUG: Failed to abort multipart upload {multipart_upload_id}: {abort_error}")
                raise
        else:
            print("ERROR: No notion_uploader available")
            raise Exception("NotionUploader not available")
    def _abort_multipart_upload(self, upload_session: Dict[str, Any]) -> None:
        """
        Abort the multipart upload in case of error
        """
        print(f"DEBUG: Aborting multipart upload for session {upload_session['upload_id']}")
        
        if 'multipart_upload_id' in upload_session and self.notion_uploader:
            try:
                multipart_info = upload_session.get('multipart_info', {})
                upload_url = multipart_info.get('upload_url', '')
                upload_id = upload_session['multipart_upload_id']
                
                self.notion_uploader._abort_multipart_upload(upload_url, upload_id)
                print(f"DEBUG: Successfully aborted multipart upload {upload_id}")
            except Exception as e:
                print(f"DEBUG: Failed to abort multipart upload: {e}")
        else:
            print(f"DEBUG: No multipart upload to abort or no notion_uploader available")


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
