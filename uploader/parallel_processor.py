"""
Parallel Chunk Processor for Notion API uploads
Handles concurrent upload of file chunks with proper memory management
"""

import io
import threading
import concurrent.futures
from typing import Optional, Dict, Any
import time
import hashlib
import secrets

# Notion API constants
SINGLE_PART_THRESHOLD = 20 * 1024 * 1024  # 20 MiB
MULTIPART_CHUNK_SIZE = 5 * 1024 * 1024    # 5 MiB for multipart uploads

class ParallelChunkProcessor:
    """Handles parallel upload of file chunks to Notion API"""
    
    def __init__(self, max_workers=4, notion_uploader=None, upload_session=None, socketio=None):
        self.max_workers = max_workers
        self.notion_uploader = notion_uploader
        self.upload_session = upload_session
        self.socketio = socketio
        self.chunk_size = 5 * 1024 * 1024  # 5MB chunks for Notion API
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        self.semaphore = threading.Semaphore(max_workers)
        self.part_futures = {}
        self.completed_parts = set()
        self.upload_error = None
        self.lock = threading.Lock()
        
    def process_stream(self, stream_generator):
        """Process incoming stream with parallel chunk uploads"""
        try:
            print(f"DEBUG: Starting parallel processing for upload {self.upload_session['upload_id']}")
            
            # Initialize multipart upload
            multipart_info = self._initialize_multipart_upload()
            
            buffer = io.BytesIO()
            buffer_size = 0
            part_number = 1
            bytes_received = 0
            
            for chunk in stream_generator:
                if not chunk:
                    break
                
                buffer.write(chunk)
                buffer_size += len(chunk)
                bytes_received += len(chunk)
                self.upload_session['hasher'].update(chunk)
                
                # Process complete 5MB chunks
                while buffer_size >= self.chunk_size:
                    # Extract exactly 5MB
                    buffer.seek(0)
                    part_data = buffer.read(self.chunk_size)
                    
                    # Keep remaining data
                    remaining_data = buffer.read()
                    buffer = io.BytesIO()
                    buffer.write(remaining_data)
                    buffer_size = len(remaining_data)
                    
                    # Submit chunk for parallel upload
                    self._submit_chunk_upload(part_number, part_data, multipart_info)
                    part_number += 1
                    
                    # Update progress
                    self._update_progress(bytes_received)
            
            # Upload final chunk if any data remains
            if buffer_size > 0:
                buffer.seek(0)
                final_data = buffer.read()
                self._submit_chunk_upload(part_number, final_data, multipart_info, is_final=True)
            
            print(f"DEBUG: All chunks submitted, waiting for completion...")
            
            # Wait for all uploads to complete
            self._wait_for_all_uploads()
            
            print(f"DEBUG: All chunks uploaded, completing multipart upload...")
            
            # Complete multipart upload
            complete_result = self.notion_uploader.complete_multipart_upload(
                multipart_info['id']
            )
            
            print(f"DEBUG: Multipart upload completed successfully")
            
            return {
                'file_upload_id': multipart_info['id'],
                'status': 'completed',
                'result': complete_result
            }
            
        except Exception as e:
            print(f"ERROR: Parallel processing failed: {e}")
            self._abort_multipart_upload(multipart_info if 'multipart_info' in locals() else None)
            raise
        finally:
            self.executor.shutdown(wait=False)
    
    def _initialize_multipart_upload(self):
        """Initialize multipart upload with Notion"""
        total_parts = (self.upload_session['file_size'] + self.chunk_size - 1) // self.chunk_size
        
        print(f"DEBUG: Initializing multipart upload with {total_parts} parts")
        
        multipart_info = self.notion_uploader.create_file_upload(
            content_type='text/plain',
            filename='file.txt',  # Always use file.txt for Notion API
            mode='multi_part',
            number_of_parts=total_parts
        )
        
        self.upload_session['multipart_upload_id'] = multipart_info['id']
        self.upload_session['total_parts'] = total_parts
        
        print(f"DEBUG: Multipart upload initialized with ID: {multipart_info['id']}")
        
        return multipart_info
    
    def _submit_chunk_upload(self, part_number, chunk_data, multipart_info, is_final=False):
        """Submit a chunk for parallel upload"""
        if self.upload_error:
            return
        
        print(f"DEBUG: Submitting part {part_number} for upload ({len(chunk_data)} bytes)")
        
        future = self.executor.submit(
            self._upload_chunk_with_semaphore,
            part_number, chunk_data, multipart_info
        )
        
        with self.lock:
            self.part_futures[part_number] = future
    
    def _upload_chunk_with_semaphore(self, part_number, chunk_data, multipart_info):
        """Upload single chunk with concurrency control"""
        with self.semaphore:
            try:
                print(f"DEBUG: Uploading part {part_number} to Notion ({len(chunk_data)} bytes)")
                
                result = self.notion_uploader.send_file_part(
                    file_upload_id=multipart_info['id'],
                    part_number=part_number,
                    chunk_data=chunk_data,
                    filename='file.txt',
                    content_type='text/plain',
                    bytes_uploaded_so_far=part_number * len(chunk_data),
                    total_bytes=self.upload_session['file_size'],
                    total_parts=self.upload_session['total_parts'],
                    session_id=""
                )
                
                with self.lock:
                    self.completed_parts.add(part_number)
                
                print(f"DEBUG: Part {part_number} uploaded successfully")
                return result
                
            except Exception as e:
                print(f"ERROR: Part {part_number} upload failed: {e}")
                with self.lock:
                    self.upload_error = e
                raise
    
    def _wait_for_all_uploads(self):
        """Wait for all parallel uploads to complete"""
        for part_number, future in self.part_futures.items():
            try:
                future.result()  # Will raise if upload failed
            except Exception as e:
                raise Exception(f"Part {part_number} failed: {str(e)}")
    
    def _update_progress(self, bytes_received):
        """Update upload progress via SocketIO"""
        if self.socketio:
            progress = min(90, (bytes_received / self.upload_session['file_size']) * 90)
            self.socketio.emit('upload_progress', {
                'upload_id': self.upload_session['upload_id'],
                'bytes_uploaded': bytes_received,
                'total_size': self.upload_session['file_size'],
                'progress': progress,
                'completed_parts': len(self.completed_parts),
                'total_parts': self.upload_session.get('total_parts', 0),
                'status': 'uploading'
            })
    
    def _abort_multipart_upload(self, multipart_info):
        """Abort multipart upload on error"""
        try:
            if multipart_info and 'id' in multipart_info:
                print(f"DEBUG: Aborting multipart upload {multipart_info['id']}")
                
                # Cancel all pending uploads
                for future in self.part_futures.values():
                    future.cancel()
                
                # Attempt to abort multipart upload
                # Note: Notion API may not have explicit abort endpoint
                pass
        except Exception as e:
            print(f"Warning: Failed to abort multipart upload: {e}")


class MemoryManager:
    """Manages memory usage during file uploads"""
    
    def __init__(self, max_memory_mb=150):
        self.max_memory_bytes = max_memory_mb * 1024 * 1024
        self.current_memory_usage = 0
        self.memory_lock = threading.Lock()
        self.chunk_registry = {}  # Track active chunks
        
    def allocate_chunk_memory(self, chunk_id, chunk_size):
        """Allocate memory for a chunk and check limits"""
        with self.memory_lock:
            if self.current_memory_usage + chunk_size > self.max_memory_bytes:
                raise MemoryError(f"Memory limit exceeded: {self.current_memory_usage + chunk_size} bytes")
            
            self.chunk_registry[chunk_id] = chunk_size
            self.current_memory_usage += chunk_size
            
    def free_chunk_memory(self, chunk_id):
        """Free memory for a completed chunk"""
        with self.memory_lock:
            if chunk_id in self.chunk_registry:
                chunk_size = self.chunk_registry.pop(chunk_id)
                self.current_memory_usage = max(0, self.current_memory_usage - chunk_size)
                
    def get_memory_stats(self):
        """Get current memory usage statistics"""
        with self.memory_lock:
            return {
                'tracked_usage': self.current_memory_usage,
                'active_chunks': len(self.chunk_registry),
                'max_allowed': self.max_memory_bytes
            }


def generate_salt(length=32):
    """Generate cryptographic salt for file hashing"""
    return secrets.token_hex(length)


def calculate_salted_hash(file_hash, salt):
    """Calculate salted SHA-512 hash for file"""
    return hashlib.sha512((file_hash + salt).encode()).hexdigest()