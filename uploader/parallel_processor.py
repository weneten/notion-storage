"""
Parallel Chunk Processor for Notion API uploads
Handles concurrent upload of file chunks with resource-aware management
"""

import io
import threading
import concurrent.futures
from typing import Optional, Dict, Any
import time
import hashlib
import secrets

# Simple configuration for upload limits
MAX_WORKERS = 10
CHUNK_SIZE = 5 * 1024 * 1024  # 5MB chunks for Notion API

# Import checkpoint management
try:
    from .checkpoint_manager import checkpoint_manager, create_checkpoint_key
except ImportError:
    # Fallback if checkpoint manager not available
    checkpoint_manager = None
    create_checkpoint_key = None


# Notion API constants
SINGLE_PART_THRESHOLD = 20 * 1024 * 1024  # 20 MiB
MULTIPART_CHUNK_SIZE = 5 * 1024 * 1024    # 5 MiB for multipart uploads

# Helper for DB patching after part upload (used by streaming_uploader.py)
def patch_db_entry_is_visible_and_file_data(notion_uploader, db_entry_id, file_list, is_visible):
    """
    Patch a Notion DB entry to set is_visible and file_data properties.
    """
    notion_uploader.update_user_properties(db_entry_id, {
        "is_visible": {"checkbox": is_visible},
        "file_data": {"files": file_list}
    })

# Note: The main chunk upload logic is unchanged. After each part upload, streaming_uploader.py will call this helper to patch the DB entry as required by the split plan.

class ParallelChunkProcessor:
    """Handles parallel upload of file chunks to Notion API with resource awareness"""
    
    def __init__(self, max_workers=4, notion_uploader=None, upload_session=None, socketio=None):
        self.max_workers = min(max_workers, MAX_WORKERS)
        self.notion_uploader = notion_uploader
        self.upload_session = upload_session
        self.socketio = socketio
        self.chunk_size = CHUNK_SIZE
        
        # Create executor with fixed worker count
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers)
        self.semaphore = threading.Semaphore(self.max_workers)
        self.part_futures = {}
        self.completed_parts = set()
        self.upload_error = None
        self.lock = threading.Lock()
        
        print(f"🎯 Parallel processor initialized with {self.max_workers} workers")
        
    def process_stream(self, stream_generator):
        """Process incoming stream with parallel chunk uploads and checkpointing"""
        upload_id = self.upload_session.get('upload_id', 'unknown')
        checkpoint_key = None
        
        try:
            print(f"🚀 Starting parallel processing for upload {upload_id}")
            
            # Initialize multipart upload
            multipart_info = self._initialize_multipart_upload()
            
            # Create checkpoint for resume capability
            if checkpoint_manager and create_checkpoint_key:
                total_parts = self.upload_session.get('total_parts', 0)
                file_hash = self.upload_session.get('hasher', hashlib.sha512()).hexdigest()[:16]
                checkpoint_key = create_checkpoint_key(upload_id, file_hash)
                checkpoint_manager.create_checkpoint(self.upload_session, multipart_info['id'], total_parts)
                print(f"📋 Checkpoint created: {checkpoint_key}")
            
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
                    self._submit_chunk_upload_with_checkpoint(part_number, part_data, multipart_info, checkpoint_key)
                    part_number += 1
                    
                    # Update progress
                    self._update_progress(bytes_received)
            
            # Upload final chunk if any data remains
            if buffer_size > 0:
                buffer.seek(0)
                final_data = buffer.read()
                self._submit_chunk_upload_with_checkpoint(part_number, final_data, multipart_info, checkpoint_key, is_final=True)
            
            print(f"📦 All chunks submitted ({part_number} parts), waiting for completion...")
            
            # Wait for all uploads to complete
            self._wait_for_all_uploads()
            
            print(f"✅ All chunks uploaded, completing multipart upload...")
            
            # Complete multipart upload
            complete_result = self.notion_uploader.complete_multipart_upload(
                multipart_info['id']
            )
            
            # Extract the actual file ID from completion response for permanent URL generation
            actual_file_id = complete_result.get('file', {}).get('id', multipart_info['id'])
            
            # Mark upload as completed in checkpoint
            if checkpoint_manager and checkpoint_key:
                checkpoint_manager.complete_upload(checkpoint_key)
            
            print(f"🎉 Multipart upload completed successfully")
            
            return {
                'file_upload_id': actual_file_id,  # Use the correct file ID for permanent URL generation
                'status': 'completed',
                'result': complete_result
            }
            
        except Exception as e:
            print(f"❌ ERROR: Parallel processing failed: {e}")
            # Don't delete checkpoint on failure - it can be used for resume
            if checkpoint_key:
                print(f"📋 Checkpoint preserved for resume: {checkpoint_key}")
            self._abort_multipart_upload(multipart_info if 'multipart_info' in locals() else None)
            raise
        finally:
            # Cleanup executor
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
    
    def _submit_chunk_upload_with_checkpoint(self, part_number, chunk_data, multipart_info, checkpoint_key, is_final=False):
        """Submit a chunk for parallel upload with checkpoint tracking"""
        if self.upload_error:
            return
        
        print(f"📤 Submitting part {part_number} for upload with checkpoint ({len(chunk_data)} bytes)")
        
        future = self.executor.submit(
            self._upload_chunk_with_checkpoint,
            part_number, chunk_data, multipart_info, checkpoint_key
        )
        
        with self.lock:
            self.part_futures[part_number] = future
    
    
    def _upload_chunk_with_checkpoint(self, part_number, chunk_data, multipart_info, checkpoint_key):
        """Upload single chunk with checkpoint tracking and enhanced resilience"""
        # Import diagnostics
        try:
            from diagnostic_logs import timeout_diagnostics
        except ImportError:
            timeout_diagnostics = None
            
        # Log parallel processor state for critical parts
        if timeout_diagnostics and part_number >= 200:
            timeout_diagnostics.log_parallel_processor_state(self)
            
        with self.semaphore:
            try:
                print(f"🔄 Uploading part {part_number} to Notion ({len(chunk_data)} bytes)")
                
                # Log memory snapshot for high part numbers
                if timeout_diagnostics and part_number % 25 == 0:
                    timeout_diagnostics.log_memory_snapshot(f"parallel_upload_start", part_number)
                
                # Upload the part with enhanced resilience
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
                
                # Track completion
                with self.lock:
                    self.completed_parts.add(part_number)
                
                # Update checkpoint for this completed part
                if checkpoint_manager and checkpoint_key:
                    checkpoint_manager.update_checkpoint(checkpoint_key, part_number)
                
                print(f"✅ Part {part_number} uploaded and checkpointed successfully")
                
                # Log connection pool stats periodically
                if timeout_diagnostics and part_number % 50 == 0:
                    timeout_diagnostics.log_connection_pool_stats()
                    
                return result
                
            except Exception as e:
                print(f"❌ ERROR: Part {part_number} upload failed: {e}")
                
                # Enhanced error logging for timeout-related issues
                error_msg = str(e).lower()
                if any(keyword in error_msg for keyword in ['timeout', '504', 'gateway', 'connection', 'reset']):
                    print(f"🚨 TIMEOUT_ERROR_DETECTED: Part {part_number} failed with timeout-related error: {e}")
                    if timeout_diagnostics:
                        timeout_diagnostics.log_memory_snapshot(f"timeout_error", part_number)
                        timeout_diagnostics.log_parallel_processor_state(self)
                
                with self.lock:
                    self.upload_error = e
                raise
    
    def _wait_for_all_uploads(self):
        """Wait for all parallel uploads to complete"""
        try:
            from diagnostic_logs import timeout_diagnostics
        except ImportError:
            timeout_diagnostics = None
            
        print(f"DEBUG: Waiting for {len(self.part_futures)} upload futures to complete...")
        
        for part_number, future in self.part_futures.items():
            try:
                # Special logging for the critical part 227 area
                if part_number >= 220:
                    print(f"🎯 CRITICAL_PART_WAIT: Waiting for part {part_number} to complete...")
                    if timeout_diagnostics:
                        timeout_diagnostics.log_parallel_processor_state(self)
                
                future.result()  # Will raise if upload failed
                
                if part_number >= 220:
                    print(f"✅ CRITICAL_PART_SUCCESS: Part {part_number} completed successfully")
                    
            except Exception as e:
                error_msg = f"Part {part_number} failed: {str(e)}"
                print(f"❌ PART_FAILURE: {error_msg}")
                
                # Enhanced logging for part 227 area failures
                if part_number >= 220:
                    print(f"🚨 CRITICAL_PART_FAILURE: Part {part_number} in critical range failed!")
                    if timeout_diagnostics:
                        timeout_diagnostics.log_memory_snapshot(f"critical_part_failure", part_number)
                        
                raise Exception(error_msg)
    
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


def generate_salt(length=32):
    """Generate cryptographic salt for file hashing"""
    return secrets.token_hex(length)


def calculate_salted_hash(file_hash, salt):
    """Calculate salted SHA-512 hash for file"""
    return hashlib.sha512((file_hash + salt).encode()).hexdigest()