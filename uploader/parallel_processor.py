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

# Import resource management
try:
    from .resource_manager import resource_manager, backpressure_manager
except ImportError:
    # Fallback if resource manager not available
    resource_manager = None
    backpressure_manager = None

# Notion API constants
SINGLE_PART_THRESHOLD = 20 * 1024 * 1024  # 20 MiB
MULTIPART_CHUNK_SIZE = 5 * 1024 * 1024    # 5 MiB for multipart uploads

class ParallelChunkProcessor:
    """Handles parallel upload of file chunks to Notion API with resource awareness"""
    
    def __init__(self, max_workers=4, notion_uploader=None, upload_session=None, socketio=None):
        # Get optimal worker count from resource manager
        if resource_manager:
            optimal_workers = resource_manager.get_optimal_worker_count()
            self.max_workers = min(max_workers, optimal_workers)
            print(f"🎯 Adaptive workers: requested {max_workers}, using {self.max_workers} based on resources")
        else:
            self.max_workers = max_workers
            
        self.notion_uploader = notion_uploader
        self.upload_session = upload_session
        self.socketio = socketio
        self.chunk_size = 5 * 1024 * 1024  # 5MB chunks for Notion API
        
        # Create executor with adaptive worker count
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers)
        self.semaphore = threading.Semaphore(self.max_workers)
        self.part_futures = {}
        self.completed_parts = set()
        self.upload_error = None
        self.lock = threading.Lock()
        
        # Resource tracking
        self.memory_usage_tracker = {}
        self.last_resource_check = 0
        self.resource_check_interval = 10  # Check every 10 seconds
        
    def process_stream(self, stream_generator):
        """Process incoming stream with resource-aware parallel chunk uploads and checkpointing"""
        upload_id = self.upload_session.get('upload_id', 'unknown')
        checkpoint_key = None
        
        # Register upload start with resource manager
        if resource_manager:
            resource_manager.register_upload_start(upload_id)
        
        try:
            print(f"🚀 Starting resilient parallel processing for upload {upload_id}")
            
            # Check if we should proceed with this upload
            if resource_manager:
                file_size = self.upload_session.get('file_size', 0)
                should_accept, reason = resource_manager.should_accept_upload(file_size)
                if not should_accept:
                    raise Exception(f"Upload rejected due to resource constraints: {reason}")
            
            # Initialize multipart upload
            multipart_info = self._initialize_multipart_upload()
            
            # Create checkpoint for resume capability
            if checkpoint_manager:
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
                
                # Check resources periodically and apply backpressure if needed
                if resource_manager and part_number % 10 == 0:  # Every 10 chunks (50MB)
                    self._check_and_apply_backpressure(part_number)
                
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
                    
                    # Submit chunk for parallel upload with resource awareness
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
            
            # Mark upload as completed in checkpoint
            if checkpoint_manager and checkpoint_key:
                checkpoint_manager.complete_upload(checkpoint_key)
            
            print(f"🎉 Multipart upload completed successfully")
            
            return {
                'file_upload_id': multipart_info['id'],
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
            # Always unregister upload and cleanup
            if resource_manager:
                resource_manager.register_upload_end(upload_id)
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
    
    def _submit_chunk_upload_resource_aware(self, part_number, chunk_data, multipart_info, is_final=False):
        """Submit a chunk for parallel upload with resource awareness"""
        if self.upload_error:
            return
        
        # Check resources before submitting
        if resource_manager:
            # Dynamically adjust worker count if needed
            optimal_workers = resource_manager.get_optimal_worker_count()
            if optimal_workers != self.max_workers:
                self._adjust_worker_count(optimal_workers)
        
        print(f"📤 Submitting part {part_number} for upload ({len(chunk_data)} bytes)")
        
        future = self.executor.submit(
            self._upload_chunk_with_semaphore,
            part_number, chunk_data, multipart_info
        )
        
        with self.lock:
            self.part_futures[part_number] = future
            
    def _submit_chunk_upload(self, part_number, chunk_data, multipart_info, is_final=False):
        """Legacy method - redirects to resource-aware version"""
        return self._submit_chunk_upload_resource_aware(part_number, chunk_data, multipart_info, is_final)
    
    def _check_and_apply_backpressure(self, part_number):
        """Check resources and apply backpressure if needed"""
        if not resource_manager:
            return
            
        current_time = time.time()
        if current_time - self.last_resource_check < self.resource_check_interval:
            return
            
        self.last_resource_check = current_time
        
        # Get resource stats
        stats = resource_manager.get_resource_stats()
        
        # Log resource usage for critical parts
        if part_number >= 200:  # Around 1GB mark where issues typically occur
            print(f"🎯 CRITICAL_PART_RESOURCES: Part {part_number}")
            print(f"   Memory: {stats['memory_usage_mb']:.1f}MB ({stats['memory_usage_percent']:.1f}%)")
            print(f"   CPU: {stats['cpu_usage_percent']:.1f}%")
            print(f"   Active uploads: {stats['active_uploads']}")
            print(f"   Workers: {stats['optimal_workers']}")
        
        # Apply backpressure if needed
        delay = resource_manager.apply_backpressure_delay()
        if delay > 0:
            print(f"⏳ Applying backpressure: {delay}s delay at part {part_number}")
            time.sleep(delay)
            
    def _adjust_worker_count(self, new_worker_count):
        """Dynamically adjust worker count based on resource availability"""
        if new_worker_count == self.max_workers:
            return
            
        print(f"🔄 Adjusting worker count: {self.max_workers} → {new_worker_count}")
        
        # Update semaphore capacity
        if new_worker_count > self.max_workers:
            # Increase capacity
            for _ in range(new_worker_count - self.max_workers):
                self.semaphore.release()
        else:
            # Decrease capacity by acquiring permits
            for _ in range(self.max_workers - new_worker_count):
                try:
                    self.semaphore.acquire(blocking=False)
                except:
                    pass  # If can't acquire, that's fine
        
        self.max_workers = new_worker_count
    
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
    
    def _upload_chunk_with_semaphore(self, part_number, chunk_data, multipart_info):
        """Legacy method - redirects to checkpoint-aware version"""
        return self._upload_chunk_with_checkpoint(part_number, chunk_data, multipart_info, None)
    
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