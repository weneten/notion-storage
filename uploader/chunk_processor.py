import io
import threading
import concurrent.futures
import queue
from typing import Dict, Any, List, Tuple, Callable

# NOTE: This class is no longer used for concurrent uploads.
# The app.py file now uses direct threading for better concurrency.
# This class is kept for backward compatibility.
class ChunkProcessor:
    """
    Handles processing of file chunks with proper backpressure and concurrent uploads.
    """
    def __init__(self, max_concurrent_uploads=3, max_pending_chunks=5):
        """
        Initialize the chunk processor with limits for concurrent uploads and pending chunks.
        
        Args:
            max_concurrent_uploads (int): Maximum number of concurrent chunk uploads
            max_pending_chunks (int): Maximum number of chunks waiting to be uploaded
        """
        # We don't buffer chunks anymore - each chunk is processed as it comes
        self.lock = threading.Lock()
        self.upload_futures: Dict[int, concurrent.futures.Future] = {}  # {part_number: future}
        self.max_concurrent_uploads = max_concurrent_uploads
        self.max_pending_chunks = max_pending_chunks
        self.upload_error = None
        self.completed_parts = set()
        self.total_parts = 0
        self.bytes_uploaded = 0
        self.total_bytes = 0
        self.chunk_size = 5 * 1024 * 1024  # 5MB chunks for Notion API

    def process_chunk(self, part_number: int, chunk_data: bytes, 
                     executor: concurrent.futures.ThreadPoolExecutor, 
                     upload_func: Callable, 
                     total_size: int, 
                     is_last_chunk: bool = False) -> Dict[str, Any]:
        """
        Process a single chunk with proper handling of concurrency.
        
        Args:
            part_number: The part number being uploaded
            chunk_data: The chunk data to process
            executor: ThreadPoolExecutor for parallel uploads
            upload_func: Function to call for uploading chunks
            total_size: Total file size
            is_last_chunk: Whether this is the last chunk
            
        Returns:
            Dict containing upload result or status
        """
        if self.upload_error:
            raise self.upload_error

        with self.lock:
            # Update tracking information
            chunk_size = len(chunk_data)
            if self.total_bytes == 0:
                self.total_bytes = total_size
            
            # Handle backpressure - wait if we're at the concurrent limit
            while len(self.upload_futures) >= self.max_concurrent_uploads:
                # Release lock while waiting
                self.lock.release()
                try:
                    # Wait for any upload to complete
                    self._wait_for_upload_completion()
                finally:
                    # Re-acquire lock
                    self.lock.acquire()
            
            # Submit chunk for upload
            try:
                # For the last chunk, we process it immediately instead of queueing
                if is_last_chunk:
                    try:
                        # Process last chunk directly
                        result = upload_func(part_number, chunk_data)
                        self.completed_parts.add(part_number)
                        self.bytes_uploaded += chunk_size
                        return result
                    except Exception as e:
                        self.upload_error = e
                        raise
                else:
                    # Queue the chunk for concurrent processing
                    future = executor.submit(upload_func, part_number, chunk_data)
                    self.upload_futures[part_number] = future
                    
                    # Update part tracking
                    if part_number > self.total_parts:
                        self.total_parts = part_number
                    
                    return {"status": "queued", "part_number": part_number}
            except Exception as e:
                self.upload_error = e
                raise

    def _check_upload_futures(self) -> List[int]:
        """
        Check completed futures and handle any errors.
        
        Returns:
            List of completed part numbers
        """
        completed_parts = []
        
        with self.lock:
            # Check all futures
            for part_number, future in list(self.upload_futures.items()):
                if future.done():
                    try:
                        future.result()  # Will raise if upload failed
                        self.completed_parts.add(part_number)
                        self.bytes_uploaded += self.chunk_size
                        completed_parts.append(part_number)
                        # Remove from tracking
                        del self.upload_futures[part_number]
                    except Exception as e:
                        self.upload_error = e
                        # Remove from tracking
                        del self.upload_futures[part_number]
                        raise
        
        return completed_parts

    def _wait_for_upload_completion(self) -> None:
        """Wait for at least one upload to complete before continuing."""
        if not self.upload_futures:
            return

        # Try to find any completed futures
        completed = self._check_upload_futures()
        if completed:
            return
            
        # If no uploads completed yet, wait for the first one
        if self.upload_futures:
            # Sort by part number to prioritize earlier parts
            part_numbers = sorted(self.upload_futures.keys())
            if part_numbers:
                part_number = part_numbers[0]
                future = self.upload_futures[part_number]
                try:
                    future.result()  # This will block until completion
                    with self.lock:
                        self.completed_parts.add(part_number)
                        self.bytes_uploaded += self.chunk_size
                        del self.upload_futures[part_number]
                except Exception as e:
                    with self.lock:
                        del self.upload_futures[part_number]
                    self.upload_error = e
                    raise

    def get_upload_status(self) -> Dict[str, Any]:
        """Get the current upload status."""
        with self.lock:
            return {
                'completed_parts': len(self.completed_parts),
                'total_parts': self.total_parts,
                'bytes_uploaded': self.bytes_uploaded,
                'total_bytes': self.total_bytes,
                'active_uploads': len(self.upload_futures),
                'progress': (self.bytes_uploaded / self.total_bytes * 100) if self.total_bytes > 0 else 0
            }

    def wait_for_all_uploads(self) -> None:
        """Wait for all pending uploads to complete."""
        while True:
            with self.lock:
                if not self.upload_futures:
                    break
            
            self._wait_for_upload_completion()
