import threading
import queue
import asyncio
from collections import deque
from typing import Dict, Any, Tuple
import psutil

class AsyncUploader:
    """
    AsyncUploader manages concurrent uploads of file chunks to Notion.
    It maintains a queue of chunks to upload and processes them asynchronously.
    """
    def __init__(self, max_concurrent_uploads: int = 5):
        # Determine a safe level of concurrency based on available memory.
        # We assume each chunk is ~5MB and allocate twice that per worker to
        # account for overhead while uploading.
        available_mem = psutil.virtual_memory().available
        chunk_memory = 5 * 1024 * 1024 * 2  # ~10MB per upload slot
        safe_uploads = max(1, available_mem // chunk_memory)

        self.max_concurrent_uploads = min(max_concurrent_uploads, safe_uploads)
        # Limit queue size to twice the number of workers to bound memory usage
        self.chunk_queue = queue.Queue(maxsize=self.max_concurrent_uploads * 2)
        self.result_queue = queue.Queue()
        self.workers = []
        self.active_uploads = 0
        self.upload_error = None
        self.lock = threading.Lock()
        self.pending_parts = deque()
        self.completed_parts = set()

    def start_workers(self, upload_func):
        """Start upload worker threads"""
        for _ in range(self.max_concurrent_uploads):
            worker = threading.Thread(target=self._upload_worker, args=(upload_func,))
            worker.daemon = True
            worker.start()
            self.workers.append(worker)

    def _upload_worker(self, upload_func):
        """Worker thread that processes chunks from the queue"""
        while True:
            try:
                part_data = self.chunk_queue.get()
                if part_data is None:  # Sentinel value to stop the worker
                    self.chunk_queue.task_done()
                    break

                part_number, chunk_data, current_bytes = part_data
                try:
                    result = upload_func(part_number, chunk_data, current_bytes)
                    with self.lock:
                        self.completed_parts.add(part_number)
                        self.active_uploads -= 1
                    self.result_queue.put(('success', part_number, result))
                except Exception as e:
                    with self.lock:
                        self.upload_error = e
                        self.active_uploads -= 1
                    self.result_queue.put(('error', part_number, str(e)))
                finally:
                    self.chunk_queue.task_done()

            except Exception as e:
                with self.lock:
                    self.upload_error = e
                break

    def add_chunk(self, part_number: int, chunk_data: bytes, current_bytes: int) -> bool:
        """
        Add a chunk to be uploaded. Returns False if upload has been stopped due to error.
        """
        if self.upload_error:
            return False

        with self.lock:
            self.active_uploads += 1
            self.pending_parts.append(part_number)

        try:
            self.chunk_queue.put((part_number, chunk_data, current_bytes))
            return True
        except Exception as e:
            with self.lock:
                self.upload_error = e
                self.active_uploads -= 1
            return False

    def wait_for_completion(self) -> Tuple[bool, Exception]:
        """
        Wait for all uploads to complete.
        Returns (success, error) tuple.
        """
        try:
            # Signal workers to stop
            for _ in self.workers:
                self.chunk_queue.put(None)

            # Wait for all workers to finish
            for worker in self.workers:
                worker.join()

            # Check for any remaining results
            while not self.result_queue.empty():
                status, part_number, result = self.result_queue.get()
                if status != 'success':
                    return False, Exception(f"Failed to upload part {part_number}: {result}")

            return not bool(self.upload_error), self.upload_error

        except Exception as e:
            return False, e
