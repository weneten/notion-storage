"""
Diagnostic logging module for 504 Gateway Timeout debugging
"""
import time
import threading
import psutil
import requests
from typing import Dict, Any
import json

class TimeoutDiagnostics:
    def __init__(self):
        self.request_times = {}
        self.connection_pool_stats = {}
        self.memory_snapshots = []
        self.lock = threading.Lock()
    
    def log_request_start(self, request_id: str, url: str, part_number: int = None):
        """Log the start of a request with timing"""
        with self.lock:
            self.request_times[request_id] = {
                'start_time': time.time(),
                'url': url,
                'part_number': part_number,
                'thread_id': threading.current_thread().ident
            }
            print(f"🕐 REQUEST_START: {request_id} | Part {part_number} | Thread {threading.current_thread().ident} | URL: {url[:50]}...")
    
    def log_request_end(self, request_id: str, status_code: int = None, error: str = None):
        """Log the end of a request with duration"""
        with self.lock:
            if request_id in self.request_times:
                start_info = self.request_times[request_id]
                duration = time.time() - start_info['start_time']
                
                if error:
                    print(f"❌ REQUEST_ERROR: {request_id} | Part {start_info.get('part_number')} | Duration: {duration:.2f}s | Error: {error}")
                else:
                    print(f"✅ REQUEST_SUCCESS: {request_id} | Part {start_info.get('part_number')} | Duration: {duration:.2f}s | Status: {status_code}")
                
                # Flag slow requests
                if duration > 30:
                    print(f"🐌 SLOW_REQUEST: {request_id} took {duration:.2f}s - potential timeout risk")
                
                del self.request_times[request_id]
    
    def log_memory_snapshot(self, context: str, part_number: int = None):
        """Take a memory snapshot for analysis"""
        try:
            process = psutil.Process()
            memory_info = process.memory_info()
            memory_mb = memory_info.rss / (1024 * 1024)
            
            snapshot = {
                'context': context,
                'part_number': part_number,
                'memory_mb': memory_mb,
                'timestamp': time.time(),
                'thread_count': threading.active_count()
            }
            
            self.memory_snapshots.append(snapshot)
            print(f"📊 MEMORY_SNAPSHOT: {context} | Part {part_number} | Memory: {memory_mb:.1f}MB | Threads: {threading.active_count()}")
            
            # Alert on high memory usage
            if memory_mb > 500:
                print(f"⚠️  HIGH_MEMORY_WARNING: {memory_mb:.1f}MB at part {part_number}")
                
        except Exception as e:
            print(f"Error taking memory snapshot: {e}")
    
    def log_connection_pool_stats(self):
        """Log connection pool statistics"""
        try:
            # Get active connections from requests adapter
            adapter = requests.adapters.HTTPAdapter()
            if hasattr(adapter, 'poolmanager') and adapter.poolmanager:
                pools = adapter.poolmanager.pools
                print(f"🔗 CONNECTION_POOLS: Active pools: {len(pools)}")
                for pool_key, pool in pools.items():
                    print(f"   Pool {pool_key}: {pool.num_connections} connections")
        except Exception as e:
            print(f"Error getting connection pool stats: {e}")
    
    def log_parallel_processor_state(self, processor):
        """Log the state of parallel processor"""
        try:
            with processor.lock:
                print(f"🔄 PARALLEL_STATE: Active futures: {len(processor.part_futures)} | Completed parts: {len(processor.completed_parts)} | Has error: {processor.upload_error is not None}")
                
                # Log pending futures
                pending_parts = []
                for part_num, future in processor.part_futures.items():
                    if not future.done():
                        pending_parts.append(part_num)
                
                if pending_parts:
                    print(f"⏳ PENDING_PARTS: {sorted(pending_parts)}")
                    
        except Exception as e:
            print(f"Error logging parallel processor state: {e}")

# Global diagnostic instance
timeout_diagnostics = TimeoutDiagnostics()