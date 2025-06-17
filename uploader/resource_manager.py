"""
Resource-Aware Manager for Constrained PaaS Environment
Handles adaptive concurrency and memory management for 512MB RAM, 0.2 vCPU
"""

import threading
import time
import psutil
import os
from typing import Dict, Tuple, Optional
from dataclasses import dataclass


@dataclass
class ResourceThresholds:
    """Resource usage thresholds for scaling decisions"""
    memory_low_mb: int = 200      # Allow 3 workers
    memory_medium_mb: int = 350   # Allow 2 workers  
    memory_high_mb: int = 450     # Allow 1 worker
    memory_critical_mb: int = 500 # Reject uploads
    
    cpu_medium: float = 0.12      # 60% of 0.2 vCPU
    cpu_high: float = 0.16        # 80% of 0.2 vCPU
    cpu_critical: float = 0.18    # 90% of 0.2 vCPU


class ResourceAwareManager:
    """
    Manages system resources and adaptive concurrency for upload operations
    Designed specifically for resource-constrained PaaS environments
    """
    
    def __init__(self, max_memory_mb: int = 100, max_cpu_percent: float = 0.16):
        self.max_memory_mb = max_memory_mb  # 20% of 512MB
        self.max_cpu_percent = max_cpu_percent  # 80% of 0.2 vCPU
        self.thresholds = ResourceThresholds()
        
        # Tracking
        self.current_uploads = 0
        self.lock = threading.Lock()
        self.last_resource_check = 0
        self.check_interval = 5  # Check every 5 seconds
        
        # Worker management
        self.max_workers = 3
        self.min_workers = 1
        self.current_optimal_workers = 2
        
        # Memory tracking
        self.process = psutil.Process(os.getpid())
        self.baseline_memory_mb = self._get_memory_usage_mb()
        
        print(f"🎯 ResourceAwareManager initialized:")
        print(f"   Memory limit: {max_memory_mb}MB (20% of 512MB)")
        print(f"   CPU limit: {max_cpu_percent*100:.1f}% (80% of 0.2 vCPU)")
        print(f"   Baseline memory: {self.baseline_memory_mb:.1f}MB")
    
    def should_accept_upload(self, estimated_file_size: int) -> Tuple[bool, str]:
        """
        Determine if a new upload should be accepted based on current resources
        
        Args:
            estimated_file_size: Estimated file size in bytes
            
        Returns:
            Tuple of (should_accept, reason)
        """
        with self.lock:
            # Get current resource usage
            memory_mb = self._get_memory_usage_mb()
            cpu_percent = self._get_cpu_usage_percent()
            
            # Estimate additional memory needed (conservative estimate)
            estimated_memory_mb = min(20, estimated_file_size / (1024 * 1024 * 50))  # ~20MB max per upload
            projected_memory = memory_mb + estimated_memory_mb
            
            # Check memory limits
            if projected_memory > self.thresholds.memory_critical_mb:
                return False, f"Memory would exceed critical limit ({projected_memory:.1f}MB > {self.thresholds.memory_critical_mb}MB)"
            
            # Check current memory usage
            if memory_mb > self.thresholds.memory_high_mb:
                return False, f"Current memory usage too high ({memory_mb:.1f}MB > {self.thresholds.memory_high_mb}MB)"
            
            # Check CPU usage
            if cpu_percent > self.thresholds.cpu_critical:
                return False, f"CPU usage too high ({cpu_percent*100:.1f}% > {self.thresholds.cpu_critical*100:.1f}%)"
            
            # Check concurrent upload limit
            max_concurrent = self._get_max_concurrent_uploads()
            if self.current_uploads >= max_concurrent:
                return False, f"Too many concurrent uploads ({self.current_uploads} >= {max_concurrent})"
            
            return True, "Resources available"
    
    def register_upload_start(self, upload_id: str) -> None:
        """Register the start of a new upload"""
        with self.lock:
            self.current_uploads += 1
            print(f"📈 Upload started: {upload_id} | Active uploads: {self.current_uploads}")
    
    def register_upload_end(self, upload_id: str) -> None:
        """Register the completion of an upload"""
        with self.lock:
            self.current_uploads = max(0, self.current_uploads - 1)
            print(f"📉 Upload ended: {upload_id} | Active uploads: {self.current_uploads}")
    
    def get_optimal_worker_count(self) -> int:
        """
        Calculate optimal worker count based on current resource usage
        
        Returns:
            Optimal number of workers (1-3)
        """
        # Only check resources periodically to reduce overhead
        current_time = time.time()
        if current_time - self.last_resource_check < self.check_interval:
            return self.current_optimal_workers
        
        self.last_resource_check = current_time
        
        memory_mb = self._get_memory_usage_mb()
        cpu_percent = self._get_cpu_usage_percent()
        
        # Determine worker count based on resource usage
        if memory_mb > self.thresholds.memory_high_mb or cpu_percent > self.thresholds.cpu_high:
            optimal_workers = 1  # Conservative single worker
        elif memory_mb > self.thresholds.memory_medium_mb or cpu_percent > self.thresholds.cpu_medium:
            optimal_workers = 2  # Moderate concurrency
        else:
            optimal_workers = 3  # Maximum safe concurrency
        
        # Log changes in worker count
        if optimal_workers != self.current_optimal_workers:
            print(f"🔄 Worker count adjusted: {self.current_optimal_workers} → {optimal_workers}")
            print(f"   Memory: {memory_mb:.1f}MB | CPU: {cpu_percent*100:.1f}%")
        
        self.current_optimal_workers = optimal_workers
        return optimal_workers
    
    def apply_backpressure_delay(self) -> float:
        """
        Calculate backpressure delay based on current system load
        
        Returns:
            Delay in seconds (0 = no delay)
        """
        memory_mb = self._get_memory_usage_mb()
        cpu_percent = self._get_cpu_usage_percent()
        
        # Calculate load factors
        memory_load = memory_mb / self.thresholds.memory_critical_mb
        cpu_load = cpu_percent / self.thresholds.cpu_critical
        max_load = max(memory_load, cpu_load)
        
        if max_load > 0.9:
            delay = 2.0  # Heavy backpressure
        elif max_load > 0.7:
            delay = 1.0  # Medium backpressure
        elif max_load > 0.5:
            delay = 0.5  # Light backpressure
        else:
            delay = 0.0  # No backpressure needed
        
        if delay > 0:
            print(f"⏳ Applying backpressure: {delay}s delay (memory: {memory_load*100:.1f}%, cpu: {cpu_load*100:.1f}%)")
        
        return delay
    
    def get_resource_stats(self) -> Dict:
        """Get comprehensive resource usage statistics"""
        memory_mb = self._get_memory_usage_mb()
        cpu_percent = self._get_cpu_usage_percent()
        
        return {
            'memory_usage_mb': memory_mb,
            'memory_usage_percent': (memory_mb / 512) * 100,  # Percent of total 512MB
            'cpu_usage_percent': cpu_percent * 100,
            'active_uploads': self.current_uploads,
            'optimal_workers': self.current_optimal_workers,
            'max_concurrent_uploads': self._get_max_concurrent_uploads(),
            'backpressure_delay': self.apply_backpressure_delay(),
            'resource_state': self._get_resource_state()
        }
    
    def _get_memory_usage_mb(self) -> float:
        """Get current memory usage in MB"""
        try:
            memory_info = self.process.memory_info()
            return memory_info.rss / (1024 * 1024)
        except Exception as e:
            print(f"Warning: Could not get memory usage: {e}")
            return 0.0
    
    def _get_cpu_usage_percent(self) -> float:
        """Get current CPU usage as a fraction (0.0 - 1.0)"""
        try:
            # Use interval to get accurate CPU usage
            cpu_percent = self.process.cpu_percent(interval=None)
            # Convert to fraction and normalize to available CPU (0.2 vCPU = 20%)
            return min(1.0, cpu_percent / 100.0)
        except Exception as e:
            print(f"Warning: Could not get CPU usage: {e}")
            return 0.0
    
    def _get_max_concurrent_uploads(self) -> int:
        """Calculate maximum concurrent uploads based on current resources"""
        memory_mb = self._get_memory_usage_mb()
        
        # Conservative approach: each upload can use up to 20MB
        available_memory = max(0, self.thresholds.memory_high_mb - memory_mb)
        memory_based_limit = max(1, int(available_memory / 20))
        
        # CPU-based limit (very conservative for 0.2 vCPU)
        cpu_based_limit = 5  # Maximum 5 concurrent uploads
        
        return min(memory_based_limit, cpu_based_limit, 10)  # Absolute max of 10
    
    def _get_resource_state(self) -> str:
        """Get human-readable resource state"""
        memory_mb = self._get_memory_usage_mb()
        cpu_percent = self._get_cpu_usage_percent()
        
        if memory_mb > self.thresholds.memory_high_mb or cpu_percent > self.thresholds.cpu_high:
            return "HIGH_LOAD"
        elif memory_mb > self.thresholds.memory_medium_mb or cpu_percent > self.thresholds.cpu_medium:
            return "MEDIUM_LOAD"
        else:
            return "LOW_LOAD"


class BackpressureManager:
    """
    Advanced backpressure management for upload operations
    Implements progressive throttling based on system resources
    """
    
    def __init__(self, resource_manager: ResourceAwareManager):
        self.resource_manager = resource_manager
        self.upload_queue_size = 0
        self.queue_limit = 10
        self.lock = threading.Lock()
    
    def should_throttle_request(self) -> Tuple[bool, float]:
        """
        Determine if request should be throttled and by how much
        
        Returns:
            Tuple of (should_throttle, delay_seconds)
        """
        with self.lock:
            # Check queue size
            if self.upload_queue_size >= self.queue_limit:
                return True, 5.0  # Queue full, significant delay
            
            # Get resource-based delay
            delay = self.resource_manager.apply_backpressure_delay()
            return delay > 0, delay
    
    def enter_queue(self) -> None:
        """Register entry into upload queue"""
        with self.lock:
            self.upload_queue_size += 1
    
    def exit_queue(self) -> None:
        """Register exit from upload queue"""
        with self.lock:
            self.upload_queue_size = max(0, self.upload_queue_size - 1)
    
    def get_queue_stats(self) -> Dict:
        """Get queue statistics"""
        with self.lock:
            return {
                'queue_size': self.upload_queue_size,
                'queue_limit': self.queue_limit,
                'queue_utilization': (self.upload_queue_size / self.queue_limit) * 100
            }


# Global instance for application use
resource_manager = ResourceAwareManager()
backpressure_manager = BackpressureManager(resource_manager)


def log_resource_usage():
    """Utility function to log current resource usage"""
    stats = resource_manager.get_resource_stats()
    queue_stats = backpressure_manager.get_queue_stats()
    
    print(f"📊 RESOURCE_STATS:")
    print(f"   Memory: {stats['memory_usage_mb']:.1f}MB ({stats['memory_usage_percent']:.1f}% of 512MB)")
    print(f"   CPU: {stats['cpu_usage_percent']:.1f}%")
    print(f"   Active uploads: {stats['active_uploads']}")
    print(f"   Optimal workers: {stats['optimal_workers']}")
    print(f"   Queue: {queue_stats['queue_size']}/{queue_stats['queue_limit']}")
    print(f"   State: {stats['resource_state']}")