"""
Circuit Breaker Pattern Implementation for Upload Resilience
Provides automatic failure detection and recovery for upload operations
"""

import time
import threading
from enum import Enum
from typing import Callable, Any, Dict, Optional
from dataclasses import dataclass


class CircuitBreakerState(Enum):
    """Circuit breaker states"""
    CLOSED = "CLOSED"      # Normal operation
    OPEN = "OPEN"          # Failing fast
    HALF_OPEN = "HALF_OPEN"  # Testing recovery


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker behavior"""
    failure_threshold: int = 5          # Failures before opening
    timeout_duration: int = 60          # Seconds to wait before trying again
    half_open_max_calls: int = 3        # Max calls to test in half-open state
    success_threshold: int = 2          # Successes needed to close from half-open
    
    # Failure detection
    timeout_as_failure: bool = True
    connection_error_as_failure: bool = True
    server_error_codes: list = None
    
    def __post_init__(self):
        if self.server_error_codes is None:
            self.server_error_codes = [502, 503, 504]


class CircuitBreakerOpenError(Exception):
    """Exception raised when circuit breaker is open"""
    pass


class UploadCircuitBreaker:
    """
    Circuit breaker implementation for upload operations
    Provides automatic failure detection and recovery
    """
    
    def __init__(self, name: str = "upload", config: Optional[CircuitBreakerConfig] = None):
        self.name = name
        self.config = config or CircuitBreakerConfig()
        
        # State management
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = None
        self.half_open_calls = 0
        
        # Thread safety
        self.lock = threading.Lock()
        
        # Statistics
        self.total_calls = 0
        self.total_failures = 0
        self.total_successes = 0
        self.state_changes = []
        
        print(f"🔒 Circuit breaker '{name}' initialized:")
        print(f"   Failure threshold: {self.config.failure_threshold}")
        print(f"   Timeout duration: {self.config.timeout_duration}s")
        print(f"   Half-open max calls: {self.config.half_open_max_calls}")
    
    def call(self, func: Callable, *args, **kwargs) -> Any:
        """
        Execute function with circuit breaker protection
        
        Args:
            func: Function to execute
            *args, **kwargs: Arguments to pass to function
            
        Returns:
            Function result
            
        Raises:
            CircuitBreakerOpenError: When circuit is open
            Original exception: When function fails and circuit allows it
        """
        with self.lock:
            self.total_calls += 1
            
            # Check if we should reject the call
            if self.state == CircuitBreakerState.OPEN:
                if self._should_attempt_reset():
                    self._transition_to_half_open()
                else:
                    raise CircuitBreakerOpenError(
                        f"Circuit breaker '{self.name}' is OPEN. "
                        f"Will retry after {self._time_until_retry():.1f} seconds."
                    )
            
            elif self.state == CircuitBreakerState.HALF_OPEN:
                if self.half_open_calls >= self.config.half_open_max_calls:
                    raise CircuitBreakerOpenError(
                        f"Circuit breaker '{self.name}' is HALF_OPEN and at call limit."
                    )
                self.half_open_calls += 1
        
        # Execute the function
        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
            
        except Exception as e:
            self._on_failure(e)
            raise
    
    def _on_success(self):
        """Handle successful function execution"""
        with self.lock:
            self.total_successes += 1
            
            if self.state == CircuitBreakerState.HALF_OPEN:
                self.success_count += 1
                print(f"✅ Circuit breaker '{self.name}' success {self.success_count}/{self.config.success_threshold} in HALF_OPEN")
                
                if self.success_count >= self.config.success_threshold:
                    self._transition_to_closed()
                    
            elif self.state == CircuitBreakerState.CLOSED:
                # Reset failure count on success
                if self.failure_count > 0:
                    print(f"🔄 Circuit breaker '{self.name}' failure count reset after success")
                    self.failure_count = 0
    
    def _on_failure(self, exception: Exception):
        """Handle failed function execution"""
        with self.lock:
            self.total_failures += 1
            
            # Determine if this failure should count towards circuit breaking
            if self._is_circuit_breaking_failure(exception):
                self.failure_count += 1
                self.last_failure_time = time.time()
                
                print(f"❌ Circuit breaker '{self.name}' failure {self.failure_count}/{self.config.failure_threshold}")
                
                if self.state == CircuitBreakerState.CLOSED:
                    if self.failure_count >= self.config.failure_threshold:
                        self._transition_to_open()
                        
                elif self.state == CircuitBreakerState.HALF_OPEN:
                    # Any failure in half-open state transitions back to open
                    self._transition_to_open()
    
    def _is_circuit_breaking_failure(self, exception: Exception) -> bool:
        """Determine if an exception should count towards circuit breaking"""
        exception_str = str(exception).lower()
        
        # Check for timeout errors
        if self.config.timeout_as_failure:
            if any(keyword in exception_str for keyword in ['timeout', 'timed out']):
                return True
        
        # Check for connection errors
        if self.config.connection_error_as_failure:
            if any(keyword in exception_str for keyword in ['connection', 'network', 'unreachable']):
                return True
        
        # Check for server error codes
        for code in self.config.server_error_codes:
            if str(code) in exception_str:
                return True
        
        # Check exception types
        import requests
        if isinstance(exception, (requests.exceptions.Timeout, requests.exceptions.ConnectionError)):
            return True
        
        return False
    
    def _should_attempt_reset(self) -> bool:
        """Check if enough time has passed to attempt reset"""
        if self.last_failure_time is None:
            return True
        
        time_since_failure = time.time() - self.last_failure_time
        return time_since_failure >= self.config.timeout_duration
    
    def _time_until_retry(self) -> float:
        """Calculate time until next retry attempt"""
        if self.last_failure_time is None:
            return 0
        
        time_since_failure = time.time() - self.last_failure_time
        return max(0, self.config.timeout_duration - time_since_failure)
    
    def _transition_to_open(self):
        """Transition circuit breaker to OPEN state"""
        old_state = self.state
        self.state = CircuitBreakerState.OPEN
        self.half_open_calls = 0
        self.success_count = 0
        self._record_state_change(old_state, self.state)
        
        print(f"🚨 Circuit breaker '{self.name}' OPENED after {self.failure_count} failures")
        print(f"   Will attempt retry in {self.config.timeout_duration} seconds")
    
    def _transition_to_half_open(self):
        """Transition circuit breaker to HALF_OPEN state"""
        old_state = self.state
        self.state = CircuitBreakerState.HALF_OPEN
        self.half_open_calls = 0
        self.success_count = 0
        self._record_state_change(old_state, self.state)
        
        print(f"🔓 Circuit breaker '{self.name}' HALF_OPEN - testing recovery")
    
    def _transition_to_closed(self):
        """Transition circuit breaker to CLOSED state"""
        old_state = self.state
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.half_open_calls = 0
        self._record_state_change(old_state, self.state)
        
        print(f"✅ Circuit breaker '{self.name}' CLOSED - normal operation restored")
    
    def _record_state_change(self, old_state: CircuitBreakerState, new_state: CircuitBreakerState):
        """Record state change for monitoring"""
        self.state_changes.append({
            'timestamp': time.time(),
            'from_state': old_state.value,
            'to_state': new_state.value,
            'failure_count': self.failure_count
        })
        
        # Keep only last 10 state changes
        if len(self.state_changes) > 10:
            self.state_changes = self.state_changes[-10:]
    
    def get_stats(self) -> Dict:
        """Get circuit breaker statistics"""
        with self.lock:
            success_rate = 0
            if self.total_calls > 0:
                success_rate = (self.total_successes / self.total_calls) * 100
            
            return {
                'name': self.name,
                'state': self.state.value,
                'total_calls': self.total_calls,
                'total_successes': self.total_successes,
                'total_failures': self.total_failures,
                'success_rate_percent': success_rate,
                'failure_count': self.failure_count,
                'time_until_retry': self._time_until_retry() if self.state == CircuitBreakerState.OPEN else 0,
                'state_changes': self.state_changes[-5:],  # Last 5 changes
                'config': {
                    'failure_threshold': self.config.failure_threshold,
                    'timeout_duration': self.config.timeout_duration,
                    'half_open_max_calls': self.config.half_open_max_calls
                }
            }
    
    def reset(self):
        """Manually reset circuit breaker to CLOSED state"""
        with self.lock:
            old_state = self.state
            self.state = CircuitBreakerState.CLOSED
            self.failure_count = 0
            self.success_count = 0
            self.half_open_calls = 0
            self._record_state_change(old_state, self.state)
            
            print(f"🔄 Circuit breaker '{self.name}' manually reset to CLOSED")
    
    def force_open(self):
        """Manually force circuit breaker to OPEN state"""
        with self.lock:
            old_state = self.state
            self.state = CircuitBreakerState.OPEN
            self.last_failure_time = time.time()
            self._record_state_change(old_state, self.state)
            
            print(f"🚨 Circuit breaker '{self.name}' manually forced to OPEN")


# Global circuit breaker instances for different operations
upload_circuit_breaker = UploadCircuitBreaker(
    name="upload_operations",
    config=CircuitBreakerConfig(
        failure_threshold=5,
        timeout_duration=60,
        half_open_max_calls=3,
        success_threshold=2
    )
)

notion_api_circuit_breaker = UploadCircuitBreaker(
    name="notion_api",
    config=CircuitBreakerConfig(
        failure_threshold=3,
        timeout_duration=30,
        half_open_max_calls=2,
        success_threshold=1
    )
)


def with_circuit_breaker(circuit_breaker: UploadCircuitBreaker):
    """Decorator to apply circuit breaker protection to functions"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            return circuit_breaker.call(func, *args, **kwargs)
        return wrapper
    return decorator


def log_all_circuit_breaker_stats():
    """Log statistics for all circuit breakers"""
    breakers = [upload_circuit_breaker, notion_api_circuit_breaker]
    
    print(f"🔒 CIRCUIT_BREAKER_STATS:")
    for breaker in breakers:
        stats = breaker.get_stats()
        print(f"   {stats['name']}: {stats['state']} | "
              f"Success: {stats['success_rate_percent']:.1f}% | "
              f"Calls: {stats['total_calls']} | "
              f"Failures: {stats['failure_count']}")