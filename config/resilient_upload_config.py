"""
Resilient Upload Configuration
Centralized configuration for all resilience features
Optimized for PaaS environment with 512MB RAM and 0.2 vCPU
"""

import os
from typing import Dict, Any


class ResilientUploadConfig:
    """Configuration class for resilient upload features"""
    
    def __init__(self):
        # Environment Detection
        self.environment = os.getenv('ENVIRONMENT', 'development')
        self.is_production = self.environment == 'production'
        
        # PaaS Resource Constraints
        self.total_memory_mb = int(os.getenv('TOTAL_MEMORY_MB', '512'))
        self.total_cpu_cores = float(os.getenv('TOTAL_CPU_CORES', '0.2'))
        
        # Simple Worker Configuration (Fixed limits)
        self.worker_config = {
            'max_workers': int(os.getenv('MAX_WORKERS', '4')),
            'default_workers': int(os.getenv('DEFAULT_WORKERS', '4'))
        }
        
        # Timeout Configuration (Enhanced for reliability)
        self.timeout_config = {
            'connection_timeout': int(os.getenv('CONNECTION_TIMEOUT', '30')),      # seconds
            'read_timeout': int(os.getenv('READ_TIMEOUT', '300')),                 # 5 minutes
            'total_timeout': int(os.getenv('TOTAL_TIMEOUT', '450')),               # 7.5 minutes
            'chunk_upload_timeout': int(os.getenv('CHUNK_UPLOAD_TIMEOUT', '120'))  # 2 minutes per chunk
        }
        
        # Retry Strategy (Exponential backoff)
        self.retry_config = {
            'max_retries': int(os.getenv('MAX_RETRIES', '5')),
            'initial_delay': float(os.getenv('INITIAL_RETRY_DELAY', '1.0')),
            'max_delay': float(os.getenv('MAX_RETRY_DELAY', '120.0')),
            'exponential_base': float(os.getenv('RETRY_EXPONENTIAL_BASE', '2.0')),
            'jitter_percent': int(os.getenv('RETRY_JITTER_PERCENT', '25')),
            'retryable_status_codes': [502, 503, 504],
            'retryable_exceptions': [
                'requests.exceptions.Timeout',
                'requests.exceptions.ConnectionError',
                'requests.exceptions.ChunkedEncodingError'
            ]
        }
        
        # Circuit Breaker Configuration
        self.circuit_breaker_config = {
            'upload_operations': {
                'failure_threshold': int(os.getenv('CB_UPLOAD_FAILURE_THRESHOLD', '5')),
                'timeout_duration': int(os.getenv('CB_UPLOAD_TIMEOUT_DURATION', '60')),
                'half_open_max_calls': int(os.getenv('CB_UPLOAD_HALF_OPEN_CALLS', '3')),
                'success_threshold': int(os.getenv('CB_UPLOAD_SUCCESS_THRESHOLD', '2'))
            },
            'notion_api': {
                'failure_threshold': int(os.getenv('CB_NOTION_FAILURE_THRESHOLD', '3')),
                'timeout_duration': int(os.getenv('CB_NOTION_TIMEOUT_DURATION', '30')),
                'half_open_max_calls': int(os.getenv('CB_NOTION_HALF_OPEN_CALLS', '2')),
                'success_threshold': int(os.getenv('CB_NOTION_SUCCESS_THRESHOLD', '1'))
            }
        }
        
        # Connection Pool Configuration (Optimized for resource constraints)
        self.connection_config = {
            'pool_connections': int(os.getenv('POOL_CONNECTIONS', '3')),
            'pool_maxsize': int(os.getenv('POOL_MAXSIZE', '5')),
            'keep_alive_timeout': int(os.getenv('KEEP_ALIVE_TIMEOUT', '300')),
            'max_retries_per_connection': int(os.getenv('MAX_RETRIES_PER_CONNECTION', '0')),  # Handle in app layer
            'pool_block': os.getenv('POOL_BLOCK', 'false').lower() == 'true'
        }

        # Download URL cache configuration
        # Allows adjusting how long signed download URLs are cached locally
        # before requesting a fresh one from Notion. Defaults to 5 minutes.
        self.download_url_cache_ttl = int(os.getenv('DOWNLOAD_URL_CACHE_TTL', '300'))
        
        # Checkpoint Configuration
        self.checkpoint_config = {
            'enabled': os.getenv('CHECKPOINT_ENABLED', 'true').lower() == 'true',
            'interval_parts': int(os.getenv('CHECKPOINT_INTERVAL_PARTS', '50')),  # Every 250MB
            'storage_backend': os.getenv('CHECKPOINT_STORAGE_BACKEND', 'memory'),
            'expiration_hours': int(os.getenv('CHECKPOINT_EXPIRATION_HOURS', '24')),
            'cleanup_interval': int(os.getenv('CHECKPOINT_CLEANUP_INTERVAL', '3600'))  # 1 hour
        }
        
        # Simple Upload Limits (keeping basic queue controls)
        self.upload_limits = {
            'max_concurrent_uploads': int(os.getenv('MAX_CONCURRENT_UPLOADS', '10')),
            'queue_size_limit': int(os.getenv('QUEUE_SIZE_LIMIT', '10')),
            'max_workers': int(os.getenv('MAX_WORKERS', '4'))
        }
        
        # Monitoring Configuration
        self.monitoring_config = {
            'metrics_interval': int(os.getenv('METRICS_INTERVAL', '30')),           # seconds
            'log_level': os.getenv('LOG_LEVEL', 'INFO'),
            'enable_detailed_logging': os.getenv('ENABLE_DETAILED_LOGGING', 'true').lower() == 'true',
            'alert_thresholds': {
                'success_rate_percent': float(os.getenv('ALERT_SUCCESS_RATE_THRESHOLD', '90')),
                'circuit_breaker_failures': int(os.getenv('ALERT_CB_FAILURES', '3'))
            }
        }
        
        # File Upload Limits
        self.file_limits = {
            'max_file_size_gb': float(os.getenv('MAX_FILE_SIZE_GB', '5.0')),
            'single_part_threshold_mb': int(os.getenv('SINGLE_PART_THRESHOLD_MB', '20')),
            'chunk_size_mb': int(os.getenv('CHUNK_SIZE_MB', '5')),
            'read_buffer_size_kb': int(os.getenv('READ_BUFFER_SIZE_KB', '64'))
        }
        
        # Security Configuration
        self.security_config = {
            'rate_limit_per_user': int(os.getenv('RATE_LIMIT_PER_USER', '5')),     # uploads per minute
            'max_upload_sessions_per_user': int(os.getenv('MAX_SESSIONS_PER_USER', '3')),
            'session_timeout_minutes': int(os.getenv('SESSION_TIMEOUT_MINUTES', '30')),
            'enable_upload_validation': os.getenv('ENABLE_UPLOAD_VALIDATION', 'true').lower() == 'true'
        }
    
    def get_environment_specific_config(self) -> Dict[str, Any]:
        """Get configuration adjusted for current environment"""
        if self.is_production:
            # Production optimizations
            config = self.to_dict()
            config['monitoring_config']['enable_detailed_logging'] = False
            config['monitoring_config']['metrics_interval'] = 60  # Less frequent in prod
            config['checkpoint_config']['cleanup_interval'] = 7200  # 2 hours
            return config
        else:
            # Development settings
            config = self.to_dict()
            config['worker_config']['max_workers'] = 6  # Allow more workers in dev
            config['monitoring_config']['enable_detailed_logging'] = True
            return config
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary"""
        return {
            'environment': self.environment,
            'worker_config': self.worker_config,
            'timeout_config': self.timeout_config,
            'retry_config': self.retry_config,
            'circuit_breaker_config': self.circuit_breaker_config,
            'connection_config': self.connection_config,
            'download_url_cache_ttl': self.download_url_cache_ttl,
            'checkpoint_config': self.checkpoint_config,
            'upload_limits': self.upload_limits,
            'monitoring_config': self.monitoring_config,
            'file_limits': self.file_limits,
            'security_config': self.security_config
        }
    
    def validate_config(self) -> bool:
        """Validate configuration values"""
        try:
            # Worker validation
            if self.worker_config['max_workers'] > 10:
                print("⚠️  Warning: High worker count may impact performance")
            
            # Timeout validation
            if self.timeout_config['total_timeout'] < self.timeout_config['read_timeout']:
                print("⚠️  Warning: Total timeout should be >= read timeout")
                return False
            
            # Circuit breaker validation
            for name, config in self.circuit_breaker_config.items():
                if config['failure_threshold'] < 1:
                    print(f"⚠️  Warning: {name} circuit breaker failure threshold too low")
                    return False
            
            print("✅ Configuration validation passed")
            return True
            
        except Exception as e:
            print(f"❌ Configuration validation failed: {e}")
            return False
    
    def print_summary(self):
        """Print configuration summary"""
        print("🔧 SIMPLIFIED_UPLOAD_CONFIGURATION:")
        print(f"   Environment: {self.environment}")
        print(f"   Workers: {self.worker_config['max_workers']}")
        print(f"   Timeouts: {self.timeout_config['connection_timeout']}s connect, {self.timeout_config['read_timeout']}s read")
        print(f"   Retries: {self.retry_config['max_retries']} attempts")
        print(f"   Checkpoints: {'Enabled' if self.checkpoint_config['enabled'] else 'Disabled'} every {self.checkpoint_config['interval_parts']} parts")
        print(f"   Max File Size: {self.file_limits['max_file_size_gb']}GB")
        print(f"   Max Concurrent: {self.upload_limits['max_concurrent_uploads']} uploads")


# Global configuration instance
resilient_config = ResilientUploadConfig()

# Validate configuration on import
if not resilient_config.validate_config():
    print("❌ Configuration validation failed - check settings")

# Export commonly used values
MAX_WORKERS = resilient_config.worker_config['max_workers']
MAX_CONCURRENT_UPLOADS = resilient_config.upload_limits['max_concurrent_uploads']
TIMEOUT_CONFIG = resilient_config.timeout_config
RETRY_CONFIG = resilient_config.retry_config
CHECKPOINT_ENABLED = resilient_config.checkpoint_config['enabled']
DOWNLOAD_URL_CACHE_TTL = resilient_config.download_url_cache_ttl
