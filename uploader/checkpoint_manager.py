"""
Checkpoint Manager for Upload Resume Functionality
Enables recovery from partial uploads by saving progress at regular intervals
"""

import json
import time
import threading
import hashlib
from typing import Dict, Set, Optional, Any, List
from dataclasses import dataclass, asdict
from abc import ABC, abstractmethod


@dataclass
class UploadCheckpoint:
    """Data structure for upload checkpoint"""
    upload_id: str
    file_hash: str
    total_parts: int
    completed_parts: List[int]
    multipart_upload_id: str
    file_size: int
    filename: str
    user_database_id: str
    created_at: float
    last_updated: float
    expires_at: float
    
    def to_dict(self) -> Dict:
        """Convert checkpoint to dictionary"""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'UploadCheckpoint':
        """Create checkpoint from dictionary"""
        return cls(**data)
    
    def is_expired(self) -> bool:
        """Check if checkpoint has expired"""
        return time.time() > self.expires_at
    
    def get_progress_percent(self) -> float:
        """Calculate upload progress percentage"""
        if self.total_parts == 0:
            return 0.0
        return (len(self.completed_parts) / self.total_parts) * 100
    
    def get_remaining_parts(self) -> Set[int]:
        """Get set of parts that still need to be uploaded"""
        all_parts = set(range(1, self.total_parts + 1))
        completed_set = set(self.completed_parts)
        return all_parts - completed_set


class CheckpointStorage(ABC):
    """Abstract base class for checkpoint storage backends"""
    
    @abstractmethod
    def save(self, key: str, checkpoint: UploadCheckpoint) -> None:
        """Save checkpoint to storage"""
        pass
    
    @abstractmethod
    def load(self, key: str) -> Optional[UploadCheckpoint]:
        """Load checkpoint from storage"""
        pass
    
    @abstractmethod
    def delete(self, key: str) -> None:
        """Delete checkpoint from storage"""
        pass
    
    @abstractmethod
    def list_expired(self) -> List[str]:
        """List expired checkpoint keys"""
        pass


class MemoryCheckpointStorage(CheckpointStorage):
    """In-memory checkpoint storage (for development/fallback)"""
    
    def __init__(self):
        self.storage = {}
        self.lock = threading.Lock()
    
    def save(self, key: str, checkpoint: UploadCheckpoint) -> None:
        with self.lock:
            self.storage[key] = checkpoint.to_dict()
    
    def load(self, key: str) -> Optional[UploadCheckpoint]:
        with self.lock:
            data = self.storage.get(key)
            if data:
                checkpoint = UploadCheckpoint.from_dict(data)
                if checkpoint.is_expired():
                    del self.storage[key]
                    return None
                return checkpoint
            return None
    
    def delete(self, key: str) -> None:
        with self.lock:
            self.storage.pop(key, None)
    
    def list_expired(self) -> List[str]:
        with self.lock:
            expired_keys = []
            current_time = time.time()
            for key, data in list(self.storage.items()):
                if data.get('expires_at', 0) < current_time:
                    expired_keys.append(key)
            return expired_keys


class RedisCheckpointStorage(CheckpointStorage):
    """Redis-based checkpoint storage (recommended for production)"""
    
    def __init__(self, redis_url: Optional[str] = None):
        try:
            import redis
            if redis_url:
                self.redis = redis.from_url(redis_url)
            else:
                # Try to connect to local Redis
                self.redis = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
            
            # Test connection
            self.redis.ping()
            self.available = True
            print("✅ Redis checkpoint storage initialized")
            
        except Exception as e:
            print(f"⚠️  Redis not available, falling back to memory storage: {e}")
            self.available = False
            self.memory_storage = MemoryCheckpointStorage()
    
    def save(self, key: str, checkpoint: UploadCheckpoint) -> None:
        if not self.available:
            return self.memory_storage.save(key, checkpoint)
        
        try:
            # Calculate expiration time
            ttl_seconds = int(checkpoint.expires_at - time.time())
            if ttl_seconds > 0:
                self.redis.setex(
                    f"checkpoint:{key}",
                    ttl_seconds,
                    json.dumps(checkpoint.to_dict())
                )
        except Exception as e:
            print(f"Error saving checkpoint to Redis: {e}")
            if hasattr(self, 'memory_storage'):
                self.memory_storage.save(key, checkpoint)
    
    def load(self, key: str) -> Optional[UploadCheckpoint]:
        if not self.available:
            return self.memory_storage.load(key)
        
        try:
            data_json = self.redis.get(f"checkpoint:{key}")
            if data_json:
                data = json.loads(data_json)
                return UploadCheckpoint.from_dict(data)
            return None
        except Exception as e:
            print(f"Error loading checkpoint from Redis: {e}")
            if hasattr(self, 'memory_storage'):
                return self.memory_storage.load(key)
            return None
    
    def delete(self, key: str) -> None:
        if not self.available:
            return self.memory_storage.delete(key)
        
        try:
            self.redis.delete(f"checkpoint:{key}")
        except Exception as e:
            print(f"Error deleting checkpoint from Redis: {e}")
    
    def list_expired(self) -> List[str]:
        if not self.available:
            return self.memory_storage.list_expired()
        
        try:
            # Redis automatically expires keys, so we don't need to track expired ones
            return []
        except Exception as e:
            print(f"Error listing expired checkpoints from Redis: {e}")
            return []


class CheckpointManager:
    """
    Manages upload checkpoints for resume functionality
    Handles saving, loading, and cleanup of upload progress
    """
    
    def __init__(self, storage_backend: str = 'memory', checkpoint_interval: int = 50):
        self.checkpoint_interval = checkpoint_interval  # Save every N parts
        self.lock = threading.Lock()
        
        # Initialize storage backend
        if storage_backend == 'redis':
            self.storage = RedisCheckpointStorage()
        else:
            self.storage = MemoryCheckpointStorage()
            
        print(f"📋 CheckpointManager initialized:")
        print(f"   Storage: {storage_backend}")
        print(f"   Interval: {checkpoint_interval} parts")
    
    def create_checkpoint(self, upload_session: Dict, multipart_upload_id: str, 
                         total_parts: int) -> str:
        """
        Create initial checkpoint for upload session
        
        Args:
            upload_session: Upload session data
            multipart_upload_id: Notion multipart upload ID
            total_parts: Total number of parts for the upload
            
        Returns:
            Checkpoint key for future reference
        """
        upload_id = upload_session['upload_id']
        
        # Calculate file hash for checkpoint identification
        file_content_hash = upload_session.get('hasher', hashlib.sha512()).hexdigest()[:16]
        checkpoint_key = f"{upload_id}_{file_content_hash}"
        
        checkpoint = UploadCheckpoint(
            upload_id=upload_id,
            file_hash=file_content_hash,
            total_parts=total_parts,
            completed_parts=[],
            multipart_upload_id=multipart_upload_id,
            file_size=upload_session['file_size'],
            filename=upload_session['filename'],
            user_database_id=upload_session['user_database_id'],
            created_at=time.time(),
            last_updated=time.time(),
            expires_at=time.time() + 86400  # 24 hours
        )
        
        self.storage.save(checkpoint_key, checkpoint)
        
        print(f"📋 Created checkpoint: {checkpoint_key}")
        print(f"   File: {checkpoint.filename} ({checkpoint.file_size / 1024 / 1024:.1f}MB)")
        print(f"   Parts: {total_parts}")
        
        return checkpoint_key
    
    def update_checkpoint(self, checkpoint_key: str, completed_part: int) -> None:
        """
        Update checkpoint with completed part
        
        Args:
            checkpoint_key: Checkpoint identifier
            completed_part: Part number that was completed
        """
        with self.lock:
            checkpoint = self.storage.load(checkpoint_key)
            if not checkpoint:
                print(f"⚠️  Checkpoint not found: {checkpoint_key}")
                return
            
            # Add completed part if not already present
            if completed_part not in checkpoint.completed_parts:
                checkpoint.completed_parts.append(completed_part)
                checkpoint.completed_parts.sort()  # Keep sorted for easier processing
                checkpoint.last_updated = time.time()
                
                # Save updated checkpoint
                self.storage.save(checkpoint_key, checkpoint)
                
                # Log progress for significant milestones
                if completed_part % self.checkpoint_interval == 0:
                    progress = checkpoint.get_progress_percent()
                    print(f"📋 Checkpoint updated: {checkpoint_key}")
                    print(f"   Progress: {progress:.1f}% ({len(checkpoint.completed_parts)}/{checkpoint.total_parts} parts)")
    
    def should_create_checkpoint(self, part_number: int) -> bool:
        """
        Determine if a checkpoint should be created for this part
        
        Args:
            part_number: Current part number
            
        Returns:
            True if checkpoint should be created
        """
        return part_number % self.checkpoint_interval == 0
    
    def load_checkpoint(self, checkpoint_key: str) -> Optional[UploadCheckpoint]:
        """
        Load checkpoint for resume
        
        Args:
            checkpoint_key: Checkpoint identifier
            
        Returns:
            UploadCheckpoint if found and valid, None otherwise
        """
        checkpoint = self.storage.load(checkpoint_key)
        if checkpoint and not checkpoint.is_expired():
            return checkpoint
        
        if checkpoint and checkpoint.is_expired():
            print(f"📋 Checkpoint expired: {checkpoint_key}")
            self.storage.delete(checkpoint_key)
        
        return None
    
    def get_resume_info(self, checkpoint_key: str) -> Optional[Dict]:
        """
        Get resume information for an upload
        
        Args:
            checkpoint_key: Checkpoint identifier
            
        Returns:
            Dictionary with resume information or None
        """
        checkpoint = self.load_checkpoint(checkpoint_key)
        if not checkpoint:
            return None
        
        remaining_parts = checkpoint.get_remaining_parts()
        progress = checkpoint.get_progress_percent()
        
        return {
            'upload_id': checkpoint.upload_id,
            'multipart_upload_id': checkpoint.multipart_upload_id,
            'completed_parts': set(checkpoint.completed_parts),
            'remaining_parts': remaining_parts,
            'total_parts': checkpoint.total_parts,
            'progress_percent': progress,
            'file_size': checkpoint.file_size,
            'filename': checkpoint.filename,
            'can_resume': len(remaining_parts) > 0,
            'next_part': min(remaining_parts) if remaining_parts else None
        }
    
    def complete_upload(self, checkpoint_key: str) -> None:
        """
        Mark upload as completed and cleanup checkpoint
        
        Args:
            checkpoint_key: Checkpoint identifier
        """
        checkpoint = self.load_checkpoint(checkpoint_key)
        if checkpoint:
            print(f"✅ Upload completed, cleaning up checkpoint: {checkpoint_key}")
            print(f"   File: {checkpoint.filename}")
            print(f"   Final progress: {checkpoint.get_progress_percent():.1f}%")
            
        self.storage.delete(checkpoint_key)
    
    def cleanup_expired_checkpoints(self) -> int:
        """
        Clean up expired checkpoints
        
        Returns:
            Number of checkpoints cleaned up
        """
        try:
            expired_keys = self.storage.list_expired()
            for key in expired_keys:
                self.storage.delete(key)
            
            if expired_keys:
                print(f"🧹 Cleaned up {len(expired_keys)} expired checkpoints")
            
            return len(expired_keys)
            
        except Exception as e:
            print(f"Error during checkpoint cleanup: {e}")
            return 0
    
    def get_checkpoint_stats(self) -> Dict:
        """Get checkpoint system statistics"""
        # This is a basic implementation - could be enhanced with more detailed stats
        return {
            'storage_type': type(self.storage).__name__,
            'checkpoint_interval': self.checkpoint_interval,
            'storage_available': True  # Could check storage health here
        }


# Global checkpoint manager instance
checkpoint_manager = CheckpointManager(
    storage_backend='redis',  # Try Redis first, fallback to memory
    checkpoint_interval=50    # Save every 50 parts (250MB for 5MB chunks)
)


def create_checkpoint_key(upload_id: str, file_hash: str) -> str:
    """Utility function to create standardized checkpoint keys"""
    return f"{upload_id}_{file_hash[:16]}"


def log_checkpoint_stats():
    """Utility function to log checkpoint system statistics"""
    stats = checkpoint_manager.get_checkpoint_stats()
    print(f"📋 CHECKPOINT_STATS:")
    print(f"   Storage: {stats['storage_type']}")
    print(f"   Interval: {stats['checkpoint_interval']} parts")
    print(f"   Available: {stats['storage_available']}")