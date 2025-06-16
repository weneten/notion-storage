#!/usr/bin/env python3
"""
Test script to validate the upload fix implementation
This script tests the database integration and parallel processing components
"""

import sys
import os
import io
import hashlib
import uuid
from unittest.mock import Mock, MagicMock

# Add the project root to the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_parallel_processor():
    """Test the parallel chunk processor"""
    print("Testing ParallelChunkProcessor...")
    
    try:
        from uploader.parallel_processor import ParallelChunkProcessor, generate_salt, calculate_salted_hash
        
        # Test salt generation
        salt = generate_salt()
        assert len(salt) == 64, f"Salt should be 64 chars, got {len(salt)}"
        print("✓ Salt generation works")
        
        # Test hash calculation
        file_hash = "test_hash"
        salted_hash = calculate_salted_hash(file_hash, salt)
        assert len(salted_hash) == 128, f"Salted hash should be 128 chars, got {len(salted_hash)}"
        print("✓ Salted hash calculation works")
        
        # Test processor initialization
        mock_uploader = Mock()
        mock_socketio = Mock()
        upload_session = {
            'upload_id': str(uuid.uuid4()),
            'filename': 'test.txt',
            'file_size': 30 * 1024 * 1024,  # 30MB
            'hasher': hashlib.sha512()
        }
        
        processor = ParallelChunkProcessor(
            max_workers=2,
            notion_uploader=mock_uploader,
            upload_session=upload_session,
            socketio=mock_socketio
        )
        
        assert processor.max_workers == 2
        assert processor.chunk_size == 5 * 1024 * 1024
        print("✓ ParallelChunkProcessor initialization works")
        
        print("✓ ParallelChunkProcessor tests passed")
        return True
        
    except Exception as e:
        print(f"✗ ParallelChunkProcessor test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_streaming_uploader():
    """Test the streaming uploader with database integration"""
    print("Testing NotionStreamingUploader...")
    
    try:
        from uploader.streaming_uploader import NotionStreamingUploader
        
        # Mock dependencies
        mock_notion_uploader = Mock()
        mock_notion_uploader.global_file_index_db_id = "test_global_db_id"
        mock_notion_uploader.add_file_to_user_database.return_value = {'id': 'test_file_id'}
        mock_notion_uploader.add_file_to_index.return_value = {'status': 'success'}
        
        mock_socketio = Mock()
        
        uploader = NotionStreamingUploader(
            api_token="test_token",
            socketio=mock_socketio,
            notion_uploader=mock_notion_uploader
        )
        
        # Test upload session creation
        session = uploader.create_upload_session(
            filename="test.txt",
            file_size=30 * 1024 * 1024,  # 30MB (multipart)
            user_database_id="test_db_id"
        )
        
        assert session['is_multipart'] == True
        assert 'hasher' in session
        print("✓ Upload session creation works")
        
        # Test database integration
        mock_notion_result = {'file_upload_id': 'test_upload_id'}
        
        result = uploader.complete_upload_with_database_integration(
            session, mock_notion_result
        )
        
        assert 'file_id' in result
        assert 'file_hash' in result
        assert result['status'] == 'completed'
        print("✓ Database integration works")
        
        # Verify that the required methods were called
        mock_notion_uploader.add_file_to_user_database.assert_called_once()
        mock_notion_uploader.add_file_to_index.assert_called_once()
        print("✓ Database methods called correctly")
        
        print("✓ NotionStreamingUploader tests passed")
        return True
        
    except Exception as e:
        print(f"✗ NotionStreamingUploader test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_memory_manager():
    """Test the memory manager"""
    print("Testing MemoryManager...")
    
    try:
        from uploader.parallel_processor import MemoryManager
        
        manager = MemoryManager(max_memory_mb=50)
        
        # Test memory allocation
        manager.allocate_chunk_memory("chunk1", 10 * 1024 * 1024)  # 10MB
        stats = manager.get_memory_stats()
        assert stats['tracked_usage'] == 10 * 1024 * 1024
        assert stats['active_chunks'] == 1
        print("✓ Memory allocation works")
        
        # Test memory deallocation
        manager.free_chunk_memory("chunk1")
        stats = manager.get_memory_stats()
        assert stats['tracked_usage'] == 0
        assert stats['active_chunks'] == 0
        print("✓ Memory deallocation works")
        
        # Test memory limit
        try:
            manager.allocate_chunk_memory("big_chunk", 100 * 1024 * 1024)  # 100MB (exceeds limit)
            assert False, "Should have raised MemoryError"
        except MemoryError:
            print("✓ Memory limit enforcement works")
        
        print("✓ MemoryManager tests passed")
        return True
        
    except Exception as e:
        print(f"✗ MemoryManager test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_constants():
    """Test that constants are properly defined"""
    print("Testing constants...")
    
    try:
        from uploader.parallel_processor import SINGLE_PART_THRESHOLD, MULTIPART_CHUNK_SIZE
        from uploader.streaming_uploader import NotionStreamingUploader
        
        assert SINGLE_PART_THRESHOLD == 20 * 1024 * 1024
        assert MULTIPART_CHUNK_SIZE == 5 * 1024 * 1024
        
        uploader = NotionStreamingUploader("test", None, None)
        assert uploader.SINGLE_PART_THRESHOLD == 20 * 1024 * 1024
        assert uploader.MULTIPART_CHUNK_SIZE == 5 * 1024 * 1024
        
        print("✓ Constants are properly defined")
        return True
        
    except Exception as e:
        print(f"✗ Constants test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Run all tests"""
    print("🚀 Starting upload fix validation tests...\n")
    
    tests = [
        test_constants,
        test_parallel_processor,
        test_memory_manager,
        test_streaming_uploader,
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        print(f"\n{'='*50}")
        if test():
            passed += 1
        else:
            failed += 1
    
    print(f"\n{'='*50}")
    print(f"📊 Test Results:")
    print(f"  ✓ Passed: {passed}")
    print(f"  ✗ Failed: {failed}")
    print(f"  📈 Success Rate: {passed/(passed+failed)*100:.1f}%")
    
    if failed == 0:
        print("\n🎉 All tests passed! The upload fix implementation is ready.")
        return 0
    else:
        print(f"\n❌ {failed} test(s) failed. Please check the implementation.")
        return 1

if __name__ == "__main__":
    sys.exit(main())