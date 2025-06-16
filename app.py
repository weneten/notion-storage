from flask import Flask, request, jsonify, render_template, redirect, url_for, session, g, Response, stream_with_context
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from flask_socketio import SocketIO
from flask_cors import CORS
from uploader import NotionFileUploader, ChunkProcessor
from uploader.streaming_uploader import StreamingUploadManager
from dotenv import load_dotenv
import os
import secrets
import hashlib
import concurrent.futures
import math
import base64
import bcrypt
import mimetypes
import traceback
from typing import Dict, Any, List
import threading
import time
import uuid
import random
import string
import psutil  # Add psutil for memory monitoring
import json
from flask_socketio import emit

# Function to get current memory usage
def get_memory_usage():
    """Get current memory usage of the process in MB"""
    try:
        process = psutil.Process(os.getpid())
        memory_info = process.memory_info()
        memory_mb = memory_info.rss / (1024 * 1024)  # Convert to MB
        return memory_mb
    except Exception as e:
        print(f"Error getting memory usage: {e}")
        return 0

# Function to log memory usage periodically and clean up old sessions
def log_memory_usage():
    """Log memory usage every minute and clean up old upload sessions"""
    try:
        memory_mb = get_memory_usage()
        cached_chunks_count = 0
        cached_chunks_size = 0
        current_time = time.time()
        
        if hasattr(app, 'upload_processors'):
            # Clean up old upload sessions (older than 30 minutes)
            expired_sessions = []
            for upload_id, upload_data in app.upload_processors.items():
                last_activity = upload_data.get('last_activity', 0)
                if current_time - last_activity > 1800:  # 30 minutes
                    expired_sessions.append(upload_id)
                else:
                    # Count active sessions
                    cached_chunks = upload_data.get('cached_chunks', {})
                    cached_chunks_count += len(cached_chunks)
                    cached_chunks_size += sum(len(chunk) for chunk in cached_chunks.values())
            
            # Clean up expired sessions
            for upload_id in expired_sessions:
                print(f"Cleaning up expired upload session: {upload_id}")
                cleanup_upload_session(upload_id)
        
        # Clean up old metadata entries
        if hasattr(app, 'upload_metadata'):
            expired_metadata = []
            for session_id, metadata in app.upload_metadata.items():
                if current_time - metadata.get('timestamp', 0) > 300:  # 5 minutes
                    expired_metadata.append(session_id)
            
            for session_id in expired_metadata:
                del app.upload_metadata[session_id]
                print(f"Cleaned up expired metadata for session: {session_id}")
        
        cached_chunks_mb = cached_chunks_size / (1024 * 1024)
        active_sessions = len(app.upload_processors) if hasattr(app, 'upload_processors') else 0
        print(f"MEMORY: Process using {memory_mb:.2f} MB | Active sessions: {active_sessions} | Cached chunks: {cached_chunks_count} ({cached_chunks_mb:.2f} MB)")
        
    except Exception as e:
        print(f"Error in memory logging: {e}")
        
    # Schedule next check
    threading.Timer(60, log_memory_usage).start()
    
# Function to log memory usage periodically
def log_memory_usage_old():
    """Log memory usage every minute"""
    try:
        memory_mb = get_memory_usage()
        cached_chunks_count = 0
        cached_chunks_size = 0
        
        if hasattr(app, 'upload_processors'):
            for upload_id, upload_data in app.upload_processors.items():
                cached_chunks = upload_data.get('cached_chunks', {})
                cached_chunks_count += len(cached_chunks)
                cached_chunks_size += sum(len(chunk) for chunk in cached_chunks.values())
        
        cached_chunks_mb = cached_chunks_size / (1024 * 1024)
        print(f"MEMORY: Process using {memory_mb:.2f} MB | Cached chunks: {cached_chunks_count} ({cached_chunks_mb:.2f} MB)")
    except Exception as e:
        print(f"Error in memory logging: {e}")
        
    # Schedule next check
    threading.Timer(60, log_memory_usage_old).start()
    
# Load environment variables from .env file
load_dotenv()

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY')  # Default secret key for development
CORS(app)  # Enable CORS for all routes
socketio = SocketIO(
    app, 
    cors_allowed_origins="*", 
    binary=True,
    async_mode='eventlet',
    max_http_buffer_size=5 * 1024 * 1024,  # Reduce to 5MB buffer
    ping_timeout=60,
    ping_interval=25
)

# Add memory limit protection
MAX_MEMORY_PERCENT = 80  # Max memory usage percentage before rejecting uploads

# Function to check if memory usage is too high
def is_memory_usage_high():
    try:
        process = psutil.Process(os.getpid())
        memory_info = process.memory_info()
        memory_percent = process.memory_percent()
        print(f"DEBUG: Current memory usage: {memory_percent:.2f}% ({memory_info.rss / (1024 * 1024):.2f} MB)")
        return memory_percent > MAX_MEMORY_PERCENT
    except Exception as e:
        print(f"ERROR checking memory usage: {e}")
        return False  # Assume it's safe if we can't check

# Initialize global upload state containers
app.upload_locks = {}
app.upload_processors = {}

# Add global metadata store
app.upload_metadata = {}

def format_bytes(bytes, decimals=2):
    if bytes == 0:
        return '0 Bytes'
    k = 1000  # Changed from 1024 to 1000 to display MB/GB instead of MiB/GiB
    dm = 0 if decimals < 0 else decimals
    sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']
    i = math.floor(math.log(bytes) / math.log(k))
    return f"{round(bytes / (k ** i), dm)} {sizes[i]}"

app.jinja_env.filters['format_bytes'] = format_bytes

# Initialize Flask-Login
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

# Configure your Notion API token and page ID here
NOTION_API_TOKEN = os.environ.get('NOTION_API_TOKEN')
NOTION_USER_DB_ID = os.environ.get('NOTION_USER_DB_ID')
GLOBAL_FILE_INDEX_DB_ID = os.environ.get('GLOBAL_FILE_INDEX_DB_ID')

uploader = NotionFileUploader(api_token=NOTION_API_TOKEN, socketio=socketio, global_file_index_db_id=GLOBAL_FILE_INDEX_DB_ID)

# Initialize streaming upload manager
streaming_upload_manager = StreamingUploadManager(api_token=NOTION_API_TOKEN, socketio=socketio, notion_uploader=uploader)

# User class for Flask-Login
class User(UserMixin):
    def __init__(self, id, username, password_hash):
        self.id = id
        self.username = username
        self.password_hash = password_hash

    def check_password(self, password):
        # Decode base64 string to bytes
        hashed_bytes = base64.b64decode(self.password_hash)
        return bcrypt.checkpw(password.encode('utf-8'), hashed_bytes)

@login_manager.user_loader
def load_user(user_id):
    try:
        user_data = uploader.get_user_by_id(user_id)
        if user_data:
            username = user_data.get('properties', {}).get('Name', {}).get('title', [{}])[0].get('text', {}).get('content', '')
            password_hash = user_data.get('properties', {}).get('Password-Hash', {}).get('rich_text', [{}])[0].get('text', {}).get('content', '')
            return User(id=user_id, username=username, password_hash=password_hash)
    except Exception as e:
        print(f"Error loading user: {e}")
    return None

@app.route('/')
@login_required
def home():
    try:
        user_database_id = uploader.get_user_database_id(current_user.id)
        files = []
        if user_database_id:
            # Ensure 'is_public' and 'salt' properties exist in the user's database

            files_data = uploader.get_files_from_user_database(user_database_id)
            for file_data in files_data.get('results', []):
                try:
                    properties = file_data.get('properties', {})
                    # The filename in title property is the original filename
                    name = properties.get('filename', {}).get('title', [{}])[0].get('text', {}).get('content', '')
                    
                    size = properties.get('filesize', {}).get('number', 0)
                    file_id = file_data.get('id') # Extract the Notion page ID
                    is_public = properties.get('is_public', {}).get('checkbox', False) # Get is_public status
                    file_hash = properties.get('filehash', {}).get('rich_text', [{}])[0].get('text', {}).get('content', '') # Get filehash
                    
                    if name:
                        files.append({
                            "name": name,  # This is already the original filename
                            "size": size,
                            "id": file_id, # Add the file_id to the dictionary
                            "is_public": is_public, # Add is_public status
                            "file_hash": file_hash # Add file_hash
                        })
                except Exception as e:
                    print(f"Error processing file data in home route: {e}")
                    continue
        return render_template('home.html', files=files)
    except Exception as e:
        return f"Error loading home page: {str(e)}", 500

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')

        try:
            # Query Notion user database
            result = uploader.query_user_database_by_username(NOTION_USER_DB_ID, username)
            users = result.get('results', [])

            if not users:
                return "Benutzer nicht gefunden", 401

            # Get first matching user
            user_data = users[0]
            properties = user_data.get('properties', {})

            # Extract user details
            user_id = user_data.get('id')
            password_hash = properties.get('Password-Hash', {}).get('rich_text', [{}])[0].get('text', {}).get('content', '')

            # Create user object
            user = User(id=user_id, username=username, password_hash=password_hash)

            # Check password
            if user.check_password(password):
                login_user(user)
                return redirect(url_for('home'))
            else:
                return "Ungültige Anmeldedaten", 401

        except Exception as e:
            return f"Fehler bei der Anmeldung: {str(e)}", 500

    return render_template('login.html')

@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')

        # Hash password
        # Encode hash as base64 string for safe storage
        hashed_bytes = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())
        password_hash = base64.b64encode(hashed_bytes).decode('utf-8')

        try:
            # Ensure the User Database relation property exists in the main user database
            uploader.ensure_database_property(
                NOTION_USER_DB_ID,
                'User Database',
                'url',
                {}
            )

            # Create user in Notion database
            result = uploader.create_user(NOTION_USER_DB_ID, username, password_hash)
            user_id = result.get('id')

            # Create user-specific directory for file storage

            # Create a Notion database for the user to store file information
            try:
                user_database = uploader.create_user_database(user_id, username)
                if not user_database or not user_database.get('url'):
                    raise Exception("Failed to create user database")

                # Update user properties with database URL
                uploader.update_user_properties(user_id, {
                    'User Database': {
                        'url': user_database['url']
                    }
                })
            except Exception as db_error:
                # Clean up user directory if database creation fails
                raise Exception(f"Failed to create user database: {str(db_error)}")

            return redirect(url_for('login'))
        except Exception as e:
            import traceback
            error_details = traceback.format_exc()
            return f"Fehler bei der Registrierung: {str(e)}\n\nDetails:\n{error_details}", 500

    return render_template('register.html')

@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('login'))


# TEMPORARY: Disable old upload routes to force streaming upload usage
# These can be re-enabled later if needed for backwards compatibility

# @app.route('/upload_file', methods=['POST'])
# @login_required
# def upload_file():
#     # OLD UPLOAD ROUTE - DISABLED FOR TESTING
#     return jsonify({"error": "Old upload route disabled - use streaming upload"}), 403

# @app.route('/init_upload', methods=['POST']) 
# @login_required
# def init_upload():
#     # OLD INIT ROUTE - DISABLED FOR TESTING
#     return jsonify({"error": "Old init route disabled - use streaming upload"}), 403

# @app.route('/finalize_upload', methods=['POST'])
# @login_required  
# def finalize_upload():
#     # OLD FINALIZE ROUTE - DISABLED FOR TESTING
#     return jsonify({"error": "Old finalize route disabled - use streaming upload"}), 403

# ============================================================================
# STREAMING UPLOAD API ENDPOINTS
# ============================================================================

@app.route('/api/upload/create-session', methods=['POST'])
@login_required
def create_streaming_upload_session():
    """
    Create a new streaming upload session
    """
    try:
        print("DEBUG: Streaming upload session creation called")
        data = request.get_json()
        print(f"DEBUG: Request data: {data}")
        
        filename = data.get('filename')
        file_size = data.get('fileSize')
        content_type = data.get('contentType', 'application/octet-stream')
        
        print(f"DEBUG: Creating session for {filename}, size: {file_size}")
        
        if not filename or not file_size:
            print("DEBUG: Missing filename or fileSize")
            return jsonify({'error': 'Missing filename or fileSize'}), 400
        
        # Get user database ID
        user_database_id = uploader.get_user_database_id(current_user.id)
        if not user_database_id:
            print("DEBUG: User database not found")
            return jsonify({'error': 'User database not found'}), 404
        
        print(f"DEBUG: User database ID: {user_database_id}")
        
        # Create upload session
        upload_id = streaming_upload_manager.create_upload_session(
            filename=filename,
            file_size=file_size,
            user_database_id=user_database_id,
            progress_callback=None  # Will use SocketIO for progress updates
        )
        
        print(f"DEBUG: Created upload session with ID: {upload_id}")
        
        response = {
            'upload_id': upload_id,
            'status': 'ready',
            'filename': filename,
            'file_size': file_size,
            'is_multipart': file_size > streaming_upload_manager.uploader.SINGLE_PART_THRESHOLD
        }
        
        print(f"DEBUG: Returning response: {response}")
        return jsonify(response)
        
    except Exception as e:
        print(f"Error creating upload session: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/api/upload/stream/<upload_id>', methods=['POST'])
@login_required
def stream_file_upload(upload_id):
    """
    Handle streaming file upload
    """
    try:
        print(f"DEBUG: Streaming upload called for upload_id: {upload_id}")
        print(f"DEBUG: Request headers: {dict(request.headers)}")
        
        # Get upload session
        upload_session = streaming_upload_manager.get_upload_status(upload_id)
        if not upload_session:
            print(f"DEBUG: Upload session {upload_id} not found")
            return jsonify({'error': 'Upload session not found'}), 404
        
        print(f"DEBUG: Found upload session: {upload_session['filename']}, size: {upload_session['file_size']}")
        
        # Verify file size matches headers
        content_length = request.headers.get('Content-Length')
        expected_size = upload_session['file_size']
        
        print(f"DEBUG: Content-Length: {content_length}, Expected: {expected_size}")
        
        if content_length and int(content_length) != expected_size:
            print(f"DEBUG: Content-Length mismatch: {content_length} vs {expected_size}")
            return jsonify({'error': 'Content-Length mismatch'}), 400
        
        # Create a generator that yields chunks from the request stream
        def stream_generator():
            try:
                print("DEBUG: Starting to read request stream")
                # Read in small chunks to avoid memory issues
                chunk_size = 64 * 1024  # 64KB chunks
                total_read = 0
                while True:
                    chunk = request.stream.read(chunk_size)
                    if not chunk:
                        print(f"DEBUG: Stream ended, total read: {total_read}")
                        break
                    total_read += len(chunk)
                    if total_read % (1024 * 1024) == 0:  # Log every MB
                        print(f"DEBUG: Read {total_read / (1024*1024):.1f} MB")
                    yield chunk
            except Exception as e:
                print(f"Error reading request stream: {e}")
                raise
        
        print("DEBUG: Processing upload stream")
        # Process the stream
        result = streaming_upload_manager.process_upload_stream(upload_id, stream_generator())
        
        print(f"DEBUG: Upload processing completed: {result}")
        return jsonify({
            'status': 'completed',
            'upload_id': upload_id,
            'filename': result['filename'],
            'file_size': result['bytes_uploaded'],
            'file_hash': result['file_hash'],
            'notion_file_id': result.get('notion_file_id')
        })
        
    except Exception as e:
        print(f"Error in streaming upload: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/api/upload/status/<upload_id>', methods=['GET'])
@login_required
def get_upload_status(upload_id):
    """
    Get the status of an upload session
    """
    try:
        upload_session = streaming_upload_manager.get_upload_status(upload_id)
        if not upload_session:
            return jsonify({'error': 'Upload session not found'}), 404
        
        # Return safe status information
        return jsonify({
            'upload_id': upload_id,
            'status': upload_session['status'],
            'filename': upload_session['filename'],
            'file_size': upload_session['file_size'],
            'bytes_uploaded': upload_session['bytes_uploaded'],
            'is_multipart': upload_session['is_multipart'],
            'created_at': upload_session['created_at']
        })
        
    except Exception as e:
        print(f"Error getting upload status: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/upload/abort/<upload_id>', methods=['POST'])
@login_required
def abort_upload(upload_id):
    """
    Abort an active upload session
    """
    try:
        upload_session = streaming_upload_manager.get_upload_status(upload_id)
        if not upload_session:
            return jsonify({'message': 'Upload session not found or already completed'}), 200
        
        # Mark as aborted
        upload_session['status'] = 'aborted'
        upload_session['aborted_at'] = time.time()
        
        # If it's a multipart upload, abort it with Notion
        if upload_session.get('is_multipart') and upload_session['status'] in ['uploading', 'initialized']:
            streaming_upload_manager.uploader._abort_multipart_upload(upload_session)
        
        return jsonify({'message': 'Upload aborted successfully'})
        
    except Exception as e:
        print(f"Error aborting upload: {e}")
        return jsonify({'error': str(e)}), 500


# ============================================================================
# HELPER FUNCTIONS FOR LEGACY COMPATIBILITY
# ============================================================================

def cleanup_upload_session(upload_id):
    """
    Clean up a specific upload session (legacy compatibility)
    """
    try:
        with app.upload_locks.get(upload_id, threading.Lock()):
            if upload_id in app.upload_processors:
                del app.upload_processors[upload_id]
                print(f"Cleaned up legacy upload session: {upload_id}")
    except Exception as e:
        print(f"Error cleaning up upload session {upload_id}: {e}")


def process_websocket_chunk_robust(upload_id, part_number, chunk_data, is_last_chunk, chunk_size, session_id):
    """
    Legacy WebSocket chunk processing function (placeholder for compatibility)
    """
    try:
        print(f"Legacy WebSocket chunk processing called for upload {upload_id}, part {part_number}")
        # This is a placeholder - the new streaming upload doesn't use this method
        # but it's referenced in some legacy code paths
        pass
    except Exception as e:
        print(f"Error in legacy WebSocket chunk processing: {e}")


# ============================================================================
# END HELPER FUNCTIONS
# ============================================================================

# Background task to clean up old upload sessions
def cleanup_upload_sessions():
    """
    Clean up old upload sessions periodically
    """
    try:
        streaming_upload_manager.cleanup_old_sessions(max_age_seconds=3600)
    except Exception as e:
        print(f"Error cleaning up upload sessions: {e}")
    
    # Schedule next cleanup
    threading.Timer(300, cleanup_upload_sessions).start()  # Every 5 minutes


# Start the cleanup task
cleanup_upload_sessions()

# ============================================================================
# END OF STREAMING UPLOAD API
# ============================================================================
