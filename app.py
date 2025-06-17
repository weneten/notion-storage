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


@app.route('/change_password', methods=['GET', 'POST'])
@login_required
def change_password():
    if request.method == 'POST':
        current_password = request.form.get('current_password')
        new_password = request.form.get('new_password')
        confirm_password = request.form.get('confirm_password')

        # Verify new password matches confirmation
        if new_password != confirm_password:
            return "New password and confirmation do not match", 400

        # Verify current password
        if not current_user.check_password(current_password):
            return "Current password is incorrect", 401

        # Hash new password
        hashed_bytes = bcrypt.hashpw(new_password.encode('utf-8'), bcrypt.gensalt())
        new_password_hash = base64.b64encode(hashed_bytes).decode('utf-8')

        try:
            # Update user password in Notion
            uploader.update_user_password(current_user.id, new_password_hash)
            return redirect(url_for('home'))
        except Exception as e:
            return f"Error changing password: {str(e)}", 500

    return render_template('change_password.html')


# ============================================================================
# FILE DOWNLOAD ROUTES
# ============================================================================

@app.route('/download/<filename>')
@login_required
def download_file(filename):
    """
    Download file by filename (authenticated route) - redirects to hash-based download
    """
    try:
        user_database_id = uploader.get_user_database_id(current_user.id)
        if not user_database_id:
            return "User database not found", 404

        files_data = uploader.get_files_from_user_database(user_database_id)
        file_hash = None
        for file_data in files_data.get('results', []):
            properties = file_data.get('properties', {})
            name = properties.get('filename', {}).get('title', [{}])[0].get('text', {}).get('content', '')
            if name == filename:
                file_hash = properties.get('filehash', {}).get('rich_text', [{}])[0].get('text', {}).get('content', '')
                break

        if file_hash:
            return redirect(url_for('download_by_hash', salted_sha512_hash=file_hash))
        else:
            return "File not found in database", 404
    except Exception as e:
        return str(e), 500


@app.route('/d/<salted_sha512_hash>')
def download_by_hash(salted_sha512_hash):
    """
    Download file by hash (public/private route with access control)
    """
    try:
        # Find the file in the global file index using the hash
        index_entry = uploader.get_file_by_salted_sha512_hash(salted_sha512_hash)

        if not index_entry:
            return "File not found", 404

        properties = index_entry.get('properties', {})
        
        # Extract properties from the index_entry
        is_public = properties.get('Is Public', {}).get('checkbox', False)
        file_page_id = properties.get('File Page ID', {}).get('rich_text', [{}])[0].get('text', {}).get('content', '')
        file_user_db_id = properties.get('User Database ID', {}).get('rich_text', [{}])[0].get('text', {}).get('content', '')
        original_filename = properties.get('Original Filename', {}).get('title', [{}])[0].get('text', {}).get('content', 'download')

        # Now fetch the actual file details from the user's specific database using file_page_id
        # This is necessary to get the actual download link and other file-specific properties
        file_details = uploader.get_user_by_id(file_page_id)
        if not file_details:
            return "File details not found in user database.", 404

        # Explicitly retrieve a new signed download URL from Notion
        notion_download_link = uploader.get_notion_file_url_from_page_property(file_page_id, original_filename)

        # Check access control
        if not is_public:
            if not current_user.is_authenticated:
                return redirect(url_for('login', next=request.url))

            current_user_db_id = uploader.get_user_database_id(current_user.id)

            if file_user_db_id != current_user_db_id:
                return "Access Denied: You do not have permission to download this file.", 403

        if not notion_download_link:
            return "Download link not available for this file", 500

        # Import necessary modules for streaming
        import mimetypes
        mimetype, _ = mimetypes.guess_type(original_filename)
        if not mimetype:
            mimetype = 'application/octet-stream'

        response = Response(stream_with_context(uploader.stream_file_from_notion(notion_download_link)), mimetype=mimetype)
        response.headers['Content-Disposition'] = f'attachment; filename="{original_filename}"'
        return response

    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        print(f"DEBUG: Error in /d/<hash> route: {str(e)}\n{error_trace}")
        return "An error occurred during download.", 500


@app.route('/v/<salted_sha512_hash>')
def stream_by_hash(salted_sha512_hash):
    """
    Stream file by hash with HTTP Range Request support for inline media viewing
    """
    try:
        # Find the file in the global file index using the hash
        index_entry = uploader.get_file_by_salted_sha512_hash(salted_sha512_hash)

        if not index_entry:
            return "File not found", 404

        properties = index_entry.get('properties', {})
        
        # Extract properties from the index_entry
        is_public = properties.get('Is Public', {}).get('checkbox', False)
        file_page_id = properties.get('File Page ID', {}).get('rich_text', [{}])[0].get('text', {}).get('content', '')
        file_user_db_id = properties.get('User Database ID', {}).get('rich_text', [{}])[0].get('text', {}).get('content', '')
        original_filename = properties.get('Original Filename', {}).get('title', [{}])[0].get('text', {}).get('content', 'video')

        # Now fetch the actual file details from the user's specific database using file_page_id
        file_details = uploader.get_user_by_id(file_page_id)
        if not file_details:
            return "File details not found in user database.", 404

        # Check access control (same logic as download route)
        if not is_public:
            if not current_user.is_authenticated:
                return redirect(url_for('login', next=request.url))

            current_user_db_id = uploader.get_user_database_id(current_user.id)

            if file_user_db_id != current_user_db_id:
                return "Access Denied: You do not have permission to view this file.", 403

        # Get the file size from the file details
        file_properties = file_details.get('properties', {})
        file_size = file_properties.get('filesize', {}).get('number', 0)

        # Explicitly retrieve a new signed download URL from Notion
        notion_download_link = uploader.get_notion_file_url_from_page_property(file_page_id, original_filename)

        if not notion_download_link:
            return "Stream link not available for this file", 500

        # Enhanced MIME type detection for media files
        def get_enhanced_mimetype(filename):
            """Enhanced MIME type detection for common media formats"""
            import mimetypes
            
            # Get MIME type from filename
            mimetype, _ = mimetypes.guess_type(filename)
            
            if mimetype:
                return mimetype
            
            # Fallback based on file extension for common media types
            extension = filename.lower().split('.')[-1] if '.' in filename else ''
            
            media_types = {
                # Video formats
                'mp4': 'video/mp4',
                'webm': 'video/webm',
                'avi': 'video/x-msvideo',
                'mov': 'video/quicktime',
                'mkv': 'video/x-matroska',
                'wmv': 'video/x-ms-wmv',
                'flv': 'video/x-flv',
                'm4v': 'video/x-m4v',
                
                # Audio formats
                'mp3': 'audio/mpeg',
                'wav': 'audio/wav',
                'ogg': 'audio/ogg',
                'aac': 'audio/aac',
                'flac': 'audio/flac',
                'm4a': 'audio/mp4',
                'wma': 'audio/x-ms-wma',
                
                # Image formats
                'jpg': 'image/jpeg',
                'jpeg': 'image/jpeg',
                'png': 'image/png',
                'gif': 'image/gif',
                'webp': 'image/webp',
                'svg': 'image/svg+xml',
                'bmp': 'image/bmp',
                'tiff': 'image/tiff',
                
                # Document formats
                'pdf': 'application/pdf',
                'txt': 'text/plain',
                'html': 'text/html',
                'css': 'text/css',
                'js': 'application/javascript'
            }
            
            return media_types.get(extension, 'application/octet-stream')

        mimetype = get_enhanced_mimetype(original_filename)

        # Parse Range header for partial content requests
        range_header = request.headers.get('Range', '').strip()
        
        if range_header:
            # Parse Range header (e.g., "bytes=0-1023", "bytes=1024-", "bytes=-500")
            if not range_header.startswith('bytes='):
                return "Invalid range header", 416
            
            try:
                range_spec = range_header[6:]  # Remove "bytes="
                
                if '-' not in range_spec:
                    return "Invalid range format", 416
                
                range_start, range_end = range_spec.split('-', 1)
                
                # Handle different range formats
                if range_start and range_end:
                    # bytes=start-end
                    start = int(range_start)
                    end = int(range_end)
                elif range_start and not range_end:
                    # bytes=start-
                    start = int(range_start)
                    end = file_size - 1
                elif not range_start and range_end:
                    # bytes=-suffix (last N bytes)
                    suffix_length = int(range_end)
                    start = max(0, file_size - suffix_length)
                    end = file_size - 1
                else:
                    return "Invalid range format", 416
                
                # Validate range
                if start < 0 or end >= file_size or start > end:
                    response = Response(status=416)
                    response.headers['Content-Range'] = f'bytes */{file_size}'
                    return response
                
                # Stream the requested range
                def stream_range():
                    for chunk in uploader.stream_file_from_notion_range(notion_download_link, start, end):
                        yield chunk
                
                # Create partial content response
                response = Response(stream_with_context(stream_range()), mimetype=mimetype, status=206)
                response.headers['Content-Disposition'] = f'inline; filename="{original_filename}"'
                response.headers['Content-Length'] = str(end - start + 1)
                response.headers['Content-Range'] = f'bytes {start}-{end}/{file_size}'
                response.headers['Accept-Ranges'] = 'bytes'
                response.headers['Cache-Control'] = 'public, max-age=3600'
                
                return response
                
            except (ValueError, TypeError) as e:
                return "Invalid range values", 416
        
        else:
            # Full content request
            response = Response(stream_with_context(uploader.stream_file_from_notion(notion_download_link)), mimetype=mimetype)
            response.headers['Content-Disposition'] = f'inline; filename="{original_filename}"'
            response.headers['Content-Length'] = str(file_size)
            response.headers['Accept-Ranges'] = 'bytes'
            response.headers['Cache-Control'] = 'public, max-age=3600'
            
            return response

    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        print(f"DEBUG: Error in /v/<hash> route: {str(e)}\n{error_trace}")
        return "An error occurred during streaming.", 500


@app.route('/api/files')
@login_required
def get_files_api():
    """
    API endpoint to get user's files (for AJAX requests) - WITH DIAGNOSTIC LOGGING
    """
    print("🔍 DIAGNOSTIC: /api/files endpoint called (used by streaming upload)")
    try:
        user_database_id = uploader.get_user_database_id(current_user.id)
        print(f"🔍 DIAGNOSTIC: User database ID: {user_database_id}")
        
        if not user_database_id:
            print("🚨 DIAGNOSTIC: User database not found")
            return jsonify({'error': 'User database not found'}), 404
        
        files_response = uploader.get_files_from_user_database(user_database_id)
        files = files_response.get('results', [])
        
        print(f"🔍 DIAGNOSTIC: Raw files from database: {len(files)} files")
        
        # Format files for JSON response
        formatted_files = []
        for i, file_data in enumerate(files):
            file_props = file_data.get('properties', {})
            
            # Extract all properties with diagnostic logging
            file_id = file_data.get('id')
            name = file_props.get('filename', {}).get('title', [{}])[0].get('text', {}).get('content', 'Unknown')
            size = file_props.get('filesize', {}).get('number', 0)
            file_hash = file_props.get('filehash', {}).get('rich_text', [{}])[0].get('text', {}).get('content', '')
            is_public = file_props.get('is_public', {}).get('checkbox', False)
            
            print(f"🔍 DIAGNOSTIC: File {i+1} - {name}:")
            print(f"  - ID: {file_id} (needed for delete button)")
            print(f"  - Hash: {file_hash} (needed for toggle)")
            print(f"  - Is Public: {is_public} (needed for toggle state)")
            print(f"  - Size: {size}")
            print(f"  - Has all button data: {bool(file_id and file_hash is not None and is_public is not None)}")
            
            formatted_file = {
                'id': file_id,
                'name': name,
                'size': size,
                'file_hash': file_hash,
                'is_public': is_public
            }
            
            formatted_files.append(formatted_file)
        
        print(f"🔍 DIAGNOSTIC: Returning {len(formatted_files)} formatted files to frontend")
        if formatted_files:
            print(f"🔍 DIAGNOSTIC: First file in response: {formatted_files[0]}")
        
        return jsonify({'files': formatted_files})
        
    except Exception as e:
        print(f"🚨 DIAGNOSTIC: Error in /api/files: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/files-api')
@login_required
def list_files_api():
    """
    Legacy API endpoint to get user's files (matches old code implementation)
    """
    try:
        # Get user's database ID
        user_database_id = uploader.get_user_database_id(current_user.id)
        if not user_database_id:
            return jsonify({"error": "No user database ID found"}), 404
            
        # Query files from Notion database using uploader's method
        files_data = uploader.get_files_from_user_database(user_database_id)
        
        # Format files for API response (matches old code format)
        files = []
        for file_data in files_data.get('results', []):
            try:
                properties = file_data.get('properties', {})
                name = properties.get('filename', {}).get('title', [{}])[0].get('text', {}).get('content', '')
                size = properties.get('filesize', {}).get('number', 0)
                file_id = file_data.get('id')  # Extract the Notion page ID
                is_public = properties.get('is_public', {}).get('checkbox', False)  # Get is_public status
                file_hash = properties.get('filehash', {}).get('rich_text', [{}])[0].get('text', {}).get('content', '') # Get filehash
                
                if name:  # Only include files with names
                    files.append({
                        "name": name,
                        "size": size,
                        "id": file_id,  # Add the file_id to the dictionary
                        "is_public": is_public,  # Add is_public status
                        "file_hash": file_hash  # Add file_hash
                    })
            except Exception as e:
                print(f"Error processing file data: {e}")
                continue
                
        return jsonify({
            "files": files,
            "debug": {
                "user_database_id": user_database_id,
                "results_count": len(files)
            }
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/delete_file', methods=['POST'])
@login_required
def delete_file():
    """
    Delete a file from user's database and global index
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({'error': 'No data provided'}), 400
        
        file_id = data.get('file_id')
        file_hash = data.get('file_hash')
        
        if not file_id:
            return jsonify({'error': 'file_id is required'}), 400
        
        print(f"Deleting file: ID={file_id}, Hash={file_hash}")
        
        # Delete from user database and global index
        result = uploader.delete_file_from_db(file_id)
        
        return jsonify({
            'status': 'success',
            'message': 'File deleted successfully'
        })
        
    except Exception as e:
        print(f"Error deleting file: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/toggle_public_access', methods=['POST'])
@login_required
def toggle_public_access():
    """
    Toggle public access status for a file - matches old code implementation
    """
    print("DEBUG: /toggle_public_access route accessed.")
    try:
        data = request.get_json()
        print(f"DEBUG: Received JSON data: {data}")
        
        if not data:
            return jsonify({'error': 'No data provided'}), 400
        
        file_id = data.get('file_id')
        is_public = data.get('is_public')
        salted_sha512_hash = data.get('salted_sha512_hash')  # Get the hash to update index
        
        if not file_id or is_public is None or not salted_sha512_hash:
            print("DEBUG: Missing file_id, is_public, or salted_sha512_hash from request.")
            return jsonify({"error": "File ID, public status, and hash are required"}), 400
        
        print(f"DEBUG: File with ID {file_id} public status set to {is_public}.")
        
        # Call the function in notion_uploader.py to update the public status
        uploader.update_file_public_status(file_id, is_public, salted_sha512_hash)
        print(f"DEBUG: File with ID {file_id} public status set to {is_public}.")
        
        return jsonify({"status": "success", "message": "File public status updated successfully"}), 200
        
    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        print(f"DEBUG: Toggling public access failed with error: {str(e)}\n{error_trace}")
        return jsonify({"error": str(e)}), 500


# ============================================================================
# END FILE DOWNLOAD ROUTES
# ============================================================================

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
        content_type = data.get('contentType', 'text/plain')  # Default to text/plain for Notion compatibility
        
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
    Handle streaming file upload with enhanced error handling and parallel processing
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
        
        print("DEBUG: Processing upload stream with parallel processing and database integration")
        
        # Process upload with enhanced error handling
        try:
            result = streaming_upload_manager.process_upload_stream(upload_id, stream_generator())
            
            print(f"DEBUG: Upload processing completed successfully: {result}")
            
            # **ISSUE 2 FIX**: Add database integration after successful Notion upload
            try:
                print("DEBUG: Starting database integration after successful upload")
                
                # Get user database ID
                user_database_id = uploader.get_user_database_id(current_user.id)
                if not user_database_id:
                    print("ERROR: User database not found for database integration")
                    raise Exception("User database not found")
                
                print(f"DEBUG: User database ID: {user_database_id}")
                
                # Save file metadata to user database
                file_page_result = uploader.add_file_to_user_database(
                    user_database_id,
                    result['filename'],
                    result['bytes_uploaded'],
                    result['file_hash'],
                    result['file_id'],
                    is_public=False,  # Default to private
                    salt="",  # Add salt if needed
                    original_filename=result.get('original_filename', result['filename'])
                )
                
                print(f"DEBUG: File added to user database: {file_page_result['id']}")
                
                # Add to global index
                uploader.add_file_to_index(
                    result['file_hash'],
                    file_page_result['id'],
                    user_database_id,
                    result.get('original_filename', result['filename']),
                    False  # is_public = False by default
                )
                
                print("DEBUG: File added to global index successfully")
                
            except Exception as db_error:
                print(f"ERROR: Database integration failed: {db_error}")
                # Don't fail the entire upload, but log the error
                # The file is already uploaded to Notion successfully
                import traceback
                traceback.print_exc()
            
            # Emit final progress update
            if socketio:
                socketio.emit('upload_progress', {
                    'upload_id': upload_id,
                    'status': 'completed',
                    'progress': 100,
                    'bytes_uploaded': result['bytes_uploaded'],
                    'total_size': result['bytes_uploaded']
                })
            
            # **ISSUE 3 FIX**: Enhanced response to include all data needed for UI buttons
            return jsonify({
                'status': 'completed',
                'upload_id': upload_id,
                'filename': result['filename'],
                'file_size': result['bytes_uploaded'],
                'file_hash': result['file_hash'],
                'file_id': file_page_result['id'] if 'file_page_result' in locals() else result['file_id'],
                'is_public': False,  # Default to private
                'name': result.get('original_filename', result['filename']),  # For UI display
                'size': result['bytes_uploaded']  # For UI consistency
            })
            
        except MemoryError as e:
            print(f"Memory limit exceeded during upload: {e}")
            return jsonify({'error': f'Memory limit exceeded: {str(e)}'}), 507
        except Exception as e:
            print(f"Upload processing error: {e}")
            import traceback
            traceback.print_exc()
            return jsonify({'error': f'Upload failed: {str(e)}'}), 500
        
    except Exception as e:
        print(f"Error in streaming upload endpoint: {e}")
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
