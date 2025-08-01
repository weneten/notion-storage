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
import json
from flask_socketio import emit

# Function to clean up old upload sessions periodically
def cleanup_old_sessions():
    """Clean up old upload sessions every minute"""
    try:
        current_time = time.time()
        
        if hasattr(app, 'upload_processors'):
            # Clean up old upload sessions (older than 30 minutes)
            expired_sessions = []
            for upload_id, upload_data in app.upload_processors.items():
                last_activity = upload_data.get('last_activity', 0)
                if current_time - last_activity > 1800:  # 30 minutes
                    expired_sessions.append(upload_id)
            
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
        
        active_sessions = len(app.upload_processors) if hasattr(app, 'upload_processors') else 0
        print(f"SESSION_CLEANUP: Active sessions: {active_sessions}")
        
    except Exception as e:
        print(f"Error in session cleanup: {e}")
        
    # Schedule next check
    threading.Timer(60, cleanup_old_sessions).start()
    
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


# Initialize global upload state containers with thread synchronization
app.upload_locks = {}
app.upload_processors = {}

# Add global metadata store
app.upload_metadata = {}

# CRITICAL FIX 3: Thread Synchronization - Global locks for upload session management
app.upload_session_lock = threading.Lock()  # Master lock for upload session operations
app.id_validation_lock = threading.Lock()   # Lock for ID validation operations

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
NOTION_SPACE_ID = os.environ.get('NOTION_SPACE_ID')  # Add space ID configuration

uploader = NotionFileUploader(
    api_token=NOTION_API_TOKEN,
    socketio=socketio,
    global_file_index_db_id=GLOBAL_FILE_INDEX_DB_ID,
    notion_space_id=NOTION_SPACE_ID
)

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
        current_folder = request.args.get('folder', '/')
        entries = []
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
                    # Only use file_data for file storage
                    file_data_files = properties.get('file_data', {}).get('files', [])
                    folder_path = properties.get('folder_path', {}).get('rich_text', [{}])[0].get('text', {}).get('content', '/')
                    is_folder = properties.get('is_folder', {}).get('checkbox', False)
                    is_visible = properties.get('is_visible', {}).get('checkbox', True)
                    if name and is_visible and folder_path == current_folder:
                        if is_folder:
                            full_path = folder_path.rstrip('/') + '/' + name if folder_path != '/' else '/' + name
                            entries.append({
                                "type": "folder",
                                "name": name,
                                "id": file_id,
                                "full_path": full_path
                            })
                        else:
                            entries.append({
                                "type": "file",
                                "name": name,
                                "size": size,
                                "id": file_id,
                                "is_public": is_public,
                                "file_hash": file_hash,
                                "salted_hash": "",
                                "file_data": file_data_files,
                                "folder": folder_path
                            })
                except Exception as e:
                    print(f"Error processing file data in home route: {e}")
                    continue
        return render_template('home.html', entries=entries, current_folder=current_folder)
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
                # Explicitly set folder=/ so the URL shows the root folder
                return redirect(url_for('home', folder='/'))
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
        manifest_page_id = None
        for file_data in files_data.get('results', []):
            properties = file_data.get('properties', {})
            name = properties.get('filename', {}).get('title', [{}])[0].get('text', {}).get('content', '')
            if name == filename:
                file_hash = properties.get('filehash', {}).get('rich_text', [{}])[0].get('text', {}).get('content', '')
                manifest_page_id = file_data.get('id')
                break

        if filename.lower().endswith('.json') and manifest_page_id:
            return redirect(url_for('download_multipart_by_page_id', manifest_page_id=manifest_page_id))
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

        # Check access control
        if not is_public:
            if not current_user.is_authenticated:
                return redirect(url_for('login', next=request.url))

            current_user_db_id = uploader.get_user_database_id(current_user.id)

            if file_user_db_id != current_user_db_id:
                return "Access Denied: You do not have permission to download this file.", 403

        # Check if this is a manifest JSON (multi-part file)
        is_manifest = original_filename.lower().endswith('.json')
        if is_manifest:
            # Stream all parts as a single file
            try:
                # Download manifest JSON to get original filename and total size
                import requests, json
                manifest_page = uploader.get_user_by_id(file_page_id)
                file_property = manifest_page.get('properties', {}).get('file_data', {})
                files_array = file_property.get('files', [])
                manifest_file = files_array[0] if files_array else None

                # Use NotionFileUploader's method to get a fresh signed URL for the manifest JSON
                manifest_filename = manifest_file.get('name', 'file.txt') if manifest_file else 'file.txt'
                manifest_metadata = uploader.get_file_download_metadata(file_page_id, manifest_filename)
                manifest_url = manifest_metadata.get('url', '')
                if not manifest_url:
                    return "Manifest file not found", 404

                resp = requests.get(manifest_url)
                resp.raise_for_status()
                manifest = resp.json() if resp.headers.get('content-type','').startswith('application/json') else json.loads(resp.content)
                orig_name = manifest.get('original_filename', 'download')
                total_size = manifest.get('total_size', 0)
                # Use video mimetype if possible, fallback to octet-stream
                import mimetypes
                mimetype = mimetypes.guess_type(orig_name)[0] or 'application/octet-stream'
                response = Response(stream_with_context(uploader.stream_multi_part_file(file_page_id)), mimetype=mimetype)
                response.headers['Content-Disposition'] = f'attachment; filename="{orig_name}"'
                if total_size > 0:
                    response.headers['Content-Length'] = str(total_size)
                return response
            except Exception as e:
                import traceback
                error_trace = traceback.format_exc()
                print(f"DEBUG: Error streaming multi-part file: {str(e)}\n{error_trace}")
                return f"Error streaming multi-part file: {str(e)}", 500
        # Otherwise, normal single file download
        file_metadata = uploader.get_file_download_metadata(file_page_id, original_filename)
        if not file_metadata['url']:
            return "Download link not available for this file", 500
        mimetype = file_metadata['content_type']
        if mimetype == 'application/octet-stream':
            import mimetypes
            detected_type, _ = mimetypes.guess_type(original_filename)
            if detected_type:
                mimetype = detected_type
        response = Response(stream_with_context(uploader.stream_file_from_notion(file_page_id, original_filename)), mimetype=mimetype)
        response.headers['Content-Disposition'] = f'attachment; filename="{original_filename}"'
        if file_metadata['file_size'] > 0:
            response.headers['Content-Length'] = str(file_metadata['file_size'])
            # Avoid printing Unicode emoji to stdout to prevent UnicodeEncodeError in some environments
            print(f"Download response includes Content-Length: {file_metadata['file_size']} bytes for {original_filename}")
        else:
            print(f"No file size available for Content-Length header for {original_filename}")
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

        # Get comprehensive file metadata including URL, size, and content type
        file_metadata = uploader.get_file_download_metadata(file_page_id, original_filename)
        
        if not file_metadata['url']:
            return "Stream link not available for this file", 500
            
        file_size = file_metadata['file_size']
        notion_download_link = file_metadata['url']
        detected_content_type = file_metadata['content_type']
            
        print(f"📊 Streaming file: {original_filename}, size: {file_size} bytes, type: {detected_content_type}")

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

        # Use detected content type from metadata, with enhanced fallback
        mimetype = detected_content_type
        if mimetype == 'application/octet-stream':
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
                
                # Validate range - handle cases where file_size might be 0
                if file_size > 0:
                    if start < 0 or end >= file_size or start > end:
                        response = Response(status=416)
                        response.headers['Content-Range'] = f'bytes */{file_size}'
                        return response
                else:
                    # If file size is unknown, we can't validate the range properly
                    # But we can still try to serve the requested range
                    print(f"⚠️ File size unknown, attempting to serve range {start}-{end} anyway")
                
                # Stream the requested range
                def stream_range():
                    for chunk in uploader.stream_file_from_notion_range(notion_download_link, start, end):
                        yield chunk
                
                # Create partial content response
                response = Response(stream_with_context(stream_range()), mimetype=mimetype, status=206)
                response.headers['Content-Disposition'] = f'inline; filename="{original_filename}"'
                response.headers['Content-Length'] = str(end - start + 1)
                
                # Include total file size in Content-Range if known
                if file_size > 0:
                    response.headers['Content-Range'] = f'bytes {start}-{end}/{file_size}'
                    print(f"📊 Range response: bytes {start}-{end}/{file_size}")
                else:
                    response.headers['Content-Range'] = f'bytes {start}-{end}/*'
                    print(f"📊 Range response: bytes {start}-{end}/* (size unknown)")
                    
                response.headers['Accept-Ranges'] = 'bytes'
                
                # iOS Safari optimized headers for partial content
                response.headers['Cache-Control'] = 'public, max-age=3600, must-revalidate'
                response.headers['X-Content-Type-Options'] = 'nosniff'
                response.headers['Vary'] = 'Range, Accept-Encoding'
                
                # Additional iOS streaming optimizations for partial content
                if mimetype.startswith('video/'):
                    response.headers['Connection'] = 'keep-alive'
                    response.headers['Content-Transfer-Encoding'] = 'binary'
                
                return response
                
            except (ValueError, TypeError) as e:
                return "Invalid range values", 416
        
        else:
            # Full content request
            response = Response(stream_with_context(uploader.stream_file_from_notion(file_page_id, original_filename)), mimetype=mimetype)
            response.headers['Content-Disposition'] = f'inline; filename="{original_filename}"'
            
            # Add Content-Length header if file size is available
            if file_size > 0:
                response.headers['Content-Length'] = str(file_size)
                print(f"📊 Full content response includes Content-Length: {file_size} bytes for {original_filename}")
            else:
                print(f"⚠️ No file size available for Content-Length header for {original_filename}")
                
            response.headers['Accept-Ranges'] = 'bytes'
            
            # iOS Safari optimized headers
            response.headers['Cache-Control'] = 'public, max-age=3600, must-revalidate'
            response.headers['X-Content-Type-Options'] = 'nosniff'
            response.headers['Vary'] = 'Range, Accept-Encoding'
            
            # Additional iOS streaming optimizations
            if mimetype.startswith('video/'):
                response.headers['Connection'] = 'keep-alive'
                response.headers['Content-Transfer-Encoding'] = 'binary'
            
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
            is_manifest = file_props.get('is_manifest', {}).get('checkbox', False)
            is_visible = file_props.get('is_visible', {}).get('checkbox', True)
            salt = file_props.get('salt', {}).get('rich_text', [{}])[0].get('text', {}).get('content', '')

            # Compute salted hash for download link if salt is present
            salted_hash = file_hash
            if salt and file_hash:
                import hashlib
                salted_hash = hashlib.sha512((file_hash + salt).encode('utf-8')).hexdigest()

            print(f"🔍 DIAGNOSTIC: File {i+1} - {name}:")
            print(f"  - ID: {file_id} (needed for delete button)")
            print(f"  - Hash: {file_hash} (needed for toggle)")
            print(f"  - Salt: {salt}")
            print(f"  - Salted Hash: {salted_hash}")
            print(f"  - Is Public: {is_public} (needed for toggle state)")
            print(f"  - Size: {size}")
            print(f"  - Has all button data: {bool(file_id and file_hash is not None and is_public is not None)}")
            
            if is_visible:
                formatted_file = {
                    'id': file_id,
                    'name': name,
                    'size': size,
                    'file_hash': file_hash,
                    'salted_hash': salted_hash,
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
        
        # Use unified deletion logic from StreamingUploadManager
        user_database_id = uploader.get_user_database_id(current_user.id)
        if not user_database_id:
            return jsonify({'error': 'User database not found'}), 404
        streaming_upload_manager.uploader.delete_file_entry(file_id, user_database_id)
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


@app.route('/update_file_metadata', methods=['POST'])
@login_required
def update_file_metadata():
    try:
        data = request.get_json()
        file_id = data.get('file_id')
        new_name = data.get('filename')
        new_folder = data.get('folder_path')

        if not file_id:
            return jsonify({'error': 'file_id required'}), 400

        uploader.update_file_metadata(file_id, filename=new_name, folder_path=new_folder)
        return jsonify({'status': 'success'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/create_folder', methods=['POST'])
@login_required
def create_folder():
    try:
        data = request.get_json()
        folder_name = data.get('folder_name')
        parent_path = data.get('parent_path', '/')

        if not folder_name:
            return jsonify({'error': 'folder_name required'}), 400

        user_database_id = uploader.get_user_database_id(current_user.id)
        if not user_database_id:
            return jsonify({'error': 'User database not found'}), 404

        # Ensure the 'is_folder' property exists in the user's database
        uploader.ensure_database_property(
            user_database_id,
            'is_folder',
            'checkbox'
        )

        uploader.create_folder(user_database_id, folder_name, parent_path)
        return jsonify({'status': 'success'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/delete_folder', methods=['POST'])
@login_required
def delete_folder():
    try:
        data = request.get_json()
        folder_id = data.get('folder_id')
        delete_contents = data.get('delete_contents', False)

        if not folder_id:
            return jsonify({'error': 'folder_id required'}), 400

        user_database_id = uploader.get_user_database_id(current_user.id)
        if not user_database_id:
            return jsonify({'error': 'User database not found'}), 404

        folder_entry = uploader.get_user_by_id(folder_id)
        if not folder_entry:
            return jsonify({'error': 'Folder not found'}), 404

        props = folder_entry.get('properties', {})
        folder_name = props.get('filename', {}).get('title', [{}])[0].get('text', {}).get('content', '')
        parent_path = props.get('folder_path', {}).get('rich_text', [{}])[0].get('text', {}).get('content', '/')

        folder_path = parent_path.rstrip('/') + '/' + folder_name if parent_path != '/' else '/' + folder_name

        all_entries = uploader.get_files_from_user_database(user_database_id)
        to_delete = []
        for entry in all_entries.get('results', []):
            e_props = entry.get('properties', {})
            path = e_props.get('folder_path', {}).get('rich_text', [{}])[0].get('text', {}).get('content', '/')
            if path == folder_path or path.startswith(folder_path.rstrip('/') + '/'):
                to_delete.append(entry)

        if to_delete and not delete_contents:
            return jsonify({'needs_confirm': True, 'count': len(to_delete)})

        # sort deepest first
        to_delete.sort(key=lambda e: e.get('properties', {}).get('folder_path', {}).get('rich_text', [{}])[0].get('text', {}).get('content', '/').count('/'), reverse=True)

        for entry in to_delete:
            e_id = entry.get('id')
            is_folder = entry.get('properties', {}).get('is_folder', {}).get('checkbox', False)
            if is_folder:
                uploader.delete_file_from_user_database(e_id)
            else:
                streaming_upload_manager.uploader.delete_file_entry(e_id, user_database_id)

        uploader.delete_file_from_user_database(folder_id)
        return jsonify({'status': 'success'})
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


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
        folder_path = data.get('folderPath', '/')
        
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
            progress_callback=None,  # Will use SocketIO for progress updates
            folder_path=folder_path
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
    Handle streaming file upload with enhanced resilience, resource management, and circuit breaker protection
    """
    # Import circuit breaker for reliability
    try:
        from uploader.circuit_breaker import upload_circuit_breaker, CircuitBreakerOpenError
    except ImportError:
        upload_circuit_breaker = None
    
    try:
        # CRITICAL FIX 3: Thread Synchronization - Protect upload session access
        with app.upload_session_lock:
            print(f"🚀 Streaming upload initiated for upload_id: {upload_id}")
            print(f"🔒 THREAD SAFETY: Acquired upload session lock for {upload_id}")
            
            # Get upload session with thread safety
            upload_session = streaming_upload_manager.get_upload_status(upload_id)
            if not upload_session:
                print(f"❌ Upload session {upload_id} not found")
                return jsonify({'error': 'Upload session not found'}), 404
            
            # Mark session as being processed to prevent concurrent access
            upload_session['processing_thread'] = threading.current_thread().ident
            upload_session['last_activity'] = time.time()
            print(f"🔒 THREAD SAFETY: Session {upload_id} locked to thread {threading.current_thread().ident}")
        
        print(f"📁 Processing upload: {upload_session['filename']} ({upload_session['file_size'] / 1024 / 1024:.1f}MB)")
        
        # Verify file size matches headers
        content_length = request.headers.get('Content-Length')
        expected_size = upload_session['file_size']
        
        if content_length and int(content_length) != expected_size:
            print(f"❌ Content-Length mismatch: {content_length} vs {expected_size}")
            return jsonify({'error': 'Content-Length mismatch'}), 400
        
        try:
            # Create a resource-aware stream generator
            def stream_generator():
                try:
                    print("📡 Starting stream reading...")
                    chunk_size = 64 * 1024  # 64KB chunks for memory efficiency
                    total_read = 0
                    last_log_mb = 0
                    
                    while True:
                        chunk = request.stream.read(chunk_size)
                        if not chunk:
                            print(f"📡 Stream completed, total read: {total_read / 1024 / 1024:.1f}MB")
                            break
                        
                        total_read += len(chunk)
                        
                        # Log progress every 10MB
                        current_mb = total_read // (10 * 1024 * 1024)
                        if current_mb > last_log_mb:
                            print(f"📡 Read {total_read / (1024*1024):.1f}MB...")
                            last_log_mb = current_mb
                        
                        yield chunk
                        
                except Exception as e:
                    print(f"❌ Error reading request stream: {e}")
                    raise
            
            print("🔄 Processing upload...")
            
            # Process upload with circuit breaker protection
            def upload_with_circuit_breaker():
                return streaming_upload_manager.process_upload_stream(upload_id, stream_generator())
            
            if upload_circuit_breaker:
                try:
                    result = upload_circuit_breaker.call(upload_with_circuit_breaker)
                except CircuitBreakerOpenError as e:
                    print(f"🚨 Circuit breaker open: {e}")
                    return jsonify({'error': 'Upload service temporarily unavailable. Please try again later.'}), 503
            else:
                result = upload_with_circuit_breaker()
            
            print(f"✅ Upload processing completed: {result}")
            
            # LEGACY CODE REMOVAL: Removed problematic add_file_to_user_database call
            # This was causing file upload ID mismatch errors after upload completion
            # The file is already successfully uploaded to Notion at this point
            print("💾 Upload completed successfully - legacy database integration step removed")
            print(f"🔍 Upload result: {result.get('filename')} ({result.get('bytes_uploaded', 0)} bytes)")
            
            # Final progress update
            if socketio:
                socketio.emit('upload_progress', {
                    'upload_id': upload_id,
                    'status': 'completed',
                    'progress': 100,
                    'bytes_uploaded': result['bytes_uploaded'],
                    'total_size': result['bytes_uploaded']
                })
            
            return jsonify({
                'status': 'completed',
                'upload_id': upload_id,
                'filename': result['filename'],
                'file_size': result['bytes_uploaded'],
                'file_hash': result['file_hash'],
                'file_id': result.get('file_id'),  # Return the file ID from upload result
                'is_public': False,
                'name': result.get('original_filename', result['filename']),
                'size': result['bytes_uploaded']
            })
            
        except MemoryError as e:
            print(f"💥 Memory limit exceeded: {e}")
            return jsonify({'error': f'Memory limit exceeded: {str(e)}'}), 507
        except Exception as e:
            print(f"💥 Upload processing error: {e}")
            import traceback
            traceback.print_exc()
            return jsonify({'error': f'Upload failed: {str(e)}'}), 500
        
    except Exception as e:
        print(f"💥 Critical error in streaming upload endpoint: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/api/upload/resume/<upload_id>', methods=['POST'])
@login_required
def resume_stream_file_upload(upload_id):
    """Resume a previously started upload"""
    try:
        with app.upload_session_lock:
            upload_session = streaming_upload_manager.get_upload_status(upload_id)
            if not upload_session:
                return jsonify({'error': 'Upload session not found'}), 404
            upload_session['processing_thread'] = threading.current_thread().ident
            upload_session['last_activity'] = time.time()

        def stream_generator():
            chunk_size = 64 * 1024
            while True:
                chunk = request.stream.read(chunk_size)
                if not chunk:
                    break
                yield chunk

        result = streaming_upload_manager.resume_upload_stream(upload_id, stream_generator())

        if socketio:
            socketio.emit('upload_progress', {
                'upload_id': upload_id,
                'status': 'completed',
                'progress': 100,
                'bytes_uploaded': result['bytes_uploaded'],
                'total_size': result['bytes_uploaded']
            })

        return jsonify({
            'status': 'completed',
            'upload_id': upload_id,
            'filename': result['filename'],
            'file_size': result['bytes_uploaded'],
            'file_hash': result['file_hash'],
            'file_id': result.get('file_id'),
            'is_public': False,
            'name': result.get('original_filename', result['filename']),
            'size': result['bytes_uploaded']
        })

    except Exception as e:
        print(f"Resume upload error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/upload/status/<upload_id>', methods=['GET'])
@login_required
def get_upload_status(upload_id):
    """
    Get the status of an upload session with enhanced monitoring
    """
    try:
        upload_session = streaming_upload_manager.get_upload_status(upload_id)
        if not upload_session:
            return jsonify({'error': 'Upload session not found'}), 404
        
        # Import circuit breaker for status monitoring
        try:
            from uploader.circuit_breaker import upload_circuit_breaker, notion_api_circuit_breaker
        except ImportError:
            upload_circuit_breaker = None
            notion_api_circuit_breaker = None
        
        # Basic status response
        status_response = {
            'upload_id': upload_id,
            'status': upload_session['status'],
            'filename': upload_session['filename'],
            'file_size': upload_session['file_size'],
            'bytes_uploaded': upload_session['bytes_uploaded'],
            'is_multipart': upload_session['is_multipart'],
            'created_at': upload_session['created_at']
        }
        
        # Add circuit breaker status if available
        if upload_circuit_breaker:
            cb_stats = upload_circuit_breaker.get_stats()
            status_response['circuit_breaker'] = {
                'state': cb_stats['state'],
                'success_rate': cb_stats['success_rate_percent']
            }
        
        return jsonify(status_response)
        
    except Exception as e:
        print(f"Error getting upload status: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/system/health', methods=['GET'])
@login_required
def get_system_health():
    """
    Get comprehensive system health and resilience status
    """
    try:
        # Import monitoring components
        try:
            from uploader.circuit_breaker import upload_circuit_breaker, notion_api_circuit_breaker, log_all_circuit_breaker_stats
            from uploader.checkpoint_manager import checkpoint_manager, log_checkpoint_stats
        except ImportError as e:
            return jsonify({'error': f'Monitoring components not available: {e}'}), 500
        
        health_status = {
            'timestamp': time.time(),
            'status': 'healthy',
            'components': {}
        }
        
        # Circuit breaker health
        circuit_breakers = []
        if upload_circuit_breaker:
            cb_stats = upload_circuit_breaker.get_stats()
            circuit_breakers.append({
                'name': cb_stats['name'],
                'state': cb_stats['state'],
                'success_rate': cb_stats['success_rate_percent'],
                'total_calls': cb_stats['total_calls'],
                'failure_count': cb_stats['failure_count']
            })
        
        if notion_api_circuit_breaker:
            cb_stats = notion_api_circuit_breaker.get_stats()
            circuit_breakers.append({
                'name': cb_stats['name'],
                'state': cb_stats['state'],
                'success_rate': cb_stats['success_rate_percent'],
                'total_calls': cb_stats['total_calls'],
                'failure_count': cb_stats['failure_count']
            })
        
        health_status['components']['circuit_breakers'] = {
            'status': 'healthy' if all(cb['state'] != 'OPEN' for cb in circuit_breakers) else 'critical',
            'breakers': circuit_breakers
        }
        
        # Checkpoint manager health
        if checkpoint_manager:
            checkpoint_stats = checkpoint_manager.get_checkpoint_stats()
            health_status['components']['checkpoint_manager'] = {
                'status': 'healthy' if checkpoint_stats['storage_available'] else 'warning',
                'storage_type': checkpoint_stats['storage_type'],
                'checkpoint_interval': checkpoint_stats['checkpoint_interval']
            }
        
        # Overall system status
        component_statuses = [comp.get('status', 'unknown') for comp in health_status['components'].values()]
        if 'critical' in component_statuses:
            health_status['status'] = 'critical'
        elif 'warning' in component_statuses:
            health_status['status'] = 'warning'
        
        # Performance metrics
        health_status['performance'] = {
            'active_upload_sessions': len(streaming_upload_manager.active_uploads) if hasattr(streaming_upload_manager, 'active_uploads') else 0
        }
        
        return jsonify(health_status)
        
    except Exception as e:
        print(f"Error getting system health: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e), 'status': 'error'}), 500


@app.route('/api/system/cache', methods=['GET', 'POST', 'DELETE'])
@login_required
def manage_url_cache():
    """
    Manage the download URL cache for performance optimization
    """
    try:
        if request.method == 'GET':
            # Get cache statistics
            cache_stats = uploader.get_cache_stats()
            return jsonify({
                'status': 'success',
                'cache_stats': cache_stats,
                'message': f"Cache contains {cache_stats['total_entries']} entries, {cache_stats['valid_entries']} valid"
            })
        
        elif request.method == 'POST':
            # Cleanup expired entries
            uploader.cleanup_expired_cache_entries()
            cache_stats = uploader.get_cache_stats()
            return jsonify({
                'status': 'success',
                'cache_stats': cache_stats,
                'message': 'Expired cache entries cleaned up'
            })
        
        elif request.method == 'DELETE':
            # Clear all cache entries
            uploader.clear_url_cache()
            return jsonify({
                'status': 'success',
                'message': 'All cache entries cleared'
            })
    
    except Exception as e:
        print(f"Error managing URL cache: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/system/migrate-permanent-urls', methods=['POST'])
@login_required
def migrate_to_permanent_urls():
    """
    Migrate existing files to use permanent URLs with enhanced error handling and logging
    """
    try:
        print(f"🔄 Starting permanent URL migration for user: {current_user.id}")
        
        # Validate that NOTION_SPACE_ID is configured
        if not NOTION_SPACE_ID:
            error_msg = "NOTION_SPACE_ID is not configured. Cannot generate permanent URLs."
            print(f"❌ Migration failed: {error_msg}")
            return jsonify({
                'status': 'error',
                'error': error_msg,
                'code': 'MISSING_SPACE_ID'
            }), 400
        
        # Get user's database ID
        user_database_id = uploader.get_user_database_id(current_user.id)
        if not user_database_id:
            error_msg = f"User database not found for user: {current_user.id}"
            print(f"❌ Migration failed: {error_msg}")
            return jsonify({
                'status': 'error',
                'error': 'User database not found',
                'code': 'USER_DATABASE_NOT_FOUND'
            }), 404
        
        print(f"📁 Found user database: {user_database_id}")
        
        # Perform migration with detailed logging
        migration_result = uploader.migrate_to_permanent_urls(user_database_id)
        
        # Check migration status
        if migration_result.get('status') == 'failed':
            error_msg = migration_result.get('error', 'Unknown migration error')
            print(f"❌ Migration failed: {error_msg}")
            return jsonify({
                'status': 'error',
                'error': error_msg,
                'code': 'MIGRATION_FAILED'
            }), 500
        
        # Success response with detailed information
        migrated_count = migration_result.get('migrated', 0)
        skipped_count = migration_result.get('skipped', 0)
        error_count = migration_result.get('errors', 0)
        total_files = migration_result.get('total_files', 0)
        
        print(f"✅ Migration completed: {migrated_count} migrated, {skipped_count} skipped, {error_count} errors")
        
        return jsonify({
            'status': 'success',
            'migration_result': {
                'total_files': total_files,
                'migrated': migrated_count,
                'skipped': skipped_count,
                'errors': error_count,
                'database_id': user_database_id,
                'space_id': NOTION_SPACE_ID
            },
            'message': f"Migration completed successfully: {migrated_count} files migrated, {skipped_count} skipped, {error_count} errors",
            'summary': {
                'success_rate': f"{((migrated_count + skipped_count) / total_files * 100):.1f}%" if total_files > 0 else "N/A",
                'permanent_urls_enabled': True
            }
        })
        
    except Exception as e:
        error_msg = str(e)
        print(f"💥 Critical error during migration: {error_msg}")
        import traceback
        traceback.print_exc()
        
        return jsonify({
            'status': 'error',
            'error': error_msg,
            'code': 'INTERNAL_ERROR',
            'message': 'An internal error occurred during migration. Please check the logs and try again.'
        }), 500


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


@app.route('/download-multipart/<manifest_page_id>')
def download_multipart_by_page_id(manifest_page_id):
    """
    Download a multi-part file using the manifest's Notion page ID (bypasses global file index).
    """
    try:
        import requests, json
        manifest_page = uploader.get_user_by_id(manifest_page_id)
        if not manifest_page:
            return "Manifest page not found", 404
        file_property = manifest_page.get('properties', {}).get('file_data', {})
        files_array = file_property.get('files', [])
        manifest_file = files_array[0] if files_array else None

        # Use NotionFileUploader's method to get a fresh signed URL for the manifest JSON
        manifest_filename = manifest_file.get('name', 'file.txt') if manifest_file else 'file.txt'
        manifest_metadata = uploader.get_file_download_metadata(manifest_page_id, manifest_filename)
        manifest_url = manifest_metadata.get('url', '')
        if not manifest_url:
            return "Manifest file not found", 404

        resp = requests.get(manifest_url)
        resp.raise_for_status()
        manifest = resp.json() if resp.headers.get('content-type','').startswith('application/json') else json.loads(resp.content)
        orig_name = manifest.get('original_filename', 'download')
        total_size = manifest.get('total_size', 0)
        import mimetypes
        mimetype = mimetypes.guess_type(orig_name)[0] or 'application/octet-stream'
        response = Response(stream_with_context(uploader.stream_multi_part_file(manifest_page_id)), mimetype=mimetype)
        response.headers['Content-Disposition'] = f'attachment; filename="{orig_name}"'
        if total_size > 0:
            response.headers['Content-Length'] = str(total_size)
        return response
    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        print(f"DEBUG: Error streaming multi-part file: {str(e)}\n{error_trace}")
        return f"Error streaming multi-part file: {str(e)}", 500

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

# Start the cleanup task
cleanup_old_sessions()

# ============================================================================
# END OF STREAMING UPLOAD API
# ============================================================================
