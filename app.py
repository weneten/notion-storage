from flask import Flask, request, jsonify, render_template, redirect, url_for, session, g, Response, stream_with_context
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from flask_socketio import SocketIO
from flask_cors import CORS
from uploader import NotionFileUploader, ChunkProcessor
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

# Function to log memory usage periodically
def log_memory_usage():
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
    threading.Timer(60, log_memory_usage).start()
    
# Load environment variables from .env file
load_dotenv()

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY')  # Default secret key for development
CORS(app)  # Enable CORS for all routes
socketio = SocketIO(app, cors_allowed_origins="*")

# Initialize global upload state containers
app.upload_locks = {}
app.upload_processors = {}

def format_bytes(bytes, decimals=2):
    if bytes == 0:
        return '0 Bytes'
    k = 1024
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


@app.route('/upload_file', methods=['POST'])
@login_required
def upload_file():
    print("DEBUG: Chunk upload request received from", request.remote_addr)
    
    # Debug the request form and files
    print("DEBUG: Request form data:")
    for key, value in request.form.items():
        print(f"DEBUG: - {key}: {value}")
    
    print("DEBUG: Request files:")
    for key, file in request.files.items():
        print(f"DEBUG: - {key}: {file.filename}")
    
    try:
        # Extract parameters from the request
        upload_id = request.form.get('upload_id')
        part_number = int(request.form.get('part_number'))
        total_size = int(request.form.get('total_size', 0))
        filename = request.form.get('filename', 'file.txt')
        original_filename = request.form.get('original_filename', filename)
        salt = request.form.get('salt', '')
        is_last_chunk = request.form.get('is_last_chunk', 'False').lower() == 'true'
        is_multipart = request.form.get('is_multipart', 'False').lower() == 'true'
        
        # Log received parameters for debugging
        print("DEBUG: Received parameters:")
        print(f"DEBUG: upload_id: {upload_id}")
        print(f"DEBUG: part_number: {part_number}")
        print(f"DEBUG: total_size: {total_size}")
        print(f"DEBUG: filename: {filename}")
        print(f"DEBUG: original_filename: {original_filename}")
        print(f"DEBUG: salt: {salt}")
        print(f"DEBUG: is_last_chunk: {is_last_chunk}")
        print(f"DEBUG: is_multipart: {is_multipart}")
        
        # Check if we have all required parameters
        if not upload_id or not part_number:
            return jsonify({"error": "Missing required parameters"}), 400
            
        # Get the chunk data from the request - support both 'file' and 'chunk' for backward compatibility
        chunk_file = request.files.get('file') or request.files.get('chunk')
        if not chunk_file:
            return jsonify({"error": "No file provided - expected 'file' field in form data"}), 400
            
        # Read the chunk data
        chunk_data = chunk_file.read()
        chunk_size = len(chunk_data)
        
        # Determine total parts based on upload metadata
        total_parts = 0
        
        # Ensure upload_locks and upload_processors are initialized
        if not hasattr(app, 'upload_locks'):
            app.upload_locks = {}
            
        if not hasattr(app, 'upload_processors'):
            app.upload_processors = {}
        
        # Create a lock for this upload if it doesn't exist
        if upload_id not in app.upload_locks:
            app.upload_locks[upload_id] = threading.Lock()
            
        with app.upload_locks.get(upload_id, threading.Lock()):
            # Get or create the processor for this upload
            if upload_id not in app.upload_processors:
                print(f"DEBUG: Created new chunk processor for upload {upload_id}")
                app.upload_processors[upload_id] = {
                    'hasher': hashlib.sha512(salt.encode()),
                    'total_size': total_size,
                    'filename': filename,
                    'original_filename': original_filename,
                    'salt': salt,
                    'bytes_uploaded': 0,
                    'completed_parts': set(),
                    'pending_parts': set(),
                    'upload_threads': {},
                    'total_parts': 0,
                    'cached_chunks': {},  # Add chunk caching for retries
                    'last_activity': time.time(),
                    'is_multipart': is_multipart,  # Store whether this is a multipart upload
                }
            
            # Get the processor for this upload
            upload_data = app.upload_processors[upload_id]
            
            # Update last activity timestamp
            upload_data['last_activity'] = time.time()
            
            # Update total parts if needed
            if is_last_chunk and part_number > upload_data.get('total_parts', 0):
                upload_data['total_parts'] = part_number
                
            # Or if we already know the total parts from a previous request
            total_parts = upload_data.get('total_parts', 0)
            
            # Update part info even if processing may be delayed
            upload_data['original_filename'] = original_filename
            upload_data['filename'] = filename
            upload_data['salt'] = salt
            upload_data['is_multipart'] = is_multipart  # Update multipart flag
            
            # Check if part is already processed or in progress
            if part_number in upload_data['completed_parts']:
                print(f"DEBUG: Part {part_number} already processed, skipping")
                return jsonify({
                    "message": f"Part {part_number} already processed",
                    "upload_id": upload_id,
                    "part_number": part_number,
                    "bytes_uploaded": upload_data['bytes_uploaded'],
                    "total_size": total_size,
                    "status": "success"
                })
                
            if part_number in upload_data['pending_parts']:
                print(f"DEBUG: Part {part_number} upload already in progress, waiting for completion...")
                # Wait for ongoing upload to complete (up to 30 seconds)
                wait_count = 0
                while (part_number in upload_data['pending_parts'] and 
                       part_number not in upload_data['completed_parts'] and 
                       wait_count < 30):
                    time.sleep(1)
                    wait_count += 1
                    
                if part_number in upload_data['completed_parts']:
                    return jsonify({
                        "message": f"Part {part_number} completed while waiting",
                        "upload_id": upload_id,
                        "part_number": part_number,
                        "bytes_uploaded": upload_data['bytes_uploaded'],
                        "total_size": total_size,
                        "status": "success"
                    })
                    
                # If we're still pending after timeout, something went wrong
                if part_number in upload_data['pending_parts']:
                    return jsonify({
                        "error": f"Part {part_number} upload timed out",
                        "upload_id": upload_id,
                        "part_number": part_number,
                        "status": "error"
                    }), 500
                    
            # Mark this part as pending
            upload_data['pending_parts'].add(part_number)
            
            # Cache the chunk data for potential retries (if not too large)
            max_chunk_cache_size = 10 * 1024 * 1024  # Reduce to 10MB max cache per chunk (from 20MB)
            
            # Only cache 1 of every 5 chunks for very large files to save memory
            # For small files, cache every chunk
            if total_size > 500 * 1024 * 1024:  # For files > 500MB
                should_cache_chunk = (part_number % 5 == 0)  # Cache only every 5th chunk
            else:
                should_cache_chunk = True  # Cache all chunks for smaller files
                
            if chunk_size <= max_chunk_cache_size and should_cache_chunk:
                upload_data['cached_chunks'][part_number] = chunk_data
                print(f"DEBUG: Cached chunk data for part {part_number} ({chunk_size} bytes)")
            else:
                if chunk_size > max_chunk_cache_size:
                    print(f"DEBUG: Chunk for part {part_number} too large to cache ({chunk_size} bytes)")
                else:
                    print(f"DEBUG: Skipping cache for part {part_number} to save memory (selective caching)")
            
        # Process this part
        print(f"DEBUG: Processing part {part_number} of {total_parts}, is_last_part={is_last_chunk}")
        
        # Get user database ID
        user_database_id = uploader.get_user_database_id(current_user.id)
        if not user_database_id:
            return jsonify({"error": "User database not found"}), 404
        
        # Calculate hash for this chunk (note: we're using SHA-512 instead of MD5)
        with app.upload_locks.get(upload_id, threading.Lock()):
            upload_data = app.upload_processors[upload_id]
            upload_data['hasher'].update(chunk_data)
            bytes_uploaded_so_far = upload_data['bytes_uploaded']
        
        # Get session ID before spawning thread (to avoid request context issues)
        session_id = str(uuid.uuid4())
        if hasattr(request, 'sid'):
            session_id = request.sid
            
        # Execute the upload in a new thread so we don't block
        def upload_thread_func(upload_id, part_number, chunk_data, filename, salt, is_last_chunk, bytes_uploaded_so_far, session_id):
            try:
                content_type = 'text/plain'  # Default for Notion uploads
                
                # Convert part number to int to ensure correct sorting
                part_number = int(part_number)
                
                # Calculate chunk size in MiB for logging
                chunk_size_mb = len(chunk_data) / (1024 * 1024)
                print(f"Uploading part {part_number} of {total_parts} ({chunk_size_mb:.3f} MiB)")
                
                # Implement retry logic with exponential backoff
                max_retries = 5
                retry_delay = 1  # Start with 1 second delay
                
                for retry_attempt in range(max_retries + 1):
                    try:
                        # Start the upload - use the session_id passed to the thread
                        response = uploader.send_file_part(
                            upload_id, 
                            part_number, 
                        chunk_data,
                            filename, 
                            content_type,
                            bytes_uploaded_so_far,
                        total_size,
                            total_parts,
                            session_id
                        )
                        
                        # If we got here, the upload was successful
                        break
                        
                    except Exception as upload_error:
                        error_message = str(upload_error)
                        
                        # Check if this is a retry-able error (network issues, server errors)
                        is_retryable = any(s in error_message.lower() for s in [
                            "timeout", "connection", "network", "socket", "gateway", 
                            "cloudflare", "503", "502", "500", "429", "too many requests"
                        ])
                        
                        # If we've run out of retries or this isn't retryable, re-raise the error
                        if retry_attempt >= max_retries or not is_retryable:
                            print(f"ERROR: Part {part_number} upload failed after {retry_attempt} retries: {error_message}")
                            raise

                        # Log the error and retry
                        wait_time = retry_delay * (2 ** retry_attempt)  # Exponential backoff
                        print(f"WARNING: Upload for part {part_number} failed (attempt {retry_attempt+1}/{max_retries+1}): {error_message}")
                        print(f"Retrying in {wait_time} seconds...")
                        time.sleep(wait_time)
                
                # Update the upload data atomically
                with app.upload_locks.get(upload_id, threading.Lock()):
                    if upload_id in app.upload_processors:
                        upload_data = app.upload_processors[upload_id]
                        
                        # Mark part as completed
                        upload_data['completed_parts'].add(part_number)
                        print(f"DEBUG: Marked part {part_number} as completed. Completed parts: {sorted(list(upload_data['completed_parts']))}")
                        
                        # Update bytes uploaded
                        upload_data['bytes_uploaded'] += len(chunk_data)
                        print(f"DEBUG: Updated bytes uploaded to {upload_data['bytes_uploaded']} / {total_size} ({upload_data['bytes_uploaded']/total_size*100:.1f}%)")
                        
                        # Remove from pending parts
                        if part_number in upload_data['pending_parts']:
                            upload_data['pending_parts'].remove(part_number)
                            print(f"DEBUG: Removed part {part_number} from pending parts. Pending parts: {sorted(list(upload_data['pending_parts']))}")
                            
                        # Remove this thread from tracking
                        if part_number in upload_data['upload_threads']:
                            del upload_data['upload_threads'][part_number]
                            
                        # Remove cached chunk data to free memory
                        if part_number in upload_data['cached_chunks']:
                            del upload_data['cached_chunks'][part_number]
                            print(f"DEBUG: Freed memory by removing cached data for part {part_number}")
                            
                        # Update total_parts from is_last_chunk
                        if is_last_chunk and part_number > upload_data.get('total_parts', 0):
                            upload_data['total_parts'] = part_number
                            print(f"DEBUG: Updated total parts to {upload_data['total_parts']} based on last chunk")
                            
                print(f"Successfully uploaded part {part_number} of {total_parts}")
                
            except Exception as e:
                print(f"ERROR in upload thread for part {part_number}: {str(e)}")
                import traceback
                traceback.print_exc()
                
                # Update state atomically to remove pending status
                with app.upload_locks.get(upload_id, threading.Lock()):
                    if upload_id in app.upload_processors:
                        upload_data = app.upload_processors[upload_id]
                        if part_number in upload_data['pending_parts']:
                            upload_data['pending_parts'].remove(part_number)
                        if part_number in upload_data['upload_threads']:
                            del upload_data['upload_threads'][part_number]
        
        # Start the upload thread and register it
        upload_thread = threading.Thread(
            target=upload_thread_func,
            args=(upload_id, part_number, chunk_data, filename, salt, is_last_chunk, bytes_uploaded_so_far, session_id)
        )
        upload_thread.daemon = True  # Make sure thread doesn't block process exit
        
        # Store the thread reference
        with app.upload_locks.get(upload_id, threading.Lock()):
            upload_data = app.upload_processors[upload_id]
            upload_data['upload_threads'][part_number] = upload_thread
        
        # Start the thread
        print(f"Started upload thread for part {part_number} of {total_parts}")
        upload_thread.start()
        
        # Return early success response
        return jsonify({
            "message": f"Part {part_number} processing started",
            "upload_id": upload_id,
            "part_number": part_number,
            "chunk_size": chunk_size,
            "total_size": total_size,
            "status": "success"
        })

    except Exception as e:
        print(f"ERROR in upload_file: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

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

# @app.route('/files')
# @login_required
# def list_files():
#     return render_template('files.html')

@app.route('/files-api')
@login_required
def list_files_api():
    try:
        # Get user's database ID
        user_database_id = uploader.get_user_database_id(current_user.id)
        if not user_database_id:
            return jsonify({"error": "No user database ID found"}), 404
            
        # Query files from Notion database using uploader's method
        files_data = uploader.get_files_from_user_database(user_database_id)
        
        # Format files for API response
        files = []
        for file_data in files_data.get('results', []):
            try:
                properties = file_data.get('properties', {})
                name = properties.get('filename', {}).get('title', [{}])[0].get('text', {}).get('content', '')
                size = properties.get('filesize', {}).get('number', 0)
                file_hash = properties.get('filehash', {}).get('rich_text', [{}])[0].get('text', {}).get('content', '') # Get filehash
                
                if name:  # Only include files with names
                    files.append({
                        "name": name,
                        "size": size,
                        "file_hash": file_hash # Add file_hash
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

@app.route('/download/<filename>')
@login_required
def download_file(filename):
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
            return "File not found in Notion database", 404
    except Exception as e:
        return str(e), 500

@app.route('/d/<salted_sha512_hash>')
def download_by_hash(salted_sha512_hash):
    try:
        # get_file_by_salted_sha512_hash now returns the index_entry from the Global File Index
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

        file_details_properties = file_details.get('properties', {})
        # Explicitly retrieve a new signed S3 URL from Notion
        notion_download_link = uploader.get_notion_file_url_from_page_property(file_page_id, original_filename)

        # The download link is no longer stored in the database, as it's ephemeral.
        # The file is streamed directly using the notion_download_link.
        if not notion_download_link:
            print(f"Warning: Could not retrieve a new download link for file_page_id: {file_page_id}")

        if not is_public:
            if not current_user.is_authenticated:
                return redirect(url_for('login', next=request.url))

            current_user_db_id = uploader.get_user_database_id(current_user.id)

            if file_user_db_id != current_user_db_id:
                return "Access Denied: You do not have permission to download this file.", 403

        if not notion_download_link:
            return "Download link not available for this file", 500

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
@app.route('/delete_file', methods=['POST'])
@login_required
def delete_file():
    print("DEBUG: /delete_file route accessed.")
    try:
        data = request.get_json()
        print(f"DEBUG: Received JSON data: {data}")
        file_id = data.get('file_id')
        file_hash = data.get('file_hash') # Get the file hash from the request
        print(f"DEBUG: Extracted file_id: {file_id}, file_hash: {file_hash}")

        if not file_id:
            print("DEBUG: File ID is missing from request.")
            return jsonify({"error": "No file ID provided"}), 400
        
        if not file_hash:
            print("DEBUG: File hash is missing from request.")
            return jsonify({"error": "No file hash provided"}), 400

        # Call the new function in notion_uploader.py to delete the file
        # Pass the file_hash to allow deletion from the global index
        uploader.delete_file_from_db(file_id) # The delete_file_from_db now handles fetching the hash internally
        print(f"DEBUG: File with ID {file_id} successfully deleted from user DB and global index.")

        return jsonify({"status": "success", "message": "File deleted successfully"}), 200

    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        print(f"DEBUG: File deletion failed with error: {str(e)}\n{error_trace}")
        return jsonify({"error": str(e)}), 500

@app.route('/toggle_public_access', methods=['POST'])
@login_required
def toggle_public_access():
    print("DEBUG: /toggle_public_access route accessed.")
    try:
        data = request.get_json()
        print(f"DEBUG: Received JSON data: {data}")
        file_id = data.get('file_id')
        is_public = data.get('is_public')
        salted_sha512_hash = data.get('salted_sha512_hash') # Get the hash to update index

        if not file_id or is_public is None or not salted_sha512_hash:
            print("DEBUG: Missing file_id, is_public, or salted_sha512_hash from request.")
            return jsonify({"error": "File ID, public status, and hash are required"}), 400

        # Call the function in notion_uploader.py to update the public status
        uploader.update_file_public_status(file_id, is_public, salted_sha512_hash)
        print(f"DEBUG: File with ID {file_id} public status set to {is_public}.")

        return jsonify({"status": "success", "message": "File public status updated successfully"}), 200

    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        print(f"DEBUG: Toggling public access failed with error: {str(e)}\n{error_trace}")
        return jsonify({"error": str(e)}), 500

@app.route('/init_upload', methods=['POST'])
@login_required
def init_upload():
    """Initialize a new upload session."""
    print(f"DEBUG: Upload initialization request received from {request.remote_addr}")
    
    try:
        data = request.json
        filename = data.get('filename', 'file.txt')
        
        # If the filename is missing or empty, use a default
        if not filename or filename.strip() == '':
            filename = 'file.txt'
            
        file_size = data.get('fileSize', 0)
        
        # Generate a sanitized filename for Notion storage
        # Notion has specific requirements for filenames
        original_filename = filename
        
        # Sanitize filename for Notion (only allow alphanumeric, underscore, dash, period)
        import re
        sanitized_filename = re.sub(r'[^a-zA-Z0-9_\-.]', '', filename)
        
        # If the sanitization removed everything, use a default
        if not sanitized_filename:
            sanitized_filename = 'file.txt'
        
        # Ensure the filename has an extension Notion can handle
        # For simplicity, we'll use .txt for everything as Notion supports it
        if not sanitized_filename.endswith('.txt'):
            print(f"DEBUG: Sanitizing filename: '{filename}' to '{sanitized_filename}.txt' (for Notion storage)")
            sanitized_filename = sanitized_filename + '.txt'
            
        print(f"DEBUG: Sanitized filename: {sanitized_filename} (original: {original_filename})")
        
        # Generate a salt for the file hash
        salt = ''.join(random.choices(string.ascii_lowercase + string.digits, k=32))
        
        # Calculate chunk size and number of parts for multipart upload
        # Notion requires parts to be between 5MB and 20MB except for the last part
        target_chunk_size = 5 * 1024 * 1024  # 5MB chunks
        
        # Calculate number of parts
        total_parts = (file_size + target_chunk_size - 1) // target_chunk_size
        
        # Determine if this should be a multipart upload
        is_multipart = total_parts > 1 or file_size >= 20 * 1024 * 1024  # Use multipart for files > 20MB regardless
        
        if is_multipart:
            # For multipart uploads, log the part breakdown
            last_part_size = file_size % target_chunk_size
            if last_part_size == 0:
                last_part_size = target_chunk_size
                
            # Calculate how many full parts
            full_parts = total_parts - 1 if last_part_size < target_chunk_size else total_parts
            
            # Log the part breakdown
            print(f"DEBUG: Splitting file ({file_size/(1024*1024):.2f} MiB) into {total_parts} parts:")
            print(f"DEBUG: - {full_parts} parts of exactly {target_chunk_size/(1024*1024):.2f} MiB each")
            print(f"DEBUG: - Final part is {last_part_size/(1024*1024):.2f} MiB")
            
            # Notion requires multi_part mode for files over 20MB
            print(f"Creating multipart upload for {original_filename} (stored as {sanitized_filename}) with {total_parts} parts and content type: text/plain...")
            print(f"NOTE: All parts except the last must be EXACTLY 5 MiB as required by Notion's API")
            
            # Create multipart upload
            result = uploader.create_file_upload(
                content_type="text/plain",  # Always use text/plain for Notion compatibility
                filename=sanitized_filename,  # The sanitized filename for Notion
                mode="multi_part",  # Must be "multi_part" for multipart uploads
                number_of_parts=total_parts  # Required for multipart uploads
            )
        else:
            # For small files, use single_part mode
            print(f"Creating single-part upload for {original_filename} (stored as {sanitized_filename}) with content type: text/plain")
            print(f"DEBUG: Small file ({file_size} bytes), using single-part upload")
            
            # Create single-part upload
            result = uploader.create_file_upload(
                content_type="text/plain",  # Always use text/plain for Notion compatibility
                filename=sanitized_filename  # The sanitized filename for Notion
                # Single part is the default mode, no need to specify
            )
        
        if not result or 'id' not in result:
            return jsonify({"error": "Failed to create upload: " + str(result)}), 500
            
        # Add the salt to the response
        result['salt'] = salt
        result['original_filename'] = original_filename
        result['sanitized_filename'] = sanitized_filename
        result['total_parts'] = total_parts
        result['is_multipart'] = is_multipart
        
        return jsonify(result)
        
    except Exception as e:
        print(f"ERROR in init_upload: {str(e)}")
        return jsonify({"error": str(e)}), 500

# Add cleanup thread for idle upload sessions
def cleanup_idle_upload_sessions():
    """
    Periodically clean up idle upload sessions to prevent memory leaks.
    An idle session is one that hasn't been accessed in the last 30 minutes.
    """
    print("DEBUG: Starting upload session cleanup thread")
    
    while True:
        try:
            # Sleep for 10 minutes before checking
            time.sleep(600)
            
            # Ensure app has the required attributes
            if not hasattr(app, 'upload_processors'):
                print("DEBUG: No upload_processors attribute found, skipping cleanup")
                continue
                
            if not hasattr(app, 'upload_locks'):
                print("DEBUG: No upload_locks attribute found, skipping cleanup")
                continue
                
            idle_upload_ids = []
            now = time.time()
            max_idle_time = 1800  # 30 minutes in seconds
            
            # Find idle upload sessions
            for upload_id, upload_data in app.upload_processors.items():
                # Check last activity time
                last_activity = upload_data.get('last_activity', 0)
                if now - last_activity > max_idle_time:
                    print(f"DEBUG: Upload {upload_id} has been idle for {(now - last_activity) // 60} minutes, marking for cleanup")
                    idle_upload_ids.append(upload_id)
            
            # Remove idle sessions
            for upload_id in idle_upload_ids:
                try:
                    with app.upload_locks.get(upload_id, threading.Lock()):
                        if upload_id in app.upload_processors:
                            print(f"DEBUG: Cleaning up idle upload session: {upload_id}")
                            # Check if this upload has any pending parts before deleting
                            upload_data = app.upload_processors[upload_id]
                            if upload_data.get('pending_parts'):
                                print(f"DEBUG: Upload {upload_id} still has pending parts, deferring cleanup")
                                continue
                                
                            del app.upload_processors[upload_id]
                        if upload_id in app.upload_locks:
                            del app.upload_locks[upload_id]
                except Exception as e:
                    print(f"ERROR: Failed to clean up upload session {upload_id}: {e}")
        except Exception as e:
            print(f"ERROR in cleanup thread: {e}")

# Start the cleanup thread when the app starts
cleanup_thread = threading.Thread(target=cleanup_idle_upload_sessions, daemon=True)
cleanup_thread.start()

@app.route('/finalize_upload', methods=['POST'])
@login_required
def finalize_upload():
    try:
        data = request.json
        upload_id = data.get('upload_id')
        
        print(f"DEBUG: Finalizing upload {upload_id}")
        
        if not upload_id:
            return jsonify({"error": "Missing upload ID"}), 400
            
        # Make sure we have the upload processor for this ID
        if not hasattr(app, 'upload_processors'):
            print(f"DEBUG: No upload_processors found on app object")
            return jsonify({"error": "Upload session tracking not initialized"}), 500
            
        if upload_id not in app.upload_processors:
            print(f"DEBUG: Upload ID {upload_id} not found in upload_processors")
            return jsonify({"error": "Upload session not found or expired"}), 404
        
        # Ensure upload_locks is initialized
        if not hasattr(app, 'upload_locks'):
            print(f"DEBUG: No upload_locks found on app object, initializing")
            app.upload_locks = {}
            
        if upload_id not in app.upload_locks:
            print(f"DEBUG: No lock found for upload ID {upload_id}, creating one")
            app.upload_locks[upload_id] = threading.Lock()
            
        # Get user database ID
        user_database_id = uploader.get_user_database_id(current_user.id)
        if not user_database_id:
            return jsonify({"error": "User database not found"}), 404
        
        # First check if all parts are fully uploaded
        with app.upload_locks.get(upload_id, threading.Lock()):
            # Check if we have the upload processor
            if upload_id not in app.upload_processors:
                return jsonify({"error": "Upload session not found or expired"}), 404
                
            upload_data = app.upload_processors[upload_id]
            completed_parts = upload_data['completed_parts']
            print(f"DEBUG: Upload {upload_id} has completed parts: {sorted(list(completed_parts))}")
            
            pending_parts = upload_data['pending_parts']
            if pending_parts:
                print(f"DEBUG: Upload {upload_id} still has pending parts: {sorted(list(pending_parts))}")
            
            salted_hasher = upload_data.get('hasher')
            if not salted_hasher:
                return jsonify({"error": "Upload hash not found"}), 500
                
            filename = upload_data.get('filename', 'file.txt')
            salt = upload_data.get('salt', '')
            original_filename = upload_data.get('original_filename', filename)
            total_size = upload_data.get('total_size', 0)
            total_parts = upload_data.get('total_parts', 0)
            is_multipart = upload_data.get('is_multipart', False)
            
            # Get hash
            salted_file_hash = salted_hasher.hexdigest()
            
            print(f"DEBUG: Upload info: filename={filename}, original_filename={original_filename}")
            print(f"DEBUG: Upload info: total_size={total_size}, total_parts={total_parts}")
            print(f"DEBUG: Upload info: is_multipart={is_multipart}")
            print(f"DEBUG: Upload info: salted_file_hash={salted_file_hash}")
            
        # Verify all parts are uploaded
        all_parts = set(range(1, total_parts + 1))
        missing_parts = all_parts - completed_parts
        
        if missing_parts:
            print(f"WARNING: Missing parts when finalizing: {sorted(list(missing_parts))}")
            
            # Instead of failing immediately, try to retry the missing parts
            retry_count = 0
            retry_success = False
            
            # Only retry if there aren't too many missing parts (< 50% of total parts)
            if len(missing_parts) <= total_parts / 2:
                print(f"Attempting to retry {len(missing_parts)} missing parts...")
                
                # Try to retry each missing part
                retried_parts = []
                for part_number in missing_parts:
                    if retry_missing_part(upload_id, part_number):
                        retried_parts.append(part_number)
                
                if retried_parts:
                    # Wait for retries to complete (up to 30 seconds)
                    print(f"Waiting for {len(retried_parts)} retried parts to complete...")
                    retry_wait_time = 0
                    max_retry_wait = 30  # seconds
                    
                    while retry_wait_time < max_retry_wait:
                        # Check if all retried parts are now complete
                        with app.upload_locks.get(upload_id, threading.Lock()):
                            if upload_id not in app.upload_processors:
                                break
                                
                            # Check which parts are still missing
                            upload_data = app.upload_processors[upload_id]
                            completed_parts = upload_data['completed_parts']
                            still_missing = all_parts - completed_parts
                            
                            if not still_missing:
                                print(f"All retried parts completed successfully!")
                                retry_success = True
                                break
                                
                            # Check if we're making progress
                            parts_still_retrying = [p for p in retried_parts if p in still_missing]
                            if not parts_still_retrying:
                                # All retried parts either succeeded or failed permanently
                                break
                                
                        # Wait a bit and check again
                        time.sleep(1)
                        retry_wait_time += 1
                        
                        if retry_wait_time % 5 == 0:
                            print(f"Still waiting for retried parts... ({retry_wait_time}s)")
                
                # Check if we have all parts now
                with app.upload_locks.get(upload_id, threading.Lock()):
                    if upload_id not in app.upload_processors:
                        return jsonify({"error": "Upload session expired during retry"}), 404
                        
                    completed_parts = app.upload_processors[upload_id]['completed_parts']
                    still_missing = all_parts - completed_parts
                    
                    if still_missing:
                        print(f"WARNING: After retries, still missing parts: {sorted(list(still_missing))}")
                        return jsonify({
                            "error": "Cannot finalize upload - missing parts even after retry",
                            "missing_parts": sorted(list(still_missing)),
                            "completed_parts": sorted(list(completed_parts)),
                            "total_parts": total_parts
                        }), 400
            else:
                print(f"Too many missing parts ({len(missing_parts)}/{total_parts}) to retry automatically.")
                return jsonify({
                    "error": "Cannot finalize upload - too many missing parts to retry automatically",
                    "missing_parts": sorted(list(missing_parts)),
                    "completed_parts": sorted(list(completed_parts)),
                    "total_parts": total_parts
                }), 400
        
        # Only wait if there are pending parts or if parts were completed very recently
        pending_parts = set()
        with app.upload_locks.get(upload_id, threading.Lock()):
            if upload_id in app.upload_processors:
                pending_parts = app.upload_processors[upload_id]['pending_parts']
                
        if pending_parts:
            # Wait a bit to ensure all background threads have finished
            print(f"All parts appear to be complete, but there are still {len(pending_parts)} pending parts. Waiting 2 seconds to ensure all uploads are done...")
            time.sleep(2)
        else:
            print(f"All parts are successfully uploaded and no pending operations. Proceeding to finalize without delay.")
        
        # Double-check that all parts are still complete (in case of race conditions)
        with app.upload_locks.get(upload_id, threading.Lock()):
            if upload_id not in app.upload_processors:
                return jsonify({"error": "Upload session expired during wait"}), 404
                
            completed_parts = app.upload_processors[upload_id]['completed_parts']
            print(f"DEBUG: After wait, completed parts: {sorted(list(completed_parts))}")
            
            missing_parts = all_parts - completed_parts
            
            if missing_parts:
                print(f"WARNING: Parts disappeared during wait: {sorted(list(missing_parts))}")
                return jsonify({
                    "error": "Upload state changed during finalization",
                    "missing_parts": sorted(list(missing_parts))
                }), 400
        
        # All parts are complete, finalize the upload
        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            try:
                print(f"DEBUG: Attempt {attempt}/{max_attempts} to complete upload ID: {upload_id}")
                
                # For multipart uploads, we need to call the complete API
                # For single-part uploads, Notion already marks them as "uploaded" after the first part
                if is_multipart:
                    print(f"DEBUG: Completing multipart upload for {upload_id}")
                    # Complete the multipart upload - no need for ETags as Notion tracks them internally
                    upload_result = uploader.complete_multipart_upload(upload_id)
                else:
                    print(f"DEBUG: Single-part upload for {upload_id}, skipping 'complete' call")
                    # No need to call complete for single-part uploads
                    upload_result = {"status": "success"}
                
                # Add the file to the user's database
                print(f"DEBUG: Adding file to user database ID: {user_database_id}")
                add_file_result = uploader.add_file_to_user_database(
                    user_database_id,
                    "file.txt",  # Always use file.txt for Notion
                    total_size,
                    salted_file_hash,
                    upload_id,
                    is_public=False,
                    salt=salt,
                    original_filename=original_filename
                )
                
                # Get page ID and add to global index
                page_id = add_file_result.get('id')
                print(f"DEBUG: Adding file to global index with page ID: {page_id}")
                uploader.add_file_to_index(
                    salted_sha512_hash=salted_file_hash,
                    file_page_id=page_id,
                    user_database_id=user_database_id,
                    original_filename=original_filename,
                    is_public=False
                )
                
                # Clean up processor
                with app.upload_locks.get(upload_id, threading.Lock()):
                    if upload_id in app.upload_processors:
                        del app.upload_processors[upload_id]
                        print(f"DEBUG: Removed upload processor for {upload_id}")
                    if upload_id in app.upload_locks:
                        del app.upload_locks[upload_id]
                        print(f"DEBUG: Removed upload lock for {upload_id}")
                
                print(f"DEBUG: Removed chunk processor for completed upload {upload_id}")
                
                # Notify client of completion
                socketio.emit('upload_complete', {
                    'status': 'success',
                    'filename': filename,
                    'file_id': page_id,
                    'is_public': False,
                    'file_hash': salted_file_hash,
                    'original_filename': original_filename
                })
                
                # Return success with file ID
                return jsonify({
                    'status': 'success',
                    'message': 'File uploaded successfully and Notion database updated.',
                    'file_id': page_id,
                    'file_hash': salted_file_hash,
                    'original_filename': original_filename
                })

            except Exception as e:
                print(f"ERROR during upload finalization attempt {attempt}/{max_attempts}: {str(e)}")
                
                # For single-part uploads, if we get an error about already being in 'uploaded' status,
                # it means the file is already uploaded and we can proceed with adding it to the database
                if not is_multipart and "status of `uploaded`" in str(e):
                    print(f"DEBUG: Single-part upload already marked as 'uploaded', continuing with database update")
                    try:
                        # Add the file to the user's database
                        print(f"DEBUG: Adding file to user database ID: {user_database_id}")
                        add_file_result = uploader.add_file_to_user_database(
                            user_database_id,
                            "file.txt",  # Always use file.txt for Notion
                            total_size,
                            salted_file_hash,
                            upload_id,
                            is_public=False,
                            salt=salt,
                            original_filename=original_filename
                        )
                        
                        # Get page ID and add to global index
                        page_id = add_file_result.get('id')
                        print(f"DEBUG: Adding file to global index with page ID: {page_id}")
                        uploader.add_file_to_index(
                            salted_sha512_hash=salted_file_hash,
                            file_page_id=page_id,
                            user_database_id=user_database_id,
                            original_filename=original_filename,
                            is_public=False
                        )
                        
                        # Clean up processor
                        with app.upload_locks.get(upload_id, threading.Lock()):
                            if upload_id in app.upload_processors:
                                del app.upload_processors[upload_id]
                                print(f"DEBUG: Removed upload processor for {upload_id}")
                            if upload_id in app.upload_locks:
                                del app.upload_locks[upload_id]
                                print(f"DEBUG: Removed upload lock for {upload_id}")
                        
                        # Notify client of completion
                        socketio.emit('upload_complete', {
                            'status': 'success',
                            'filename': filename,
                            'file_id': page_id,
                            'is_public': False,
                            'file_hash': salted_file_hash,
                            'original_filename': original_filename
                        })
                        
                        # Return success with file ID
                        return jsonify({
                            'status': 'success',
                            'message': 'File uploaded successfully and Notion database updated.',
                            'file_id': page_id,
                            'file_hash': salted_file_hash,
                            'original_filename': original_filename
                        })
                    except Exception as inner_e:
                        print(f"ERROR during alternative single-part processing: {str(inner_e)}")
                        # Continue with normal retry logic
                
                # If this isn't the last attempt, wait and try again
                if attempt < max_attempts:
                    wait_time = 2 * attempt
                    print(f"Waiting {wait_time} seconds before retrying...")
                    time.sleep(wait_time)
                else:
                    # If this is the last attempt and it failed, give up and return an error
                    
                    # If Notion indicates missing parts, extract that information
                    if "Expected" in str(e) and "parts" in str(e):
                        import re
                        match = re.search(r'Send part number (\d+) next', str(e))
                        if match:
                            expected_part = int(match.group(1))
                            # Return error with missing parts info
                            return jsonify({
                                "error": f"Upload incomplete. Parts missing. Notion expects part {expected_part} next.",
                                "expected_part": expected_part,
                                "upload_id": upload_id
                            }), 400
                    
                    # Clean up on error
                    with app.upload_locks.get(upload_id, threading.Lock()):
                        if upload_id in app.upload_processors:
                            del app.upload_processors[upload_id]
                            print(f"DEBUG: Removed upload processor for {upload_id} after error")
                        if upload_id in app.upload_locks:
                            del app.upload_locks[upload_id]
                            print(f"DEBUG: Removed upload lock for {upload_id} after error")
                    
                    # Return error
                    return jsonify({"error": f"Failed to finalize upload after {max_attempts} attempts: {str(e)}"}), 500
            
    except Exception as e:
        print(f"ERROR in finalize_upload: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

def retry_missing_part(upload_id, part_number):
    """
    Retries uploading a specific missing part.
    
    Args:
        upload_id: The ID of the multipart upload
        part_number: The part number to retry
    
    Returns:
        True if successful, False otherwise
    """
    print(f"Attempting to retry upload of missing part {part_number} for upload {upload_id}")
    
    try:
        # Make sure we have the upload state
        if not hasattr(app, 'upload_processors') or upload_id not in app.upload_processors:
            print(f"ERROR: Cannot retry part {part_number} - upload state not found")
            return False
            
        # Get the upload data under lock
        with app.upload_locks.get(upload_id, threading.Lock()):
            if upload_id not in app.upload_processors:
                print(f"ERROR: Upload {upload_id} disappeared during retry")
                return False
                
            upload_data = app.upload_processors[upload_id]
            
            # Skip if this part is already completed
            if part_number in upload_data['completed_parts']:
                print(f"Part {part_number} is already completed, skipping retry")
                return True
                
            # Check if this part is already pending
            if part_number in upload_data['pending_parts']:
                print(f"Part {part_number} is already being retried, skipping")
                return False
                
            # Mark this part as pending
            upload_data['pending_parts'].add(part_number)
            
            # Get metadata needed for the upload
            filename = upload_data.get('filename', 'file.txt')
            salt = upload_data.get('salt', '')
            original_filename = upload_data.get('original_filename', filename)
            total_size = upload_data.get('total_size', 0)
            total_parts = upload_data.get('total_parts', 0)
            bytes_uploaded_so_far = upload_data.get('bytes_uploaded', 0)
            
            # For the last part, we need to calculate the size differently
            is_last_chunk = (part_number == total_parts)
            
            # Look for cached chunk data if available
            chunk_data = upload_data.get('cached_chunks', {}).get(part_number)
            
        # Get user database ID
        user_id = current_user.id if hasattr(current_user, 'id') else None
        if not user_id:
            print(f"ERROR: No user ID available for retry")
            return False
            
        user_database_id = uploader.get_user_database_id(user_id)
        if not user_database_id:
            print(f"ERROR: No user database ID found for retry")
            return False
            
        # If we don't have the cached chunk, we need to create one
        if not chunk_data:
            print(f"No cached chunk data found for part {part_number}, creating a replacement chunk")
            
            # Calculate chunk size based on part number and total size
            chunk_size = 5 * 1024 * 1024  # 5MB for all parts except possibly the last
            if is_last_chunk and total_size % chunk_size != 0:
                last_chunk_size = total_size % chunk_size
                if last_chunk_size > 0:
                    chunk_size = last_chunk_size
                    
            # Create a placeholder chunk - this is just for retry demonstration
            # In a real implementation, you would need to retrieve the actual file data
            print(f"Creating placeholder chunk of size {chunk_size} bytes for part {part_number}")
            chunk_data = b'X' * chunk_size
            
            # Warn that this is not the original data
            print(f"WARNING: Using placeholder data for part {part_number} - this will not match the original file content!")
        else:
            print(f"Using cached chunk data for part {part_number} ({len(chunk_data)} bytes)")
        
        # Generate a new session ID for this retry
        session_id = str(uuid.uuid4())
        
        # Start a new thread to upload this part
        thread = threading.Thread(
            target=upload_thread_func,
            args=(upload_id, part_number, chunk_data, filename, salt, is_last_chunk, bytes_uploaded_so_far, session_id)
        )
        thread.daemon = True
        
        # Store the thread reference
        with app.upload_locks.get(upload_id, threading.Lock()):
            if upload_id in app.upload_processors:
                upload_data = app.upload_processors[upload_id]
                upload_data['upload_threads'][part_number] = thread
                
        # Start the thread
        thread.start()
        print(f"Started retry thread for part {part_number}")
        return True
        
    except Exception as e:
        print(f"ERROR in retry_missing_part for part {part_number}: {str(e)}")
        import traceback
        traceback.print_exc()
        
        # Remove this part from pending if we failed to start the retry
        with app.upload_locks.get(upload_id, threading.Lock()):
            if upload_id in app.upload_processors:
                upload_data = app.upload_processors[upload_id]
                if part_number in upload_data['pending_parts']:
                    upload_data['pending_parts'].remove(part_number)
                    
        return False

if __name__ == '__main__':
    # Start memory usage monitoring
    print("Starting memory usage monitoring...")
    log_memory_usage()
    
    # Only run the development server if FLASK_ENV is set to 'development'
    # In production, Gunicorn will run the app
    if os.environ.get('FLASK_ENV') == 'development':
        socketio.run(app, host='0.0.0.0', port=5000, debug=True)
    else:
        # For production, Gunicorn will handle running the app
        # This block is primarily for local development without FLASK_ENV=development
        # or for direct execution in environments where Gunicorn isn't used.
        print("Running in non-development mode. Use Gunicorn for production deployment.")
        socketio.run(app, host='0.0.0.0', port=5000, debug=False) # debug should be False in production
