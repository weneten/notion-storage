import hashlib
import mimetypes
import secrets
from flask import Flask, request, render_template, jsonify, redirect, url_for, send_from_directory, Response, stream_with_context
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from uploader.notion_uploader import NotionFileUploader
import os
import io
import bcrypt
import base64
import uuid
from flask_socketio import SocketIO
import math
from dotenv import load_dotenv
load_dotenv()

from flask_cors import CORS

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY')
CORS(app)  # Enable CORS for all routes
socketio = SocketIO(app, cors_allowed_origins="*")

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
                    name = properties.get('filename', {}).get('title', [{}])[0].get('text', {}).get('content', '')
                    size = properties.get('filesize', {}).get('number', 0)
                    file_id = file_data.get('id') # Extract the Notion page ID
                    is_public = properties.get('is_public', {}).get('checkbox', False) # Get is_public status
                    file_hash = properties.get('filehash', {}).get('rich_text', [{}])[0].get('text', {}).get('content', '') # Get filehash
                    
                    if name:
                        files.append({
                            "name": name,
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
    print(f"DEBUG: Upload request received from {request.remote_addr}")
    print(f"DEBUG: Headers: {dict(request.headers)}")
    print(f"DEBUG: Cookies: {request.cookies}")
    
    if 'file' not in request.files:
        print("DEBUG: No file part in request")
        return jsonify({"error": "No file part"}), 400

    file = request.files['file']
    print(f"DEBUG: File received: {file.filename} ({file.content_length} bytes)")

    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400

    # Notion API supports files up to 5GB (5 GiB), so we'll set the limit accordingly
    MAX_FILE_SIZE_BYTES = 5 * 1024 * 1024 * 1024 # 5 GiB in bytes
    MAX_FILE_SIZE_MB = MAX_FILE_SIZE_BYTES / (1024 * 1024) # Convert to MiB for display

    # Get the actual file size by seeking
    file.seek(0, os.SEEK_END)
    total_size = file.tell()
    file.seek(0)

    if total_size > MAX_FILE_SIZE_BYTES:
        return jsonify({"error": f"File size exceeds the {int(MAX_FILE_SIZE_MB)} MiB limit."}), 413

    # The file content is read into a BytesIO stream.
    # The previous progress reporting loop was removed from this section.
    # It was reporting the progress of reading the file into server memory,
    # which is very fast and caused the progress bar to jump to 100% instantly.
    # True upload progress is now handled within the `NotionFileUploader`.
    try:
        # Generate a random salt for SHA512 hashing
        salt = secrets.token_hex(16) # 16 bytes = 32 hex characters
        salted_hasher = hashlib.sha512()
        salted_hasher.update(salt.encode('utf-8')) # Hash the salt first

        # Create a generator to yield chunks and update hash
        def generate_chunks_and_hash():
            # Define the target chunk size for Notion multipart upload (5 MiB)
            TARGET_CHUNK_SIZE = 5 * 1024 * 1024 # 5 MiB

            current_buffer = io.BytesIO()
            bytes_in_buffer = 0

            while True:
                chunk = file.stream.read(8192)  # Read in 8KB chunks
                if not chunk:
                    break
                salted_hasher.update(chunk) # Update hash with file content
                current_buffer.write(chunk)
                bytes_in_buffer += len(chunk)

                if bytes_in_buffer >= TARGET_CHUNK_SIZE:
                    current_buffer.seek(0)
                    yield current_buffer.read()
                    current_buffer = io.BytesIO()
                    bytes_in_buffer = 0
            
            # Yield any remaining data in the buffer
            if bytes_in_buffer > 0:
                current_buffer.seek(0)
                yield current_buffer.read()
        
        # Pass the generator to the uploader, along with the original file object
        # The uploader will consume the generator and handle the stream
        upload_result = uploader.upload_file_stream(generate_chunks_and_hash(), file.filename, current_user.id, total_size)
        
        salted_file_hash = salted_hasher.hexdigest() # This will be the public link hash

        # Get user's database ID
        user_database_id = uploader.get_user_database_id(current_user.id)
        if not user_database_id:
            raise Exception("User database not found")

        # Add file information to user's database with all metadata
        add_file_result = uploader.add_file_to_user_database(
            user_database_id,
            file.filename,
            total_size,
            salted_file_hash, # Use the salted SHA512 hash
            upload_result.get('file_upload_id'),
            is_public=False, # Default to private
            salt=salt # Store the salt
        )

        # Get the page_id of the newly created database entry
        page_id = add_file_result.get('id')

        # Add file to the Global File Index
        uploader.add_file_to_index(
            salted_sha512_hash=salted_file_hash,
            file_page_id=page_id,
            user_database_id=user_database_id,
            original_filename=file.filename,
            is_public=False # Initial public status in index
        )

        # Emit upload_complete event with necessary data
        socketio.emit('upload_complete', {
            'status': 'success',
            'original_filename': file.filename
        })
        return jsonify({
            'status': 'success',
            'message': 'File uploaded successfully and Notion database updated.'
        }), 200
    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        print(f"DEBUG: Upload failed with error: {str(e)}\n{error_trace}")
        return jsonify({"error": str(e)}), 500
    finally:
        # No need to remove file_path as it's saved to disk
        pass

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

if __name__ == '__main__':
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
