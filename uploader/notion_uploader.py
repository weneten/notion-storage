from flask import Flask, request, jsonify, Response, stream_with_context
import os
import requests
import urllib.parse
import threading
import io
import hashlib
import secrets
import queue
from typing import Dict, Any, List, Optional, Tuple, Union, Iterable
from flask_socketio import SocketIO
import concurrent.futures

class NotionFileUploader:
    def __init__(self, api_token: str, socketio: SocketIO = None, notion_version: str = "2022-06-28", global_file_index_db_id: str = None):
        self.api_token = api_token
        self.socketio = socketio
        self.base_url = "https://api.notion.com/v1"
        self.notion_version = notion_version
        self.headers = {
            "Authorization": f"Bearer {api_token}",
            "Notion-Version": notion_version,
            "accept": "application/json"
        }
        self.global_file_index_db_id = global_file_index_db_id
        self.CHUNK_SIZE = 5 * 1024 * 1024  # 5 MiB for Notion's multipart upload requirement

    def ensure_txt_filename(self, filename: str) -> str:
        """Ensure filename has .txt extension but do not replace spaces"""
        if not filename.lower().endswith('.txt'):
            name_without_ext = os.path.splitext(filename)[0]
            filename = f"{name_without_ext}.txt"
        return filename

    def get_mime_type(self, filename: str) -> str:
        """Always return text/plain to ensure compatibility with Notion's File Upload API"""
        return 'text/plain'

    def create_file_upload(self, filename: str, content_type: str) -> Dict[str, Any]:
        """Step 1: Create a file upload object for small files"""
        url = f"{self.base_url}/file_uploads"

        # Always use generic file.txt for Notion's site
        notion_filename = "file.txt"

        payload = {
            "filename": notion_filename,
            "content_type": content_type
        }

        headers = {**self.headers, "Content-Type": "application/json"}

        print(f"Creating file upload object for {filename} with content type: {content_type}...")
        response = requests.post(url, json=payload, headers=headers)

        if response.status_code != 200:
            raise Exception(f"File upload creation failed with status {response.status_code}: {response.text}")

        upload_info = response.json()
        print(f"File upload object created: ID = {upload_info['id']}")
        return upload_info

    def send_file_content(self, file_upload_id: str, file_stream: Union[Iterable[bytes], io.BytesIO], content_type: str, filename: str, total_size: int) -> Dict[str, Any]:
        """Step 2: Send file content to Notion (for single file uploads)"""
        url = f"{self.base_url}/file_uploads/{file_upload_id}/send"

        # Always use generic file.txt for Notion's site
        notion_filename = "file.txt"

        # If file_stream is a generator, consume it into a BytesIO object
        if not isinstance(file_stream, io.BytesIO):
            buffer = io.BytesIO()
            for chunk in file_stream:
                buffer.write(chunk)
            buffer.seek(0) # Rewind to the beginning
            file_content_to_send = buffer
        else:
            file_content_to_send = file_stream

        # Use the 'files' parameter to send the file content as multipart/form-data
        # The 'file' key here corresponds to the 'file' field Notion expects in the request body.
        files = {
            'file': (notion_filename, file_content_to_send, content_type)
        }

        headers = {
            'Authorization': self.headers['Authorization'],
            'Notion-Version': self.headers['Notion-Version'],
            # Content-Type for multipart/form-data is set automatically by requests when using 'files'
        }

        print(f"Uploading file content for {filename} with content type: {content_type}...")
        response = requests.post(url, files=files, headers=headers)

        if response.status_code != 200:
            raise Exception(f"File content upload failed with status {response.status_code}: {response.text}")

        result = response.json()
        print(f"File content uploaded successfully. Status: {result.get('status')}")
        return result

    def create_file_block(self, page_id: str, file_upload_id: str, filename: str) -> Dict[str, Any]:
        url = f"{self.base_url}/blocks/{page_id}/children"

        # Always use generic file.txt for Notion's site
        notion_filename = "file.txt"

        payload = {
            "children": [
                {
                    "type": "file",
                    "file": {
                        "type": "file_upload",
                        "file_upload": {
                            "id": file_upload_id
                        }
                    }
                }
            ]
        }

        headers = {**self.headers, "Content-Type": "application/json"}

        print(f"Creating file block for {filename}...")
        response = requests.patch(url, json=payload, headers=headers)

        if response.status_code != 200:
            raise Exception(f"File block creation failed with status {response.status_code}: {response.text}")

        result = response.json()
        print(f"File block created successfully!")
        return result

    def get_block_info(self, block_id: str) -> Dict[str, Any]:
        """Get information about a specific block"""
        url = f"{self.base_url}/blocks/{block_id}"

        try:
            response = requests.get(url, headers=self.headers)
            if response.status_code == 200:
                return response.json()
            else:
                raise Exception(f"Failed to get block info: {response.status_code} - {response.text}")
        except Exception as e:
            print(f"Error getting block info: {e}")
            return {}

    def get_download_url(self, block_result: Dict[str, Any], original_filename: str) -> str:
        """
        Get the Notion download link for the uploaded file.
        This method is based on get_file_download_info from uplaod_conc.py
        """
        try:
            # Get block ID from the result
            block_id = block_result['results'][0]['id'] if block_result.get('results') else None
            if not block_id:
                return ""

            # Get block information to extract file details
            block_info = self.get_block_info(block_id)
            if not block_info:
                return ""

            # Extract file information from block
            file_info = block_info.get('file', {})
            file_url = file_info.get('file', {}).get('url', '')
            if not file_url:
                return ""

            return file_url
        except Exception as e:
            print(f"Error constructing download URL: {e}")
            return ""

    def verify_page_id(self, page_id: str) -> bool:
        """Verify that the page ID exists and is accessible"""
        url = f"{self.base_url}/pages/{page_id}"

        try:
            response = requests.get(url, headers=self.headers)
            return response.status_code == 200
        except Exception as e:
            print(f"Error verifying page ID: {e}")
            return False

    def get_notion_file_url_from_page_property(self, page_id: str, original_filename: str) -> str:
        """
        Retrieves a Notion-style download link for a file attached to a database page's 'file' property.
        """
        try:
            # Get page information to extract file details
            page_info = self.get_user_by_id(page_id) # Reusing get_user_by_id as it fetches a page
            if not page_info:
                return ""

            # Extract file information from the 'file' property
            # Assuming 'file' is the name of the file property in the database
            file_property = page_info.get('properties', {}).get('file', {})
            files_array = file_property.get('files', [])

            if not files_array:
                return ""

            # Get the first file in the array
            file_info = files_array[0]
            file_url = file_info.get('file', {}).get('url', '') # Corrected path to the URL

            if not file_url:
                return ""

            return file_url
        except Exception as e:
            print(f"Error constructing download URL from page property: {e}")
            return ""

    def _emit_progress(self, percentage: float, bytes_uploaded: int, total_bytes: int, status: str = None, part: int = None, total_parts: int = None):
        """Helper method to emit progress updates with status messages"""
        if self.socketio:
            data = {
                'percentage': min(percentage, 100),  # Ensure we don't exceed 100%
                'bytes_uploaded': bytes_uploaded,
                'total_bytes': total_bytes
            }
            if status:
                data['status'] = status
            if part is not None:
                data['part'] = part
            if total_parts is not None:
                data['total_parts'] = total_parts
            self.socketio.emit('upload_progress', data)

    def _concurrent_part_uploader(
        self,
        file_upload_id: str,
        part_number: int,
        chunk_data: bytes,
        filename: str,
        content_type: str,
        total_size: int,
        number_of_parts: int,
        bytes_uploaded_counter: List[int],
        lock: threading.Lock
    ):
        """Worker function for concurrent uploads. Uploads a part and updates shared progress."""
        try:
            print(f"Worker uploading part {part_number}...")
            self.send_file_part_without_progress(file_upload_id, part_number, chunk_data, filename, content_type)
            print(f"Worker finished uploading part {part_number}.")

            # Update shared progress counter under a lock
            with lock:
                bytes_uploaded_counter[0] += len(chunk_data)
                percentage = (bytes_uploaded_counter[0] / total_size) * 100
                self._emit_progress(
                    percentage,
                    bytes_uploaded_counter[0],
                    total_size,
                    # No status here - the UI will handle it based on percentage
                    part=part_number, 
                    total_parts=number_of_parts
                )
        except Exception as e:
            print(f"Error in uploader worker thread for part {part_number}: {e}")
            raise

    def upload_file_stream(self, file_stream: Iterable[bytes], filename: str, user_id: str, total_size: int) -> Dict[str, Any]:
        """
        Handles the file upload process. For small files, it's a single operation.
        For large files, it performs a concurrent multipart upload and returns an
        intermediate dictionary for a separate finalization step.
        """
        try:
            self._emit_progress(0, 0, total_size, "Preparing upload...")
            
            salt = secrets.token_hex(16)
            salted_hasher = hashlib.sha512()
            salted_hasher.update(salt.encode('utf-8'))

            database_id = self.get_user_database_id(user_id)
            if not database_id:
                raise Exception("User database not found.")
                
            original_filename = filename
            notion_filename = "file.txt"
            content_type = self.get_mime_type(original_filename)

            if total_size <= self.CHUNK_SIZE:
                # Small files are uploaded and finalized in one step.
                self._emit_progress(0, 0, total_size, status='Buffering...')
                buffer = io.BytesIO()
                bytes_read = 0
                for chunk in file_stream:
                    salted_hasher.update(chunk)
                    buffer.write(chunk)
                    bytes_read += len(chunk)
                    percentage = (bytes_read / total_size) * 100 if total_size > 0 else 100
                    self._emit_progress(percentage, bytes_read, total_size, status='Uploading...')
                buffer.seek(0)

                upload_info = self.create_file_upload(notion_filename, content_type)
                file_upload_id = upload_info['id']
                
                self.send_file_content(file_upload_id, buffer, content_type, notion_filename, total_size)
                download_url = f"https://www.notion.so/file/{file_upload_id}"

                file_hash = salted_hasher.hexdigest()
                user_db_entry = self.add_file_to_user_database(database_id, original_filename, total_size, file_hash, file_upload_id, is_public=False, salt=salt)
                file_page_id = user_db_entry['id']
                self.add_file_to_index(file_hash, file_page_id, database_id, original_filename, False)
                
                return {
                    "upload_in_progress": False,
                    "message": "File uploaded successfully",
                    "download_link": download_url,
                    "original_filename": original_filename,
                    "database_id": database_id,
                    "file_upload_id": file_upload_id,
                    "salted_file_hash": file_hash,
                    "salt": salt,
                    "total_bytes": total_size
                }
            else:
                # Large files: only upload parts and return data for finalization.
                number_of_parts = (total_size + self.CHUNK_SIZE - 1) // self.CHUNK_SIZE
                upload_info = self.create_multipart_upload(notion_filename, content_type, number_of_parts)
                file_upload_id = upload_info['id']

                lock = threading.Lock()
                bytes_uploaded_counter = [0]
                futures = []
                self._emit_progress(0, 0, total_size, "Starting Upload...", part=0, total_parts=number_of_parts)

                with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                    current_part = 1
                    buffer = io.BytesIO()
                    for chunk in file_stream:
                        salted_hasher.update(chunk)
                        buffer.write(chunk)
                        if buffer.tell() >= self.CHUNK_SIZE:
                            buffer.seek(0)
                            part_data = buffer.read(self.CHUNK_SIZE)
                            future = executor.submit(self._concurrent_part_uploader, file_upload_id, current_part, part_data, notion_filename, content_type, total_size, number_of_parts, bytes_uploaded_counter, lock)
                            futures.append(future)
                            remaining_data = buffer.read()
                            buffer = io.BytesIO()
                            buffer.write(remaining_data)
                            current_part += 1
                    if buffer.tell() > 0:
                        buffer.seek(0)
                        last_part_data = buffer.read()
                        future = executor.submit(self._concurrent_part_uploader, file_upload_id, current_part, last_part_data, notion_filename, content_type, total_size, number_of_parts, bytes_uploaded_counter, lock)
                        futures.append(future)
                    for future in concurrent.futures.as_completed(futures):
                        future.result()

                file_hash = salted_hasher.hexdigest()
                return {
                    "upload_in_progress": True,
                    "file_upload_id": file_upload_id,
                    "database_id": database_id,
                    "original_filename": original_filename,
                    "total_size": total_size,
                    "salted_file_hash": file_hash,
                    "salt": salt,
                }
        except Exception as e:
            import traceback
            traceback.print_exc()
            self.socketio.emit('upload_complete', {'status': 'error', 'error': str(e)})
            raise Exception(f"Error in upload_file_stream: {str(e)}")

    def finalize_large_upload(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Finalizes a large multipart upload after all parts have been sent."""
        file_upload_id = data['file_upload_id']
        database_id = data['database_id']
        original_filename = data['original_filename']
        total_size = data['total_size']
        file_hash = data['salted_file_hash']
        salt = data['salt']

        try:
            complete_result = self.complete_multipart_upload(file_upload_id)
            download_url = complete_result.get('file', {}).get('url', f"https://www.notion.so/file/{file_upload_id}")

            user_db_entry = self.add_file_to_user_database(database_id, original_filename, total_size, file_hash, file_upload_id, is_public=False, salt=salt)
            file_page_id = user_db_entry['id']
            
            self.add_file_to_index(file_hash, file_page_id, database_id, original_filename, False)
            
            return {
                "message": "File uploaded successfully",
                "download_link": download_url,
                "original_filename": original_filename,
                "database_id": database_id,
                "file_upload_id": file_upload_id,
                "salted_file_hash": file_hash,
                "salt": salt,
                "total_bytes": total_size
            }
        except Exception as e:
            print(f"Error finalizing large upload: {e}")
            self.socketio.emit('upload_complete', {'status': 'error', 'error': str(e)})
            raise

    def send_file_part_without_progress(self, file_upload_id: str, part_number: int, chunk_data: bytes, filename: str, content_type: str) -> Dict[str, Any]:
        """Upload a single part of a multipart upload without sending progress"""
        url = f"{self.base_url}/file_uploads/{file_upload_id}/send"

        files = {
            'file': (filename, chunk_data, content_type),
            'part_number': (None, str(part_number))
        }

        headers = {
            'Authorization': self.headers['Authorization'],
            'Notion-Version': self.headers['Notion-Version']
        }

        response = requests.post(url, files=files, headers=headers)

        if response.status_code != 200:
            raise Exception(f"Part upload failed with status {response.status_code}: {response.text}")

        return response.json()

    def create_multipart_upload(self, filename: str, content_type: str, number_of_parts: int) -> Dict[str, Any]:
        """Create a multipart upload for large files"""
        url = f"{self.base_url}/file_uploads"

        # Always use generic file.txt for Notion's site
        notion_filename = "file.txt"

        payload = {
            "filename": notion_filename,
            "content_type": content_type,
            "mode": "multi_part",
            "number_of_parts": number_of_parts
        }

        headers = {**self.headers, "Content-Type": "application/json"}

        print(f"Creating multipart upload for {filename} with {number_of_parts} parts and content type: {content_type}...")
        response = requests.post(url, json=payload, headers=headers)

        if response.status_code != 200:
            raise Exception(f"Multipart upload creation failed: {response.text}")

        return response.json()

    def complete_multipart_upload(self, file_upload_id: str) -> Dict[str, Any]:
        """Complete the multipart upload"""
        url = f"{self.base_url}/file_uploads/{file_upload_id}/complete"

        headers = {**self.headers, "Content-Type": "application/json"}

        print("Completing multipart upload...")
        response = requests.post(url, headers=headers)

        if response.status_code != 200:
            raise Exception(f"Multipart upload completion failed: {response.text}")

        return response.json()

    def query_user_database_by_username(self, database_id: str, username: str) -> Dict[str, Any]:
        """Query the Notion user database for a specific username"""
        url = f"{self.base_url}/databases/{database_id}/query"

        payload = {
            "filter": {
                "property": "Name",
                "title": {
                    "equals": username
                }
            }
        }

        headers = {**self.headers, "Content-Type": "application/json"}

        response = requests.post(url, json=payload, headers=headers)

        if response.status_code != 200:
            raise Exception(f"User database query failed: {response.text}")

        return response.json()

    def create_user(self, database_id: str, username: str, password_hash: str) -> Dict[str, Any]:
        """Create a new user in the Notion database"""
        url = f"{self.base_url}/pages"

        # First verify the database exists and is accessible
        try:
            db_url = f"{self.base_url}/databases/{database_id}"
            db_response = requests.get(db_url, headers=self.headers)
            if db_response.status_code != 200:
                raise Exception(f"Database verification failed: {db_response.text}")
        except Exception as e:
            raise Exception(f"Failed to verify database: {str(e)}")

        # Prepare minimal required properties
        payload = {
            "parent": {"database_id": database_id},
            "properties": {
                "Name": {
                    "title": [{"text": {"content": username}}]
                },
                "Password-Hash": {
                    "rich_text": [{"text": {"content": password_hash}}]
                }
            }
        }

        headers = {
            **self.headers,
            "Content-Type": "application/json"
        }

        try:
            print(f"Creating user with payload: {payload}")  # Debug logging
            response = requests.post(url, json=payload, headers=headers)
            
            if response.status_code != 200:
                error_msg = f"Status: {response.status_code}\nResponse: {response.text}"
                print(f"User creation error: {error_msg}")  # Debug logging
                raise Exception(error_msg)
                
            return response.json()
        except Exception as e:
            raise Exception(f"User creation failed: {str(e)}")

    def ensure_database_property(self, database_id: str, property_name: str, property_type: str, property_config: Optional[Dict[str, Any]] = None) -> bool:
        """Ensure a property exists in a database, creating it if needed"""
        try:
            # First get the database schema
            url = f"{self.base_url}/databases/{database_id}"
            response = requests.get(url, headers=self.headers)
            
            if response.status_code != 200:
                raise Exception(f"Failed to get database schema: {response.text}")
                
            schema = response.json().get('properties', {})
            
            # If property already exists, return
            if property_name in schema:
                return True
                
            # Create the property with proper configuration based on type
            url = f"{self.base_url}/databases/{database_id}"
            
            # Handle relation properties specially
            if property_type == "relation":
                if not property_config or "database_id" not in property_config:
                    raise ValueError("Relation properties require a database_id in property_config")
                
                payload = {
                    "properties": {
                        property_name: {
                            "type": "relation",
                            "relation": property_config
                        }
                    }
                }
            else:
                payload = {
                    "properties": {
                        property_name: {
                            property_type: property_config if property_config is not None else {}
                        }
                    }
                }
            
            response = requests.patch(url, json=payload, headers=self.headers)
            
            if response.status_code != 200:
                raise Exception(f"Failed to create property: {response.text}")
            
            # Re-fetch the database schema to confirm the property is now present
            response = requests.get(url, headers=self.headers)
            if response.status_code != 200:
                raise Exception(f"Failed to re-fetch database schema after property creation: {response.text}")
            
            updated_schema = response.json().get('properties', {})
            if property_name in updated_schema:
                return True
            else:
                raise Exception(f"Property '{property_name}' was not found in database schema after creation attempt.")
                
        except Exception as e:
            print(f"Error ensuring database property: {e}")
            return False

    def _get_database_properties_map(self, database_id: str) -> Dict[str, Any]:
        """Helper to get a map of property names to their full property objects (including IDs)"""
        url = f"{self.base_url}/databases/{database_id}"
        response = requests.get(url, headers=self.headers)
        if response.status_code != 200:
            raise Exception(f"Failed to get database schema for properties map: {response.text}")
        return response.json().get('properties', {})

    def update_user_properties(self, user_id: str, properties: Dict[str, Any]) -> Dict[str, Any]:
        """Update multiple properties of a user's Notion page."""
        url = f"{self.base_url}/pages/{user_id}"

        try:
            # First get the current page and schema
            page = self.get_user_by_id(user_id)
            if not page:
                raise Exception("User page not found")
            
            schema = page.get('properties', {})
            print(f"Debug - Current schema: {schema}")  # Debug logging
            
            # Prepare payload with validated properties
            payload_properties = {}
            
            for prop_name, prop_value in properties.items():
                if prop_name not in schema:
                    print(f"Warning: Property {prop_name} not found in schema")
                    continue
                    
                prop_schema = schema[prop_name]
                prop_type = prop_schema.get('type')
                
                # Special handling for User Database property (can be URL or relation)
                if prop_name == 'User Database':
                    if isinstance(prop_value, dict) and 'url' in prop_value:
                        payload_properties[prop_name] = {
                            "type": "url",
                            "url": prop_value['url']
                        }
                    elif isinstance(prop_value, dict) and 'relation' in prop_value:
                        payload_properties[prop_name] = {
                            "type": "relation",
                            "relation": prop_value['relation']
                        }
                    else:
                        raise ValueError("User Database must include either url or relation data")
                else:
                    # For non-relation properties, use the value as-is
                    payload_properties[prop_name] = prop_value

            if not payload_properties:
                raise Exception("No valid properties to update")

            payload = {
                "properties": payload_properties
            }
            
            print(f"Debug - Update payload: {payload}")  # Debug logging

            headers = {
                **self.headers,
                "Content-Type": "application/json"
            }

            response = requests.patch(url, json=payload, headers=headers)
            
            if response.status_code != 200:
                error_msg = f"Status: {response.status_code}\nResponse: {response.text}"
                print(f"Debug - Update error: {error_msg}")  # Debug logging
                raise Exception(error_msg)
                
            return response.json()
            
        except Exception as e:
            error_details = f"{str(e)}"
            if hasattr(e, 'response') and e.response:
                error_details += f"\nResponse: {e.response.text}"
            raise Exception(f"Failed to update user properties: {error_details}")

    def update_user_password(self, user_id: str, new_password_hash: str) -> Dict[str, Any]:
        """Update the password hash for a user in the Notion database"""
        url = f"{self.base_url}/pages/{user_id}"

        payload = {
            "properties": {
                "Password-Hash": {
                    "type": "rich_text",
                    "rich_text": [
                        {
                            "type": "text",
                            "text": {
                                "content": new_password_hash
                            }
                        }
                    ]
                }
            }
        }

        headers = {**self.headers, "Content-Type": "application/json"}

        response = requests.patch(url, json=payload, headers=headers)

        if response.status_code != 200:
            raise Exception(f"Failed to update user password: {response.text}")

        return response.json()

    def get_user_by_id(self, user_id: str) -> Dict[str, Any]:
        """Get a user page by its ID"""
        url = f"{self.base_url}/pages/{user_id}"

        headers = {**self.headers, "Content-Type": "application/json"}

        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            raise Exception(f"Failed to get user by ID: {response.text}")

        return response.json()

    def get_user_database_id(self, user_id: str) -> Optional[str]:
        """
        Retrieves the user's dedicated Notion database ID from their user page.
        """
        try:
            user_data = self.get_user_by_id(user_id)
            if not user_data:
                print(f"User data not found for ID: {user_id}")
                return None

            # The 'User Database' property is expected to be a URL type
            user_db_property = user_data.get('properties', {}).get('User Database', {})
            user_db_url = user_db_property.get('url')

            if not user_db_url:
                print(f"User Database URL not found for user ID: {user_id}")
                return None

            # Extract the database ID from the URL
            # Notion database URLs are typically like: https://www.notion.so/DATABASE_ID_WITHOUT_HYPHENS
            # We need to convert it back to the hyphenated format.
            parsed_url = urllib.parse.urlparse(user_db_url)
            path_segments = [s for s in parsed_url.path.split('/') if s]
            
            if not path_segments:
                print(f"Could not parse database ID from URL: {user_db_url}")
                return None
            
            # The database ID is the last segment, without hyphens
            db_id_without_hyphens = path_segments[-1]
            
            # Convert to hyphenated format (8-4-4-4-12)
            hyphenated_db_id = (
                f"{db_id_without_hyphens[0:8]}-"
                f"{db_id_without_hyphens[8:12]}-"
                f"{db_id_without_hyphens[12:16]}-"
                f"{db_id_without_hyphens[16:20]}-"
                f"{db_id_without_hyphens[20:]}"
            )
            return hyphenated_db_id

        except Exception as e:
            print(f"Error getting user database ID for user {user_id}: {e}")
            return None

    # New methods for user-specific file storage and Notion database
    def create_user_database(self, parent_id: str, username: str) -> Dict[str, Any]:
        """Create a Notion database for a user to store file information"""
        url = f"{self.base_url}/databases"

        # Define the database schema with all required properties
        database_schema = {
            "parent": {"page_id": parent_id},
            "title": [
                {
                    "text": {
                        "content": f"{username}'s File Database"
                    }
                }
            ],
            "properties": {
                "filename": {
                    "title": {}
                },
                "filesize": {
                    "number": {
                        "format": "number"
                    }
                },
                "filehash": {
                    "rich_text": {}
                },
                "file": {
                    "files": {}
                },
                "is_public": {
                    "checkbox": {}
                },
                "salt": {
                    "rich_text": {}
                }
            },
            "is_inline": True
        }

        headers = {**self.headers, "Content-Type": "application/json"}

        # Create the database and return URL
        response = requests.post(url, json=database_schema, headers=headers)
        if response.status_code != 200:
            raise Exception(f"Failed to create user database: {response.text}")

        result = response.json()
        # Ensure the URL is generated correctly for the database
        # Notion database URLs are typically like: https://www.notion.so/DATABASE_ID_WITHOUT_HYPHENS
        result['url'] = f"https://www.notion.so/{result['id'].replace('-', '')}"
        return result

    def get_files_from_user_database(self, database_id: str) -> Dict[str, Any]:
        """Queries a user's Notion database for all file entries."""
        url = f"{self.base_url}/databases/{database_id}/query"
        headers = {**self.headers, "Content-Type": "application/json"}
        
        try:
            response = requests.post(url, headers=headers)
            if response.status_code != 200:
                raise Exception(f"Failed to query user database: {response.text}")
            return response.json()
        except Exception as e:
            print(f"Error querying user database {database_id}: {e}")
            raise

    def delete_file_from_db(self, file_page_id: str) -> Dict[str, Any]:
        """Deletes a file entry (page) from a Notion database and the Global File Index."""
        url = f"{self.base_url}/pages/{file_page_id}"
        headers = {**self.headers, "Content-Type": "application/json"}
        
        try:
            # First, retrieve the file's hash before archiving it
            file_details = self.get_user_by_id(file_page_id)
            if not file_details:
                raise Exception(f"File details not found for page ID: {file_page_id}")
            
            salted_sha512_hash = file_details.get('properties', {}).get('filehash', {}).get('rich_text', [{}])[0].get('text', {}).get('content', '')
            
            if not salted_sha512_hash:
                print(f"Warning: Could not retrieve salted_sha512_hash for file_page_id: {file_page_id}. Skipping Global File Index deletion.")

            # Notion's delete is an archive operation
            payload = {"archived": True}
            response = requests.patch(url, json=payload, headers=headers)
            
            if response.status_code != 200:
                raise Exception(f"Failed to delete (archive) file from user database: {response.text}")
            
            # If hash was found, delete from Global File Index as well
            if salted_sha512_hash:
                self.delete_file_from_global_index(salted_sha512_hash)
                
            return response.json()
        except Exception as e:
            print(f"Error deleting file from database {file_page_id}: {e}")
            raise

    def delete_file_from_global_index(self, salted_sha512_hash: str) -> Optional[Dict[str, Any]]:
        """Deletes an entry from the Global File Index database based on the salted SHA512 hash."""
        global_index_db_id = self.global_file_index_db_id
        if not global_index_db_id:
            print("GLOBAL_FILE_INDEX_DB_ID not set. Cannot delete from Global File Index.")
            return None

        # 1. Query the Global File Index for the hash to get the page ID
        index_query_url = f"{self.base_url}/databases/{global_index_db_id}/query"
        index_payload = {
            "filter": {
                "property": "Salted SHA512 Hash",
                "rich_text": {
                    "equals": salted_sha512_hash
                }
            }
        }
        headers = {**self.headers, "Content-Type": "application/json"}

        try:
            index_response = requests.post(index_query_url, json=index_payload, headers=headers)
            if index_response.status_code != 200:
                print(f"Failed to query Global File Index for deletion: {index_response.text}")
                return None

            index_results = index_response.json().get('results', [])
            if not index_results:
                print(f"No entry found in Global File Index for hash: {salted_sha512_hash}. Nothing to delete.")
                return None

            # Get the page ID of the entry to be deleted
            index_entry_id = index_results[0]['id']
            
            # 2. Archive (delete) the page in the Global File Index
            delete_url = f"{self.base_url}/pages/{index_entry_id}"
            delete_payload = {"archived": True}
            delete_response = requests.patch(delete_url, json=delete_payload, headers=headers)

            if delete_response.status_code != 200:
                raise Exception(f"Failed to delete (archive) entry from Global File Index: {delete_response.text}")
            
            print(f"Successfully deleted entry from Global File Index for hash: {salted_sha512_hash}")
            return delete_response.json()

        except Exception as e:
            print(f"Error deleting file from Global File Index by hash {salted_sha512_hash}: {e}")
            raise

    def add_file_to_user_database(self, database_id: str, filename: str, file_size: int, file_hash: str, file_upload_id: str, is_public: bool = False, salt: str = "") -> Dict[str, Any]:
        """Add a file entry to a user's Notion database"""
        url = f"{self.base_url}/pages"

        # Create a new page in the database with file information
        payload = {
            "parent": {"database_id": database_id},
            "properties": {
                "filename": {
                    "title": [
                        {
                            "text": {
                                "content": filename
                            }
                        }
                    ]
                },
                "filesize": {
                    "number": file_size
                },
                "filehash": {
                    "rich_text": [
                        {
                            "text": {
                                "content": file_hash
                            }
                        }
                    ]
                },
                "file": {
                    "files": [
                        {
                            "name": filename,
                            "type": "file_upload",
                            "file_upload": {
                                "id": file_upload_id
                            }
                        }
                    ]
                },
                "is_public": {
                    "checkbox": is_public
                },
                "salt": {
                    "rich_text": [
                        {
                            "text": {
                                "content": salt
                            }
                        }
                    ]
                }
            }
        }

        headers = {**self.headers, "Content-Type": "application/json"}

        response = requests.post(url, json=payload, headers=headers)

        if response.status_code != 200:
            raise Exception(f"Failed to add file to user database: {response.text}")

        return response.json()

    def get_file_by_salted_sha512_hash(self, salted_sha512_hash: str) -> Optional[Dict[str, Any]]:
        """
        Queries the Global File Index database for a file by its salted SHA512 hash,
        then fetches the full file details from the user's specific database.
        """
        global_index_db_id = self.global_file_index_db_id
        if not global_index_db_id:
            print("GLOBAL_FILE_INDEX_DB_ID not set in NotionFileUploader instance. Cannot query file index.")
            return None

        # 1. Query the Global File Index for the hash
        index_query_url = f"{self.base_url}/databases/{global_index_db_id}/query"
        index_payload = {
            "filter": {
                "property": "Salted SHA512 Hash",
                "rich_text": {
                    "equals": salted_sha512_hash
                }
            }
        }
        headers = {**self.headers, "Content-Type": "application/json"}

        try:
            index_response = requests.post(index_query_url, json=index_payload, headers=headers)
            if index_response.status_code != 200:
                print(f"Failed to query Global File Index: {index_response.text}")
                return None

            index_results = index_response.json().get('results', [])
            if not index_results:
                return None # Hash not found in index

            # Get the first matching entry from the index
            index_entry = index_results[0]
            properties = index_entry.get('properties', {})

            file_page_id = properties.get('File Page ID', {}).get('rich_text', [{}])[0].get('text', {}).get('content', '')
            user_database_id = properties.get('User Database ID', {}).get('rich_text', [{}])[0].get('text', {}).get('content', '')
            
            if not file_page_id or not user_database_id:
                print("Missing File Page ID or User Database ID in Global File Index entry.")
                return None

            # Instead of fetching the full file details, return the index entry itself
            # The caller (app.py) will then extract file_page_id and user_database_id
            return index_entry

        except Exception as e:
            print(f"Error getting file by salted SHA512 hash from index: {e}")
            return None

    def update_file_public_status(self, file_id: str, is_public: bool, salted_sha512_hash: str = None) -> Dict[str, Any]:
        """
        Updates the 'is_public' property of a file entry in the user's Notion database
        and optionally in the Global File Index.
        """
        url = f"{self.base_url}/pages/{file_id}"
        payload = {
            "properties": {
                "is_public": {
                    "checkbox": is_public
                }
            }
        }
        headers = {**self.headers, "Content-Type": "application/json"}

        try:
            response = requests.patch(url, json=payload, headers=headers)
            if response.status_code != 200:
                raise Exception(f"Failed to update file public status in user database: {response.text}")
            
            # Also update in Global File Index if hash is provided
            if salted_sha512_hash:
                global_index_db_id = self.global_file_index_db_id
                if global_index_db_id:
                    # Find the entry in the global index by hash
                    index_query_url = f"{self.base_url}/databases/{global_index_db_id}/query"
                    index_payload = {
                        "filter": {
                            "property": "Salted SHA512 Hash",
                            "rich_text": {
                                "equals": salted_sha512_hash
                            }
                        }
                    }
                    index_response = requests.post(index_query_url, json=index_payload, headers=headers)
                    if index_response.status_code == 200 and index_response.json().get('results'):
                        index_entry_id = index_response.json()['results'][0]['id']
                        index_update_url = f"{self.base_url}/pages/{index_entry_id}"
                        index_update_payload = {
                            "properties": {
                                "Is Public": {
                                    "checkbox": is_public
                                }
                            }
                        }
                        requests.patch(index_update_url, json=index_update_payload, headers=headers)
                        print(f"Updated is_public status in Global File Index for hash: {salted_sha512_hash}")
                    else:
                        print(f"Warning: Could not find entry in Global File Index for hash: {salted_sha512_hash} to update public status.")
                else:
                    print("Warning: GLOBAL_FILE_INDEX_DB_ID not set in NotionFileUploader instance. Cannot update public status in Global File Index.")

            return response.json()
        except Exception as e:
            print(f"Error updating file public status: {e}")
            raise

    def stream_file_from_notion(self, notion_download_url: str) -> Iterable[bytes]:
        """
        Streams file content from a Notion signed download URL.
        """
        try:
            with requests.get(notion_download_url, stream=True) as r:
                r.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
                for chunk in r.iter_content(chunk_size=8192): # 8KB chunks
                    yield chunk
        except requests.exceptions.RequestException as e:
            print(f"Error streaming file from Notion: {e}")
            raise Exception(f"Failed to stream file from Notion: {e}")

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit handler"""
        pass

    def add_file_to_index(self, salted_sha512_hash: str, file_page_id: str, user_database_id: str, original_filename: str, is_public: bool) -> Dict[str, Any]:
        """Adds an entry to the Global File Index database."""
        global_index_db_id = self.global_file_index_db_id
        if not global_index_db_id:
            raise Exception("GLOBAL_FILE_INDEX_DB_ID not set in NotionFileUploader instance. Global File Index database must be created manually.")

        url = f"{self.base_url}/pages"

        payload = {
            "parent": {"database_id": global_index_db_id},
            "properties": {
                "Salted SHA512 Hash": {
                    "rich_text": [{"text": {"content": salted_sha512_hash}}]
                },
                "File Page ID": {
                    "rich_text": [{"text": {"content": file_page_id}}]
                },
                "User Database ID": {
                    "rich_text": [{"text": {"content": user_database_id}}]
                },
                "Original Filename": {
                    "title": [{"text": {"content": original_filename}}]
                },
                "Is Public": {
                    "checkbox": is_public
                }
            }
        }

        headers = {**self.headers, "Content-Type": "application/json"}

        try:
            response = requests.post(url, json=payload, headers=headers)
            if response.status_code != 200:
                raise Exception(f"Failed to add file to Global File Index: {response.text}")
            return response.json()
        except Exception as e:
            print(f"Error adding file to Global File Index: {e}")
            raise