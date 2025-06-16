# Diagnostic logging to validate our assumptions
# Add these logs to app.py to confirm the issues

import logging
from flask import request

# Add this before existing routes
@app.before_request
def log_missing_endpoints():
    if request.endpoint is None:
        logging.error(f"DIAGNOSTIC: Missing endpoint called: {request.method} {request.path}")
        logging.error(f"DIAGNOSTIC: Request headers: {dict(request.headers)}")
        logging.error(f"DIAGNOSTIC: This will return HTML 404, causing JSON parse error")

# Add this to the streaming upload success section (around line 530)
def log_upload_completion():
    logging.info("DIAGNOSTIC: Upload completed successfully in Notion")
    logging.info("DIAGNOSTIC: Checking if file was saved to user database...")
    # Check if file exists in database
    user_database_id = uploader.get_user_database_id(current_user.id)
    if user_database_id:
        files_data = uploader.get_files_from_user_database(user_database_id)
        logging.info(f"DIAGNOSTIC: Files in user database: {len(files_data.get('results', []))}")
    else:
        logging.error("DIAGNOSTIC: User database ID not found!")

# Add this to validate frontend requests
@app.after_request
def log_response_type(response):
    if request.path in ['/delete_file', '/files-api', '/toggle_public_access']:
        logging.error(f"DIAGNOSTIC: Response to {request.path}: Status={response.status_code}, Content-Type={response.content_type}")
        if response.content_type and 'html' in response.content_type:
            logging.error("DIAGNOSTIC: CONFIRMED - Returning HTML instead of JSON!")
    return response