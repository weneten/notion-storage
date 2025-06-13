import os
import sys

# Get the path of the application's root directory
project_root = os.path.dirname(__file__)

# Add the 'vendor' subdirectory to the Python path
# This makes Python look for libraries inside the 'vendor' folder
vendor_path = os.path.join(project_root, 'vendor')
if vendor_path not in sys.path:
    sys.path.insert(0, vendor_path)

# Add the project root to the path, which helps find app.py
if project_root not in sys.path:
    sys.path.append(project_root)

# Import the 'app' object from your 'app.py' file
# The original sys.path.append line is no longer needed
from app import app as application

# Set a secret key as required by the example
# Note: Your app already sets this, but we'll keep it for compatibility.
application.secret_key = os.environ.get('SECRET_KEY', 'a_default_secret_key_for_wsgi') 