import os
import sys

# IMPORTANT: You must edit this line.
# Replace 'YOUR_CPANEL_USERNAME' with your actual HelioHost username.
sys.path.append("/home/YOUR_CPANEL_USERNAME/httpdocs/pythonServer")

sys.path.insert(0, os.path.dirname(__file__))

# Import the 'app' object from your 'app.py' file
from app import app as application

# Set a secret key as required by the example
# Note: Your app already sets this, but we'll keep it for compatibility.
application.secret_key = os.environ.get('SECRET_KEY', 'a_default_secret_key_for_wsgi') 