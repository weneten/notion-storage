import os
import sys

_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from app import app, _get_folder_from_request


def test_get_folder_from_request_preserves_semicolons():
    with app.test_request_context('/?folder=/series/Steins;Gate'):
        assert _get_folder_from_request() == '/series/Steins;Gate'


def test_get_folder_from_request_default_root():
    with app.test_request_context('/'):
        assert _get_folder_from_request() == '/'
