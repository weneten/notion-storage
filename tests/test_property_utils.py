import pytest
import sys
from pathlib import Path
repo_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(repo_root))

def safe_get_property_text(prop: dict, key: str = 'rich_text', default: str = '') -> str:
    try:
        values = prop.get(key, [])
        if isinstance(values, list) and values:
            return values[0].get('text', {}).get('content', default)
    except Exception:
        pass
    return default


def test_safe_get_property_text_empty_list():
    prop = {'rich_text': []}
    assert safe_get_property_text(prop) == ''


def test_safe_get_property_text_missing_key():
    assert safe_get_property_text({}) == ''


def test_safe_get_property_text_title():
    prop = {'title': [{'text': {'content': 'hello'}}]}
    assert safe_get_property_text(prop, key='title') == 'hello'
