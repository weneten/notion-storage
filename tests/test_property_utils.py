import pytest
from app import safe_get_property_text


def test_safe_get_property_text_empty_list():
    prop = {'rich_text': []}
    assert safe_get_property_text(prop) == ''


def test_safe_get_property_text_missing_key():
    assert safe_get_property_text({}) == ''


def test_safe_get_property_text_title():
    prop = {'title': [{'text': {'content': 'hello'}}]}
    assert safe_get_property_text(prop, key='title') == 'hello'
