from unittest import TestCase, mock

from correlatr import response

import pytest

@pytest.mark.parametrize("message,error", [("foo", False), ("bar", False), ("hello world", True)])
def test_response(message, error):
    resp = response.create_response(message, error)
    assert resp.statusMessage.text == message and resp.statusMessage.error == error