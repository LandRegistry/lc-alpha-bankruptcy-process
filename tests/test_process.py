import pytest
from unittest import mock
import requests
import os
from application.routes import app
import json


class TestProcessor:
    def setup_method(self, method):
        self.app = app.test_client()

    def test_app_root(self):
        response = self.app.get('/')
        assert response.status_code == 200
