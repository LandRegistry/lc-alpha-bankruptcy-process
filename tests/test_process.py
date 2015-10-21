import pytest
from unittest import mock
import requests
import os
from application.routes import app
import json
from application.listener import message_received, BankruptcyProcessError, listen


class FakeConnection(object):
    def drain_events(self):
        pass


class FakeConnectionWithKeyboardInterruptException(object):
    def drain_events(self):
        raise KeyboardInterrupt("keyboard interrupt")


class FakeConnectionWithBankruptcyProcessError(object):
    def drain_events(self):
        raise BankruptcyProcessError([{'error': 'Bankruptcy Error'}])


class FakePublisher(object):
    def __init__(self):
        self.data = {}

    def put(self, data):
        self.data = data


class FakeResponse(requests.Response):
    def __init__(self, content=None, status_code=200):
        super(FakeResponse, self).__init__()
        self.data = content
        self.status_code = status_code

    def json(self):
        return self.data


class FakeMessage(object):
    def ack(self):
        pass


directory = os.path.dirname(__file__)
no_alias = json.loads(open(os.path.join(directory, 'data/50001.json'), 'r').read())

no_alias_resp = FakeResponse(no_alias, status_code=200)


class TestProcessor:

    def setup_method(self, method):
        self.app = app.test_client()

    def test_app_root(self):
        response = self.app.get('/')
        assert response.status_code == 200

    def test_health_check(self):
        response = self.app.get("/health")
        assert response.status_code == 200

    @mock.patch('requests.get', return_value=no_alias_resp)
    @mock.patch('requests.post', return_value=FakeResponse())
    def test_message_received(self, mock_post, mock_get):
        message_received([50000], FakeMessage())

    @mock.patch('kombu.Producer.publish')
    def test_listener_run_forever(self, mock_publish):
        fake_producer = FakePublisher()
        listen(FakeConnection(), fake_producer, False)

    @mock.patch('kombu.Producer.publish')
    def test_listener_interupt_handled(self, mock_publish):
        fake_producer = FakePublisher()
        listen(FakeConnectionWithKeyboardInterruptException(), fake_producer, False)

    @mock.patch('kombu.Producer.publish')
    def test_listener_error_handled(self, mock_publish):
        fake_producer = FakePublisher()
        listen(FakeConnectionWithBankruptcyProcessError(), fake_producer, False)