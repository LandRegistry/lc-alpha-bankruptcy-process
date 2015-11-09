import pytest
from unittest import mock
import requests
import os
from application.routes import app
import json
from application.listener import message_received, BankruptcyProcessError, listen, get_complex_name_matches


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


complex_names = [
    {
        "name": "BORERBERG VISCOUNT",
        "number": "1066224"
    },
    {
        "name": "ORNMOUTH VISCOUNT",
        "number": "1066224"
    }
]

iopn_cplex = [
    {
        "prop_type": "Joint",
        "relevance": 1,
        "surname": "",
        "full_name": "Helenbury Baron",
        "sub_register": "C",
        "office": "Fictional Office",
        "forenames": "",
        "name_type": "Private",
        "title_number": "ZZ203"
    }
]

directory = os.path.dirname(__file__)
no_alias = json.loads(open(os.path.join(directory, 'data/50001.json'), 'r').read())
cplex_name = json.loads(open(os.path.join(directory, 'data/50002.json'), 'r').read())
no_alias_resp = FakeResponse(no_alias, status_code=200)

cplex_resp = FakeResponse(complex_names, status_code=200)
iopn_resp = FakeResponse(iopn_cplex, status_code=200)

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

    @mock.patch('kombu.Producer.publish')
    @mock.patch('requests.get', return_value=iopn_resp)
    @mock.patch('requests.post', return_value=cplex_resp)
    def test_complex_name(self, mp, mg, mp2):
        results = get_complex_name_matches('HRH King Stark')
        assert len(results) == 2
        assert results[0]['full_name'] == 'Helenbury Baron'


