import pytest
from unittest import mock
import requests
import os
import json
from application.process import get_complex_name_matches


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


    pass

