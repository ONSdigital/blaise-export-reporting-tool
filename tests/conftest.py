import datetime
import pytest
from unittest import mock

import pandas as pd
import numpy as np

from google.api_core.datetime_helpers import DatetimeWithNanoseconds

from app.app import app as flask_app
from models.config_model import Config
from models.interviewer_call_pattern_model import InterviewerCallPattern


@pytest.fixture
def config():
    return Config(
        mysql_host="blah",
        mysql_user="blah",
        mysql_password="blah",
        mysql_database="blah",
        blaise_api_url="blah",
        nifi_staging_bucket="blah"
    )


@pytest.fixture
def app():
    yield flask_app


@pytest.fixture
def client(app):
    app.call_history_client = mock.MagicMock()
    return app.test_client()


@pytest.fixture
def api_installed_questionnaires_response():
    return [
        {
            "name": "DST2106X",
            "id": "12345-12345-12345-12345-XXXXX",
            "serverParkName": "gusty",
            "installDate": "2021-01-01T01:01:01.99999+01:00",
            "status": "Active",
            "dataRecordCount": 1337,
            "hasData": True,
            "nodes": [
                {
                    "nodeName": "blaise-gusty-mgmt",
                    "nodeStatus": "Active"
                },
                {
                    "nodeName": "blaise-gusty-data-entry-1",
                    "nodeStatus": "Active"
                },
                {
                    "nodeName": "blaise-gusty-data-entry-2",
                    "nodeStatus": "Active"
                }
            ]
        },
        {
            "name": "DST2106Y",
            "id": "12345-12345-12345-12345-YYYYY",
            "serverParkName": "gusty",
            "installDate": "2021-01-01T01:01:01.99999+01:00",
            "status": "Active",
            "dataRecordCount": 42,
            "hasData": True,
            "nodes": [
                {
                    "nodeName": "blaise-gusty-mgmt",
                    "nodeStatus": "Active"
                },
                {
                    "nodeName": "blaise-gusty-data-entry-1",
                    "nodeStatus": "Active"
                },
                {
                    "nodeName": "blaise-gusty-data-entry-2",
                    "nodeStatus": "Active"
                }
            ]
        },
        {
            "name": "DST2106Z",
            "id": "12345-12345-12345-12345-ZZZZZ",
            "serverParkName": "gusty",
            "installDate": "2021-01-01T01:01:01.99999+01:00",
            "status": "Active",
            "dataRecordCount": 999,
            "hasData": True,
            "nodes": [
                {
                    "nodeName": "blaise-gusty-mgmt",
                    "nodeStatus": "Active"
                },
                {
                    "nodeName": "blaise-gusty-data-entry-1",
                    "nodeStatus": "Active"
                },
                {
                    "nodeName": "blaise-gusty-data-entry-2",
                    "nodeStatus": "Active"
                }
            ]
        }
    ]


@pytest.fixture
def questionnaire_name():
    return "DST2106Z"


@pytest.fixture
def questionnaire_fields_to_get():
    return [
        "QID.Serial_Number",
        "QHAdmin.HOut"
    ]


@pytest.fixture
def api_reporting_data_response():
    return {
        "instrumentName": "DST2106Z",
        "instrumentId": "12345-12345-12345-12345-12345",
        "reportingData": [
            {
                "qiD.Serial_Number": "10010",
                "qhAdmin.HOut": "110"
            },
            {
                "qiD.Serial_Number": "10020",
                "qhAdmin.HOut": "110"
            },
            {
                "qiD.Serial_Number": "10030",
                "qhAdmin.HOut": "110"

            }
        ]
    }


@pytest.fixture
def interviewer_name():
    return "ricer"


@pytest.fixture
def invalid_date():
    return "blah"


@pytest.fixture
def interviewer_call_pattern_report():
    return InterviewerCallPattern(
        total_records=10,
        hours_worked="7:24:00",
        call_time="0:00:00",
        hours_on_calls_percentage="0%",
        average_calls_per_hour=3.14,
        refusals="foobar",
        no_contacts="foobar",
        no_contact_answer_service="foobar",
        no_contact_busy="foobar",
        no_contact_disconnect="foobar",
        no_contact_no_answer="foobar",
        no_contact_other="foobar",
        completed_successfully="",
        appointments_for_contacts="100/100, 100%",
        discounted_invalid_cases="0",
        invalid_fields="n/a",
    )
