import pytest

from unittest import mock
from google.cloud import datastore

from app.app import app as flask_app

from models.config_model import Config
from models.interviewer_call_pattern_model import InterviewerCallPattern
from tests.helpers.interviewer_call_pattern_helpers import interviewer_call_pattern_report_sample_case, datetime_helper


@pytest.fixture
def config():
    return Config(
        mysql_host="blah",
        mysql_user="blah",
        mysql_password="blah",
        mysql_database="blah",
        blaise_api_url="blah",
        nifi_staging_bucket="blah",
        deliver_mi_hub_reports_task_queue_id="blah",
        gcloud_project="blah",
        region="blah",
        cloud_function_sa="cloud_function_sa_mock"
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
def start_date_as_string():
    return "2021-09-22"


@pytest.fixture
def end_date_as_string():
    return "2021-09-22"


@pytest.fixture
def survey_tla():
    return "OPN"


@pytest.fixture
def webnudge_outcome_code():
    return "120"


@pytest.fixture
def arbitrary_outcome_code():
    return "999"


@pytest.fixture
def invalid_date():
    return "blah"


@pytest.fixture
def call_history_records_status_sample():
    return [
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=10),
            call_end_time=datetime_helper(day=7, hour=11),
            dial_secs=1800,
            status="WebNudge",
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=10),
            call_end_time=datetime_helper(day=7, hour=11),
            dial_secs=1800,
            status="Completed",
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=10),
            call_end_time=datetime_helper(day=7, hour=11),
            dial_secs=1800,
            status="Finished (No contact)",
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=10),
            call_end_time=datetime_helper(day=7, hour=11),
            dial_secs=1800,
            status="Finished (Non response)",
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=10),
            call_end_time=datetime_helper(day=7, hour=11),
            dial_secs=1800,
            status="Finished (Appointment made)",
        )]


@pytest.fixture
def interviewer_call_pattern_report():
    return InterviewerCallPattern(
        total_valid_cases=10,
        hours_worked="7:24:00",
        call_time="0:00:00",
        hours_on_calls_percentage=0,
        average_calls_per_hour=3.14,
        refusals=1,
        no_contacts=2,
        no_contact_answer_service=3,
        no_contact_busy=4,
        no_contact_disconnect=5,
        no_contact_no_answer=6,
        no_contact_other=7,
        completed_successfully=8,
        appointments_for_contacts=9,
        web_nudge=10,
        invalid_fields="n/a",
    )


class RecordsInDatastore:
    def __init__(self, list_of_records):
        self.list_of_records = list_of_records
        self.datastore_client = datastore.Client()
        self.keys = []

    def __enter__(self):
        kind = "CallHistory"

        for record in self.list_of_records:
            entity_key = self.datastore_client.key(kind, record["name"])
            entity = datastore.Entity(key=entity_key)
            del record['name']
            entity.update(record)
            self.datastore_client.put(entity)
            self.keys.append(entity_key)

    def __exit__(self, exc_type, exc_val, exc_tb):
        print("Deleting keys", self.keys)
        self.datastore_client.delete_multi(self.keys)


@pytest.fixture()
def records_in_datastore():
    return RecordsInDatastore
