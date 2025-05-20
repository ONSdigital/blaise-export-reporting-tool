import time
from typing import List
from unittest import mock

import pytest
from google.cloud import datastore

from app.app import app as flask_app
from models.config_model import Config
from models.interviewer_call_pattern_model import InterviewerCallPattern
from models.mi_hub_call_history_model import MiHubCallHistory
from models.mi_hub_respondent_data_model import MiHubRespondentData
from tests.helpers.interviewer_call_pattern_helpers import (
    datetime_helper,
    interviewer_call_pattern_report_sample_case,
)


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
        cloud_function_sa="cloud_function_sa_mock",
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
                {"nodeName": "blaise-gusty-mgmt", "nodeStatus": "Active"},
                {"nodeName": "blaise-gusty-data-entry-1", "nodeStatus": "Active"},
                {"nodeName": "blaise-gusty-data-entry-2", "nodeStatus": "Active"},
            ],
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
                {"nodeName": "blaise-gusty-mgmt", "nodeStatus": "Active"},
                {"nodeName": "blaise-gusty-data-entry-1", "nodeStatus": "Active"},
                {"nodeName": "blaise-gusty-data-entry-2", "nodeStatus": "Active"},
            ],
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
                {"nodeName": "blaise-gusty-mgmt", "nodeStatus": "Active"},
                {"nodeName": "blaise-gusty-data-entry-1", "nodeStatus": "Active"},
                {"nodeName": "blaise-gusty-data-entry-2", "nodeStatus": "Active"},
            ],
        },
    ]


@pytest.fixture
def questionnaire_name():
    return "DST2106Z"


@pytest.fixture
def questionnaire_fields_to_get():
    return ["QID.Serial_Number", "QHAdmin.HOut"]


@pytest.fixture
def api_reporting_data_response():
    return {
        "instrumentName": "DST2106Z",
        "instrumentId": "12345-12345-12345-12345-12345",
        "reportingData": [
            {"qiD.Serial_Number": "10010", "qhAdmin.HOut": "110"},
            {"qiD.Serial_Number": "10020", "qhAdmin.HOut": "110"},
            {"qiD.Serial_Number": "10030", "qhAdmin.HOut": "110"},
        ],
    }


@pytest.fixture
def api_reporting_data_response_empty():
    return {
        "instrumentName": "",
        "instrumentId": "",
        "reportingData": [],
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
        ),
    ]


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


@pytest.fixture
def mock_mi_hub_call_history(questionnaire_name: str) -> List[MiHubCallHistory]:
    return [
        MiHubCallHistory(
            questionnaire_name="LMS2222Z",
            questionnaire_id="s0me-r7nd0m-gu1d",
            serial_number="900001",
            dial_date="20221017",
            dial_time="10:31:36",
            end_time="10:32:06",
            seconds_interview=30,
            call_number=1,
            dial_number=1,
            interviewer="thorne1",
            dial_line_number=None,
            outcome_code="461",
        ),
        MiHubCallHistory(
            questionnaire_name="LMS2222Z",
            questionnaire_id="s0me-r7nd0m-gu1d",
            serial_number="900002",
            dial_date="20221017",
            dial_time="10:33:36",
            end_time="10:34:36",
            seconds_interview=60,
            call_number=1,
            dial_number=1,
            interviewer="thorne1",
            dial_line_number=None,
            outcome_code="461",
        ),
        MiHubCallHistory(
            questionnaire_name="LMS2222Z",
            questionnaire_id="s0me-r7nd0m-gu1d",
            serial_number="900003",
            dial_date="20221017",
            dial_time="10:35:36",
            end_time="10:35:46",
            seconds_interview=10,
            call_number=1,
            dial_number=1,
            interviewer="thorne1",
            dial_line_number=None,
            outcome_code="461",
        ),
    ]


@pytest.fixture
def mock_mi_hub_respondent_data():
    return [
        MiHubRespondentData(
            serial_number="900001",
            outcome_code="310",
            date_completed="2-11-2022_9:20",
            interviewer="",
            mode="",
            postcode="PO57 2OD",
            gender="",
            date_of_birth="",
            age="",
            Case_ID="",
            ShiftNo="",
            Interv="",
            CaseNotes="",
            Flight1="",
            Flight2="",
            Flight3="",
            Flight4="",
            Flight5="",
            Flight6="",
            Flight7="",
            Flight8="",
            IntDate="",
            SelectionTime="",
            DMExitTime="",
            IntType="",
            SampInterval="",
            ShiftType="",
            Portroute="",
            Baseport="",
            Linecode="",
            FlightNum="",
            SelectFlight="",
            PortCode="",
            PortDestination="",
            Shuttle="",
            CrossShut="",
            Vehicle="",
            IsElig="",
            FerryTime="",
            Flow="",
            DVRespnse="",
            proportion="",
            response_visitbritain="",
            response_age_sex="",
            response_student="",
            response_fe_trailer="",
            response_migration_trailer="",
            DMTimeIsElig="",
            DMTimeAgeSex="",
            UKForeign="",
            StudyCheck="",
            Expenditure="",
            Age="",
            Sex="",
        ),
        MiHubRespondentData(
            serial_number="900002",
            outcome_code="461",
            date_completed="5-11-2022_9:20",
            interviewer="",
            mode="",
            postcode="PO57 2OD",
            gender="",
            date_of_birth="",
            age="",
            Case_ID="",
            ShiftNo="",
            Interv="",
            CaseNotes="",
            Flight1="",
            Flight2="",
            Flight3="",
            Flight4="",
            Flight5="",
            Flight6="",
            Flight7="",
            Flight8="",
            IntDate="",
            SelectionTime="",
            DMExitTime="",
            IntType="",
            SampInterval="",
            ShiftType="",
            Portroute="",
            Baseport="",
            Linecode="",
            FlightNum="",
            SelectFlight="",
            PortCode="",
            PortDestination="",
            Shuttle="",
            CrossShut="",
            Vehicle="",
            IsElig="",
            FerryTime="",
            Flow="",
            DVRespnse="",
            proportion="",
            response_visitbritain="",
            response_age_sex="",
            response_student="",
            response_fe_trailer="",
            response_migration_trailer="",
            DMTimeIsElig="",
            DMTimeAgeSex="",
            UKForeign="",
            StudyCheck="",
            Expenditure="",
            Age="",
            Sex="",
        ),
    ]


class RecordsInDatastore:
    DATASTORE_KIND = "CallHistory"
    MAX_RETRIES = 5
    RETRY_WAIT_SECONDS = 0.2

    def __init__(self, list_of_records):
        self.list_of_records = list_of_records
        self.datastore_client = datastore.Client()
        self.keys = []

    def __enter__(self):
        self._assert_datastore_is_empty()
        for record in self.list_of_records:
            self._add_record(record)

    def __exit__(self, exc_type, exc_val, exc_tb):
        print("Deleting keys", self.keys)
        self.datastore_client.delete_multi(self.keys)
        self._wait_for_datastore_to_be_empty()

    def _add_record(self, record):
        record_copy = record.copy()
        key = self.datastore_client.key(self.DATASTORE_KIND, record_copy["name"])
        entity = datastore.Entity(key=key)
        del record_copy["name"]
        entity.update(record_copy)
        self.datastore_client.put(entity)
        self.keys.append(key)
        self._wait_for_record_to_be_available(key)

    def _assert_datastore_is_empty(self):
        count = self._number_of_records_in_datastore()
        if count > 0:
            raise Exception(
                f"Expected datastore to be empty, found {count} entities. Try restarting the DataStore emulator."
            )

    def _wait_for_record_to_be_available(self, key):
        retries = self.MAX_RETRIES
        while self._entity_is_not_available(key):
            print(f"Waiting for record {key} to be available")
            time.sleep(self.RETRY_WAIT_SECONDS)
            retries -= 1
            if retries < 1:
                raise Exception(f"Record {key} never became available.")

    def _wait_for_datastore_to_be_empty(self):
        retries = self.MAX_RETRIES
        while self._number_of_records_in_datastore() > 0:
            print("Waiting for records to be deleted")
            time.sleep(self.RETRY_WAIT_SECONDS)
            retries -= 1
            if retries < 1:
                raise Exception(
                    "Failed to clear datastore. Try restarting the DataStore emulator."
                )

    def _entity_is_not_available(self, key):
        return self.datastore_client.get(key) is None

    def _number_of_records_in_datastore(self):
        query = self.datastore_client.query(kind=self.DATASTORE_KIND)
        count = len(list(query.fetch()))
        return count


@pytest.fixture()
def records_in_datastore():
    return RecordsInDatastore
