import datetime
import pandas as pd
import pytest
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
def start_date_string():
    return "2021-01-01"


@pytest.fixture
def end_date_string():
    return "2022-01-01"


@pytest.fixture
def invalid_date():
    return "blah"


@pytest.fixture
def app():
    yield flask_app


@pytest.fixture
def client(app):
    return app.test_client()


@pytest.fixture
def call_history_dataframe():
    results = [
        {
            'appointment_info': None,
            'busy_dials': 0,
            'call_end_time': DatetimeWithNanoseconds(2021, 5, 19, 12, 33, 21, tzinfo=datetime.timezone.utc),
            'call_number': 1,
            'call_result': 'NonRespons',
            'call_start_time': DatetimeWithNanoseconds(2021, 5, 19, 12, 32, 16, tzinfo=datetime.timezone.utc),
            'cohort': 'AA',
            'dial_number': 1,
            'dial_secs': 65,
            'interviewer': 'matpal',
            'number_of_interviews': 1,
            'outcome_code': '542',
            'questionnaire_id': '05cf69af-1a4e-47df-819a-928350fdda5a',
            'questionnaire_name': 'LMS2101_AA1',
            'serial_number': '1001081',
            'status': 'Timed out during questionnaire',
            'survey': 'LMS',
            'update_info': None,
            'wave': 1
        },
        {
            'appointment_info': None,
            'busy_dials': 0,
            'call_end_time': DatetimeWithNanoseconds(2021, 5, 19, 14, 32, 43, tzinfo=datetime.timezone.utc),
            'call_number': 1,
            'call_result': 'Busy',
            'call_start_time': DatetimeWithNanoseconds(2021, 5, 19, 14, 32, 22, tzinfo=datetime.timezone.utc),
            'cohort': 'AA',
            'dial_number': 2,
            'dial_secs': 21,
            'interviewer': 'matpal',
            'number_of_interviews': 1,
            'outcome_code': '561',
            'questionnaire_id': '05cf69af-2a4e-47df-819a-928350fdda5a',
            'questionnaire_name': 'LMS2101_AA1',
            'serial_number': '1001021',
            'status': 'Finished (No contact)',
            'survey': 'LMS',
            'update_info': None,
            'wave': 1
        },
        {
            'appointment_info': None,
            'busy_dials': 1,
            'call_end_time': DatetimeWithNanoseconds(2021, 5, 19, 14, 33, 40, tzinfo=datetime.timezone.utc),
            'call_number': 1,
            'call_result': 'NonRespons',
            'call_start_time': DatetimeWithNanoseconds(2021, 5, 19, 14, 33, 11, tzinfo=datetime.timezone.utc),
            'cohort': 'AA',
            'dial_number': 2,
            'dial_secs': 29,
            'interviewer': 'matpal',
            'number_of_interviews': 1,
            'outcome_code': '561',
            'questionnaire_id': '05cf69af-3a4e-47df-819a-928350fdda5a',
            'questionnaire_name': 'LMS2101_AA1',
            'serial_number': '1001021',
            'status': 'Finished (Non response)',
            'survey': 'LMS',
            'update_info': None,
            'wave': 1
        },
        {
            'appointment_info': None,
            'busy_dials': 0,
            'call_end_time': DatetimeWithNanoseconds(2021, 5, 19, 14, 35, 34, tzinfo=datetime.timezone.utc),
            'call_number': 1,
            'call_result': 'Busy',
            'call_start_time': DatetimeWithNanoseconds(2021, 5, 19, 14, 35, 29, tzinfo=datetime.timezone.utc),
            'cohort': 'AA',
            'dial_number': 3,
            'dial_secs': 5,
            'interviewer': 'matpal',
            'number_of_interviews': 1,
            'outcome_code': '561',
            'questionnaire_id': '05cf69af-3a4e-47df-819a-928350fdda5a',
            'questionnaire_name': 'LMS2101_AA1',
            'serial_number': '1001021',
            'status': 'Finished (No contact)',
            'survey': 'LMS',
            'update_info': None,
            'wave': 1
        },
        {
            'appointment_info': None,
            'busy_dials': 1,
            'call_end_time': DatetimeWithNanoseconds(2021, 5, 19, 14, 36, 8, tzinfo=datetime.timezone.utc),
            'call_number': 1,
            'call_result': 'NoAnswer',
            'call_start_time': DatetimeWithNanoseconds(2021, 5, 19, 14, 36, tzinfo=datetime.timezone.utc),
            'cohort': 'AA',
            'dial_number': 3,
            'dial_secs': 8,
            'interviewer': 'matpal',
            'number_of_interviews': 1,
            'outcome_code': '561',
            'questionnaire_id': '05cf69af-4a4e-47df-819a-928350fdda5a',
            'questionnaire_name': 'LMS2101_AA1',
            'serial_number': '1001021',
            'status': 'Finished (No contact)',
            'survey': 'LMS',
            'update_info': None,
            'wave': 1
        },
        {
            'appointment_info': None,
            'busy_dials': 0,
            'call_end_time': DatetimeWithNanoseconds(2021, 5, 19, 14, 49, 55, tzinfo=datetime.timezone.utc),
            'call_number': 1,
            'call_result': 'NonRespons',
            'call_start_time': DatetimeWithNanoseconds(2021, 5, 19, 14, 49, 40, tzinfo=datetime.timezone.utc),
            'cohort': 'AA',
            'dial_number': 4,
            'dial_secs': 15,
            'interviewer': 'matpal',
            'number_of_interviews': 1,
            'outcome_code': '561',
            'questionnaire_id': '05cf69af-4a4e-47df-819a-928350fdda5a',
            'questionnaire_name': 'LMS2101_AA1',
            'serial_number': '1001021',
            'status': "You've been Numberwanged!",
            'survey': 'LMS',
            'update_info': None,
            'wave': 1
        },
        {
            'appointment_info': None,
            'busy_dials': 0,
            'call_end_time': DatetimeWithNanoseconds(2021, 5, 19, 14, 59, 17, tzinfo=datetime.timezone.utc),
            'call_number': 1,
            'call_result': 'NonRespons',
            'call_start_time': DatetimeWithNanoseconds(2021, 5, 19, 14, 59, 1, tzinfo=datetime.timezone.utc),
            'cohort': 'AA',
            'dial_number': 1,
            'dial_secs': 16,
            'interviewer': 'matpal',
            'number_of_interviews': 1,
            'outcome_code': '460',
            'questionnaire_id': '05cf69af-4a4e-47df-819a-928350fdda5a',
            'questionnaire_name': 'LMS2101_AA1',
            'serial_number': '1001011',
            'status': 'Finished (Successful)',
            'survey': 'LMS',
            'update_info': None,
            'wave': 1
        },
        {
            'appointment_info': None,
            'busy_dials': 0,
            'call_end_time': DatetimeWithNanoseconds(2021, 5, 19, 15, 0, 13, tzinfo=datetime.timezone.utc),
            'call_number': 1,
            'call_result': 'NoAnswer',
            'call_start_time': DatetimeWithNanoseconds(2021, 5, 19, 15, 0, 7, tzinfo=datetime.timezone.utc),
            'cohort': 'AA',
            'dial_number': 5,
            'dial_secs': 6,
            'interviewer': 'matpal',
            'number_of_interviews': 1,
            'outcome_code': '561',
            'questionnaire_id': '05cf69af-5a4e-47df-819a-928350fdda5a',
            'questionnaire_name': 'LMS2101_AA1',
            'serial_number': '1001021',
            'status': 'Finished (No contact)',
            'survey': 'LMS',
            'update_info': None,
            'wave': 1
        },
    ]
    return pd.DataFrame(results)


@pytest.fixture
def invalid_call_history_dataframe():
    results = [
        {
            'appointment_info': None,
            'busy_dials': 0,
            'call_end_time': DatetimeWithNanoseconds(2021, 5, 19, 12, 33, 21, tzinfo=datetime.timezone.utc),
            'call_number': 1,
            'call_result': 'NonRespons',
            'call_start_time': DatetimeWithNanoseconds(2021, 5, 19, 12, 32, 16, tzinfo=datetime.timezone.utc),
            'cohort': 'AA',
            'dial_number': 1,
            'dial_secs': 65,
            'interviewer': 'matpal',
            'number_of_interviews': 1,
            'outcome_code': '542',
            'questionnaire_id': '05cf69af-1a4e-47df-819a-928350fdda5a',
            'questionnaire_name': 'LMS2101_AA1',
            'serial_number': '1001081',
            'status': 'Finished (Non response)',
            'survey': 'LMS',
            'update_info': None,
            'wave': 1
        },
    ]
    return pd.DataFrame(results)


@pytest.fixture
def interviewer_call_pattern_report():
    return InterviewerCallPattern(
        hours_worked="7:24:00",
        call_time="0:00:00",
        hours_on_calls_percentage="0%",
        average_calls_per_hour=3.14,
        respondents_interviewed=5,
        households_completed_successfully=42,
        average_respondents_interviewed_per_hour=123,
        no_contacts_percentage="0%",
        appointments_for_contacts_percentage="101%",
        discounted_invalid_records="0",
        invalid_fields="n/a",
    )


@pytest.fixture
def call_history_records():
    return [
        {
            'appointment_info': None,
            'busy_dials': 0,
            'call_end_time': DatetimeWithNanoseconds(2021, 8, 7, 1, 45, 18, tzinfo=datetime.timezone.utc),
            'call_number': 2,
            'call_result': 'NoAnswer',
            'call_start_time': DatetimeWithNanoseconds(2021, 8, 7, 1, 44, 26, tzinfo=datetime.timezone.utc),
            'cohort': None,
            'dial_number': 1,
            'dial_secs': 52,
            'interviewer': 'el4president',
            'number_of_interviews': 1,
            'outcome_code': 310,
            'questionnaire_id': 'remember-24-01-9dc791f0cb07',
            'questionnaire_name': 'OPN2108R',
            'serial_number': 24012022,
            'status': 'Finished (No contact)',
            'survey': 'OPN',
            'update_info': None,
            'wave': None
        },
        {
            'appointment_info': None,
            'busy_dials': 0,
            'call_end_time': DatetimeWithNanoseconds(2021, 8, 7, 1, 56, 35, tzinfo=datetime.timezone.utc),
            'call_number': 2,
            'call_result': 'NoAnswer',
            'call_start_time': DatetimeWithNanoseconds(2021, 8, 7, 1, 55, 39, tzinfo=datetime.timezone.utc),
            'cohort': None,
            'dial_number': 1,
            'dial_secs': 56,
            'interviewer': 'el4president',
            'number_of_interviews': 1,
            'outcome_code': 310,
            'questionnaire_id': 'remember-24-01-9dc791f0cb07',
            'questionnaire_name': 'OPN2108R',
            'serial_number': 24012022,
            'status': 'Finished (No contact)',
            'survey': 'OPN',
            'update_info': None,
            'wave': None
        },
        {
            'appointment_info': None,
            'busy_dials': 0,
            'call_end_time': DatetimeWithNanoseconds(2021, 8, 7, 1, 58, 18, tzinfo=datetime.timezone.utc),
            'call_number': 2,
            'call_result': 'NoAnswer',
            'call_start_time': DatetimeWithNanoseconds(2021, 8, 7, 1, 57, 15, tzinfo=datetime.timezone.utc),
            'cohort': None,
            'dial_number': 1,
            'dial_secs': 63,
            'interviewer': 'el4president',
            'number_of_interviews': 1,
            'outcome_code': 310,
            'questionnaire_id': 'remember-24-01-9dc791f0cb07',
            'questionnaire_name': 'OPN2108R',
            'serial_number': 24012022,
            'status': 'Finished (No contact)',
            'survey': 'OPN',
            'update_info': None,
            'wave': None
        },
    ]