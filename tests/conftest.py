import datetime
import pytest
import pandas as pd
from app.app import app as flask_app
from google.api_core.datetime_helpers import DatetimeWithNanoseconds


@pytest.fixture
def app():
    yield flask_app


@pytest.fixture
def client(app):
    return app.test_client()


@pytest.fixture
def mock_data():
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
            'questionnaire_id': '05cf69af-3a4e-47df-819a-928350fdda5a',
            'questionnaire_name': 'LMS2101_AA1',
            'serial_number': '1001081',
            'status': 'Finished (Non response)',
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
            'questionnaire_id': '05cf69af-3a4e-47df-819a-928350fdda5a',
            'questionnaire_name': 'LMS2101_AA1',
            'serial_number': '1001011',
            'status': 'Finished (Non response)',
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
            'questionnaire_id': '05cf69af-3a4e-47df-819a-928350fdda5a',
            'questionnaire_name': 'LMS2101_AA1',
            'serial_number': '1001021',
            'status': 'Finished (No contact)',
            'survey': 'LMS',
            'update_info': None,
            'wave': 1
        },
    ]
    return pd.DataFrame(results)


