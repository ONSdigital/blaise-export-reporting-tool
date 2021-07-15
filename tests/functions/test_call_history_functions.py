import os
from unittest import mock
from unittest.mock import patch

from functions.call_history_functions import get_cati_call_history
from functions.call_history_functions import merge_cati_call_history_and_questionnaire_data
from models.call_history import CallHistory
from models.config import Config


@mock.patch.dict(
    os.environ,
    {
        "MYSQL_HOST": "mysql_host_mock",
        "MYSQL_USER": "mysql_user_mock",
        "MYSQL_PASSWORD": "mysql_password_mock",
        "MYSQL_DATABASE": "mysql_database_mock",
        "BLAISE_API_URL": "blaise_api_url_mock",
        "NIFI_STAGING_BUCKET": "nifi_staging_bucket_mock"
    },
)
@patch("functions.call_history_functions.get_cati_call_history_from_database")
def test_get_cati_call_history(mock_get_cati_call_history_from_database):
    # Setup
    questionnaire_list = [
        {"name": "OPN2101A", "id": "05cf69af-3a4e-47df-819a-928350fdda5a"}
    ]

    mock_get_cati_call_history_from_database.return_value = [
        {
            "InstrumentId": "05cf69af-3a4e-47df-819a-928350fdda5a",
            "PrimaryKeyValue": "1001011",
            "CallNumber": 1,
            "DialNumber": 1,
            "BusyDials": 0,
            "StartTime": "2021/05/19 14:59:01",
            "EndTime": "2021/05/19 14:59:17",
            "dial_secs": 16,
            "Status": "Finished (Non response)",
            "Interviewer": "matpal",
            "DialResult": "NonRespons",
            "UpdateInfo": None,
            "AppointmentInfo": None,
        }
    ]
    config = Config.from_env()

    # Execution
    dial_history = get_cati_call_history(config, questionnaire_list)

    # Assertion
    assert len(dial_history) == 1
    assert dial_history == [
        CallHistory(
            questionnaire_id="05cf69af-3a4e-47df-819a-928350fdda5a",
            serial_number="1001011",
            call_number=1,
            dial_number=1,
            busy_dials=0,
            call_start_time="2021/05/19 14:59:01",
            call_end_time="2021/05/19 14:59:17",
            dial_secs=16,
            status="Finished (Non response)",
            interviewer="matpal",
            call_result="NonRespons",
            update_info=None,
            appointment_info=None,
            questionnaire_name="OPN2101A",
            survey="OPN",
            wave=None,
            cohort=None,
            number_of_interviews=None,
            outcome_code=None,
        )
    ]


def test_merge_cati_call_history_and_questionnaire_data():
    call_history = [
        CallHistory(
            questionnaire_id="05cf69af-3a4e-47df-819a-928350fdda5a",
            serial_number="1001031",
            call_number=1,
            dial_number=1,
            busy_dials=0,
            call_start_time="2021-05-12 13:17:56.8191819",
            call_end_time="2021-05-12 13:18:06.1431819",
            dial_secs=9,
            status="Finished (No contact)",
            interviewer="Edwin",
            call_result="Busy",
            update_info=None,
            appointment_info=None,
            questionnaire_name="LMS2101_AA1",
            wave=1,
            cohort="AA",
            number_of_interviews=None,
            outcome_code=None,
            survey="LMS",
        )
    ]
    questionnaire_data = [
        {
            "qhAdmin.HOut": 310,
            "qHousehold.QHHold.HHSize": 2,
            "qiD.Serial_Number": "1001031",
            "questionnaire_name": "LMS2101_AA1",
        }
    ]
    merged_data = merge_cati_call_history_and_questionnaire_data(call_history, questionnaire_data)
    assert merged_data[0].outcome_code == 310
    assert merged_data[0].number_of_interviews == 2
    assert merged_data == [
        CallHistory(
            questionnaire_id="05cf69af-3a4e-47df-819a-928350fdda5a",
            serial_number="1001031",
            call_number=1,
            dial_number=1,
            busy_dials=0,
            call_start_time="2021-05-12 13:17:56.8191819",
            call_end_time="2021-05-12 13:18:06.1431819",
            dial_secs=9,
            status="Finished (No contact)",
            interviewer="Edwin",
            call_result="Busy",
            update_info=None,
            appointment_info=None,
            questionnaire_name="LMS2101_AA1",
            survey="LMS",
            wave=1,
            cohort="AA",
            number_of_interviews=2,
            outcome_code=310,
        )
    ]
