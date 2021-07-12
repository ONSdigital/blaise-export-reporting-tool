import os
from unittest import mock
from unittest.mock import patch

from extract_call_history import load_cati_dial_history
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
@patch("extract_call_history.get_call_history")
def test_load_cati_dial_history(mock_get_call_history):
    # Setup
    questionnaire_list = [
        {"name": "OPN2101A", "id": "05cf69af-3a4e-47df-819a-928350fdda5a"}
    ]

    mock_get_call_history.return_value = [
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
    dial_history = load_cati_dial_history(config, questionnaire_list)

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
