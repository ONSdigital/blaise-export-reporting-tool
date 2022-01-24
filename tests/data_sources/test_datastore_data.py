import os
from unittest import mock
from unittest.mock import patch

import pytest

from data_sources.call_history_data import CallHistoryClient
from models.call_history_model import CallHistory
from models.config_model import Config


@pytest.mark.parametrize(
    "list_to_split, number_to_split_by, expected",
    [
        (
                [
                    "item",
                    "item",
                    "item",
                    "item",
                    "item",
                    "item",
                    "item",
                    "item",
                    "item",
                    "item",
                ],
                2,
                [2, 2, 2, 2, 2],
        ),
        (
                [
                    "item",
                    "item",
                    "item",
                    "item",
                    "item",
                    "item",
                    "item",
                    "item",
                    "item",
                    "item",
                    "item",
                    "item",
                    "item",
                    "item",
                    "item",
                    "item",
                    "item",
                    "item",
                    "item",
                    "item",
                ],
                5,
                [5, 5, 5, 5],
        ),
        (["item", "item", "item"], 2, [2, 1]),
        (["item", "item", "item"], 5, [3]),
    ],
)
def test_split_into_batches(list_to_split, number_to_split_by, expected):
    split_list = CallHistoryClient.split_into_batches(list_to_split, number_to_split_by)
    assert len(split_list) == len(expected)
    i = 0
    while i < len(split_list):
        assert len(split_list[i]) == expected[i]
        i += 1


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
@patch("data_sources.call_history_data.get_cati_call_history_from_database")
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
            "AdditionalData": None,
        }
    ]
    config = Config.from_env()

    # Execution
    call_history_client = CallHistoryClient(mock.MagicMock, config)
    dial_history = call_history_client.get_cati_call_history(questionnaire_list)

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
    ]\


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
@patch("data_sources.call_history_data.get_cati_call_history_from_database")
def test_get_cati_call_history_has_webnudged_case(mock_get_cati_call_history_from_database):
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
            "outcome_code": 110
        }
    ]
    config = Config.from_env()

    # Execution
    call_history_client = CallHistoryClient(mock.MagicMock, config)
    dial_history = call_history_client.get_cati_call_history(questionnaire_list)

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
            outcome_code=110,
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
    call_history_client = CallHistoryClient(mock.MagicMock, mock.MagicMock)
    merged_data = call_history_client.merge_cati_call_history_and_questionnaire_data(call_history, questionnaire_data)
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
