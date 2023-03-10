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
        "NIFI_STAGING_BUCKET": "nifi_staging_bucket_mock",
    },
)
@patch("data_sources.call_history_data.get_cati_call_history_from_database")
def test_get_cati_call_history(mock_get_cati_call_history_from_database):
    # Arrange
    mock_get_cati_call_history_from_database.return_value = [
        {
            "InstrumentName": "OPN2101A",
            "PrimaryKeyValue": "1001011",
            "CallNumber": 1,
            "DialNumber": 1,
            "BusyDials": 0,
            "StartTime": "2021/05/19 14:59:01",
            "EndTime": "2021/05/19 14:59:17",
            "DialSecs": 16,
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
    dial_history = call_history_client.get_cati_call_history()

    # Assertion
    assert len(dial_history) == 1
    assert dial_history == [
        CallHistory(
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


@mock.patch.dict(
    os.environ,
    {
        "MYSQL_HOST": "mysql_host_mock",
        "MYSQL_USER": "mysql_user_mock",
        "MYSQL_PASSWORD": "mysql_password_mock",
        "MYSQL_DATABASE": "mysql_database_mock",
        "BLAISE_API_URL": "blaise_api_url_mock",
        "NIFI_STAGING_BUCKET": "nifi_staging_bucket_mock",
    },
)
@patch.object(CallHistoryClient, "get_call_history_keys")
def test_filter_out_existing_call_history_records(
    mock_call_history_keys,
):
    mock_call_history_keys.return_value = {"1001011-2021/05/19 14:59:01": None}

    call_history_data = [
        CallHistory(
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
    config = Config.from_env()

    # Execution
    call_history_client = CallHistoryClient(mock.MagicMock, config)
    assert (
        len(
            call_history_client.filter_out_existing_call_history_records(
                call_history_data
            )
        )
        == 0
    )
