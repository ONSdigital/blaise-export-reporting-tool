from datetime import datetime

import pytest

from models.mi_hub_call_history_model import MiHubCallHistory
from reports.mi_hub_call_history_report import get_mi_hub_call_history
from unittest import mock


@pytest.fixture
def mock_get_mi_hub_call_history_record():
    return {
        "InstrumentId": "1234",
        "PrimaryKeyValue": "9000001",
        "CallNumber": 700000,
        "DialNumber": 700000,
        "Interviewer": "James",
        "DialResult": 300,
        "DialedNumber": 700000,
        "dial_secs": 500,
        "OutcomeCode": "310",
        "Cohort": "'AA'",
        "StartTime": datetime(2023, 2, 3, 20, 5, 0),
        "EndTime": datetime(2023, 2, 3, 20, 10, 0),
    }


@mock.patch("reports.mi_hub_call_history_report.get_cati_mi_hub_call_history_from_database")
def test_mi_hub_call_history_returns_something(mock_mi_hub_call_history):
    mock_mi_hub_call_history.return_value = [
        {
            "InstrumentId": "1234",
            "PrimaryKeyValue": "9000001",
            "CallNumber": 700000,
            "DialNumber": 700000,
            "Interviewer": "James",
            "DialResult": 300,
            "DialedNumber": 700000,
            "dial_secs": 500,
            "OutcomeCode": "310",
            "Cohort": "AA",
            "StartTime": datetime(2023, 2, 3, 20, 5, 0),
            "EndTime": datetime(2023, 2, 3, 20, 10, 0),
        }
    ]
    result = get_mi_hub_call_history("foo", "LMS2101_AA1", "1234")
    assert result == [
        MiHubCallHistory(
            questionnaire_name="LMS2101_AA1",
            questionnaire_id="1234",
            serial_number="9000001",
            call_number=700000,
            dial_number=700000,
            interviewer="James",
            dial_result=300,
            dial_line_number=700000,
            seconds_interview=500,
            outcome_code="310",
            cohort="AA",
            dial_date="20230203",
            dial_time="20:05:00",
            end_time="20:10:00",
        )
    ]


@mock.patch("reports.mi_hub_call_history_report.get_cati_mi_hub_call_history_from_database")
def test_mi_hub_call_history_returns_cohort_field_without_quotes_when_quotes_are_present(mock_mi_hub_call_history, mock_get_mi_hub_call_history_record):
    mock_get_mi_hub_call_history_record["Cohort"]="'AA'"
    mock_mi_hub_call_history.return_value = [
        mock_get_mi_hub_call_history_record
    ]
    result = get_mi_hub_call_history("foo", "LMS2101_AA1", "1234")
    assert result[0].cohort == "AA"
