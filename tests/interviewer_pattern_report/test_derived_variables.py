import pytest
from interviewer_pattern_report.derived_variables import *


def test_get_hours_worked(mock_data):
    assert get_hours_worked(mock_data) == "2:27:57"


def test_get_call_time(mock_data):
    assert get_call_time(mock_data) == "165"


@pytest.mark.parametrize(
    "hours_worked, total_call_seconds, expected",
    [
        ("10:00:00", "18000", 50),
        ("30:00:00", "16200", 15),
        ("50:00:00", "135000", 75),
    ],
)
def test_get_percentage_of_time_on_calls(hours_worked, total_call_seconds, expected):
    assert get_percentage_of_time_on_calls(hours_worked, total_call_seconds) == expected


def test_get_average_calls_per_hour(mock_data):
    assert get_average_calls_per_hour(mock_data) == "2.6666666666666665"
