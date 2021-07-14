import pytest

from interviewer_call_pattern_report.derived_variables import *


def test_get_hours_worked(mock_data):
    assert get_hours_worked(mock_data) == "2:27:57"


def test_get_call_time_in_seconds(mock_data):
    assert get_call_time_in_seconds(mock_data) == 165


@pytest.mark.parametrize(
    "hours_worked, total_call_seconds, expected",
    [
        ("10:00:00", "18000", "50.0%"),
        ("30:00:00", "16200", "15.0%"),
        ("50:00:00", "135000", "75.0%"),
    ],
)
def test_get_percentage_of_hours_on_calls(hours_worked, total_call_seconds, expected):
    assert get_percentage_of_hours_on_calls(hours_worked, total_call_seconds) == expected


@pytest.mark.parametrize(
    "hours_worked, expected",
    [
        ("08:00:00", 1.0),
        ("04:00:00", 2.0),
        ("16:00:00", 0.5),
    ],
)
def test_get_average_calls_per_hour(hours_worked, expected, mock_data):
    assert get_average_calls_per_hour(mock_data, hours_worked) == expected


def test_get_respondents_interviewed(mock_data):
    assert get_respondents_interviewed(mock_data) == 8


@pytest.mark.parametrize(
    "status, expected",
    [
        ("Appointment made", 0),
        ("No contact", 4),
        ("numberwang", 1),
        ("foobar", 0),
    ],
)
def test_get_number_of_households_completed_successfully(status, expected, mock_data):
    assert get_number_of_households_completed_successfully(status, mock_data) == expected


@pytest.mark.parametrize(
    "hours_worked, expected",
    [
        ("08:00:00", 1),
        ("10:00:00", 0.8),
        ("5:00:00", 1.6),
        ("30:00:00", 0.27),
    ],
)
def test_get_average_respondents_interviewed_per_hour(hours_worked, expected, mock_data):
    assert get_average_respondents_interviewed_per_hour(mock_data, hours_worked) == expected


@pytest.mark.parametrize(
    "status, expected",
    [
        ("Appointment made", "0.0%"),
        ("No contact", "50.0%"),
        ("numberwang", "12.5%"),
        ("foobar", "0.0%"),
    ],
)
def test_get_percentage_of_call_for_status(status, expected, mock_data):
    assert get_percentage_of_call_for_status(status, mock_data) == expected


@pytest.mark.parametrize(
    "seconds, expected",
    [
        (1, "0:00:01"),
        (100, "0:01:40"),
        (240, "0:04:00"),
        (856, "0:14:16"),
        (8355, "2:19:15"),
    ],
)
def test_convert_call_time_seconds_to_datetime_format(seconds, expected):
    assert convert_call_time_seconds_to_datetime_format(seconds) == expected

