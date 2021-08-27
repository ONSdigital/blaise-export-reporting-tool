import pytest

from functions.interviewer_call_pattern_data_functions import (
    get_hours_worked, get_call_time_in_seconds, get_total_seconds_from_string,
    convert_seconds_to_datetime_format, hours_on_calls, average_calls_per_hour, respondents_interviewed,
    average_respondents_interviewed_per_hour, results_for_calls_with_status)
from reports.interviewer_call_pattern_report import validate_dataframe


def test_get_hours_worked(valid_dataframe):
    assert get_hours_worked(valid_dataframe) == "0:48:05"


def test_get_call_time_in_seconds(valid_dataframe):
    assert get_call_time_in_seconds(valid_dataframe) == 770


def test_call_time_is_less_than_hours_worked(valid_dataframe):
    assert get_call_time_in_seconds(valid_dataframe) < get_total_seconds_from_string(
        get_hours_worked(valid_dataframe))


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
    assert convert_seconds_to_datetime_format(seconds) == expected


@pytest.mark.parametrize(
    "hours_worked, total_call_seconds, expected",
    [
        ("10:00:00", "18000", "50.0%"),
        ("30:00:00", "16200", "15.0%"),
        ("50:00:00", "135000", "75.0%"),
    ],
)
def test_get_percentage_of_hours_on_calls(hours_worked, total_call_seconds, expected):
    assert hours_on_calls(hours_worked, total_call_seconds) == expected


@pytest.mark.parametrize(
    "hours_worked, expected",
    [
        ("00:00:01", 1),
        ("00:00:10", 10),
        ("00:01:00", 60),
        ("01:00:00", 3600),
        ("10:00:00", 36000),
    ],
)
def test_get_total_seconds_from_string(hours_worked, expected):
    assert get_total_seconds_from_string(hours_worked) == expected


@pytest.mark.parametrize(
    "hours_worked, expected",
    [
        ("10:00:00", 1.0),
        ("05:00:00", 2.0),
        ("02:30:00", 4),
    ],
)
def test_get_average_calls_per_hour(hours_worked, expected, valid_dataframe):
    assert average_calls_per_hour(valid_dataframe, hours_worked) == expected


def test_get_respondents_interviewed(valid_dataframe):
    assert respondents_interviewed(valid_dataframe) == 10


@pytest.mark.parametrize(
    "hours_worked, expected",
    [
        ("10:00:00", 1),
        ("5:00:00", 2.0),
        ("2:00:00", 5.0),
    ],
)
def test_get_average_respondents_interviewed_per_hour(hours_worked, expected, valid_dataframe):
    assert average_respondents_interviewed_per_hour(valid_dataframe, hours_worked) == expected


@pytest.mark.parametrize(
    "status, expected",
    [
        ("non response", "2/45"),
        ("no contact", "27/45"),
        ("questionnaire|completed", "9/45"),
        ("appointment made", "1/45"),
        ("cattywampus", "0/45"),
    ],
)
def test_get_results_for_calls_with_status_total(status, expected, status_dataframe):
    assert results_for_calls_with_status('status', status, status_dataframe, 45)[0] == expected


@pytest.mark.parametrize(
    "status, expected",
    [
        ("non response", 4.44),
        ("no contact", 60.0),
        ("questionnaire|completed", 20.0),
        ("appointment made", 2.22),
        ("numberwang", 0),
    ],
)
def test_get_results_for_calls_with_status_percentage(status, expected, status_dataframe):
    assert results_for_calls_with_status('status', status, status_dataframe, 45)[1] == expected


def test_status_results_total_one_hundred_percent(dataframe_with_some_invalid_fields):
    denominator = len(dataframe_with_some_invalid_fields.index)
    valid_dataframe, invalid_dataframe = validate_dataframe(dataframe_with_some_invalid_fields)

    non_response = results_for_calls_with_status('status', 'non response', valid_dataframe, denominator)[1]
    no_contact = results_for_calls_with_status('status', 'no contact', valid_dataframe, denominator)[1]
    questionnaire_completed = results_for_calls_with_status('status', 'questionnaire|completed', valid_dataframe, denominator)[1]
    appointment_made = results_for_calls_with_status('status', 'appointment made', valid_dataframe, denominator)[1]

    discounted = 100 * len(invalid_dataframe.index) / denominator

    assert (non_response + no_contact + questionnaire_completed +
            appointment_made + discounted) == pytest.approx(100)


def test_no_contact_results_total_one_hundred_percent(status_dataframe):
    no_contact_dataframe = status_dataframe[status_dataframe["status"].str.contains('no contact', case=False, na=False)]
    denominator = len(no_contact_dataframe.index)

    answerservice = results_for_calls_with_status('call_result', 'answerservice', no_contact_dataframe, denominator)[1]
    busy = results_for_calls_with_status('call_result', 'busy', no_contact_dataframe, denominator)[1]
    disconnect = results_for_calls_with_status('call_result', 'disconnect', no_contact_dataframe, denominator)[1]
    noanswer = results_for_calls_with_status('call_result', 'noanswer', no_contact_dataframe, denominator)[1]
    other = results_for_calls_with_status('call_result', 'other', no_contact_dataframe, denominator)[1]

    assert answerservice + busy + disconnect + noanswer + other == pytest.approx(100)
