import pytest

import numpy as np
import pandas as pd

from pandas.testing import assert_frame_equal

from models.interviewer_call_pattern_model import InterviewerCallPattern, InterviewerCallPatternWithNoValidData
from models.error_capture import BertException
from reports.interviewer_call_pattern_report import (
    invalid_data_found, generate_report,
    get_call_pattern_records_by_interviewer_and_date_range,
    get_invalid_fields, validate_dataframe
)


def test_get_call_pattern_report_when_data_is_completely_invalid(call_history_records, mocker):
    mock_interviewer_name = 'el4president'
    mock_date = '2022-01-24'
    mock_discounted_invalid_records = "numberwang"
    mock_invalid_fields = "aaaaaaaallll the fields"

    mocker.patch("reports.interviewer_call_pattern_report.get_call_history_records",
                 return_value=call_history_records)
    mocker.patch("reports.interviewer_call_pattern_report.validate_dataframe",
                 return_value=(pd.DataFrame(), mock_discounted_invalid_records, mock_invalid_fields))

    assert get_call_pattern_records_by_interviewer_and_date_range(
        mock_interviewer_name,
        mock_date,
        mock_date) == InterviewerCallPatternWithNoValidData(
        discounted_invalid_records=mock_discounted_invalid_records,
        invalid_fields=mock_invalid_fields
    )


def test_get_call_pattern_records_by_interviewer_and_date_range_returns_error():
    with pytest.raises(BertException) as error:
        get_call_pattern_records_by_interviewer_and_date_range("ricer", "blah", "blah", None)
    assert error.value.message == "Invalid date range parameters provided"
    assert error.value.code == 400


def test_generate_report(call_history_dataframe):
    assert generate_report(call_history_dataframe) == InterviewerCallPattern(
        hours_worked='2:27:57',
        call_time='0:02:45',
        hours_on_calls_percentage='1.86%',
        average_calls_per_hour=3.24,
        respondents_interviewed=8,
        households_completed_successfully=1,
        average_respondents_interviewed_per_hour=3.24,
        no_contacts_percentage='50.0%',
        appointments_for_contacts_percentage='0.0%'
    )


def test_generate_report_returns_error(call_history_dataframe, invalid_call_history_dataframe):
    call_history_dataframe.loc[call_history_dataframe['questionnaire_id'] == '05cf69af-3a4e-47df-819a-928350fdda5a', [
        'call_start_time']] = 'blah'

    with pytest.raises(BertException) as error:
        generate_report(call_history_dataframe, invalid_call_history_dataframe, 1)
    assert error.value.message == 'Could not calculate get_hours_worked(): Can only use .dt accessor with datetimelike values'
    assert error.value.code == 400


def test_validate_dataframe_with_no_invalid_data(call_history_dataframe):
    call_history_dataframe.loc[
        call_history_dataframe['status'].str.contains('Timed out during questionnaire', case=False), [
            'status']] = 'happy'

    valid_dataframe, discounted_records, discounted_fields = validate_dataframe(call_history_dataframe)
    assert valid_dataframe.columns.to_series().str.islower().all()
    assert type(valid_dataframe) == pd.DataFrame
    assert discounted_records == ''


def test_validate_dataframe_with_invalid_data(call_history_dataframe):
    call_history_dataframe.loc[
        call_history_dataframe["questionnaire_id"] == "05cf69af-3a4e-47df-819a-928350fdda5a", "call_end_time"] = np.nan

    valid_dataframe, discounted_records, discounted_fields = validate_dataframe(call_history_dataframe)
    assert valid_dataframe.columns.to_series().str.islower().all()
    assert type(valid_dataframe) == pd.DataFrame
    assert discounted_records == f"3/{len(call_history_dataframe.index)}"
    assert discounted_fields == "'status' column had timed out call status, 'call_end_time' column had missing data"


def test_validate_dataframe_returns_error(call_history_dataframe):
    call_history_dataframe.loc[
        call_history_dataframe[
            "questionnaire_id"] == "05cf69af-3a4e-47df-819a-928350fdda5a", "number_of_interviews"] = "hey-yo!"

    with pytest.raises(BertException) as error:
        validate_dataframe(call_history_dataframe)
    assert error.value.message == "validate_dataframe failed: invalid literal for int() with base 10: \'hey-yo!\'"
    assert error.value.code == 400


@pytest.mark.parametrize(
    "column_names",
    [
        (["call_start_time"]),
        (["call_end_time"]),
        (["number_of_interviews"]),
        (["call_start_time", "call_end_time"]),
        (["call_start_time", "call_end_time", "number_of_interviews"]),
    ],
)
def test_get_invalid_fields(column_names, call_history_dataframe):
    msg = ["'status' column had timed out call status"]
    for col in column_names:
        call_history_dataframe.loc[
            call_history_dataframe['questionnaire_id'] == "05cf69af-3a4e-47df-819a-928350fdda5a", col] = np.nan
        msg.append(f"'{col}' column had missing data")

    result = ", ".join(msg)
    assert get_invalid_fields(call_history_dataframe) == result
    assert result.count(",") == len(column_names)


def test_get_hours_worked(call_history_dataframe):
    assert get_hours_worked(call_history_dataframe) == "2:27:57"


def test_get_call_time_in_seconds(call_history_dataframe):
    assert get_call_time_in_seconds(call_history_dataframe) == 165


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
def test_get_average_calls_per_hour(hours_worked, expected, call_history_dataframe):
    assert get_average_calls_per_hour(call_history_dataframe, hours_worked) == expected


def test_get_respondents_interviewed(call_history_dataframe):
    assert get_respondents_interviewed(call_history_dataframe) == 8


@pytest.mark.parametrize(
    "status, expected",
    [
        ("Appointment made", 0),
        ("No contact", 4),
        ("numberwang", 1),
        ("foobar", 0),
    ],
)
def test_get_number_of_households_completed_successfully(status, expected, call_history_dataframe):
    assert get_number_of_households_completed_successfully(status, call_history_dataframe) == expected


@pytest.mark.parametrize(
    "hours_worked, expected",
    [
        ("08:00:00", 1),
        ("10:00:00", 0.8),
        ("5:00:00", 1.6),
        ("30:00:00", 0.27),
    ],
)
def test_get_average_respondents_interviewed_per_hour(hours_worked, expected, call_history_dataframe):
    assert get_average_respondents_interviewed_per_hour(call_history_dataframe, hours_worked) == expected


@pytest.mark.parametrize(
    "status, expected",
    [
        ("Appointment made", "0.0%"),
        ("No contact", "50.0%"),
        ("numberwang", "12.5%"),
        ("foobar", "0.0%"),
    ],
)
def test_get_percentage_of_call_for_status(status, expected, call_history_dataframe):
    assert get_percentage_of_call_for_status(status, call_history_dataframe) == expected


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
