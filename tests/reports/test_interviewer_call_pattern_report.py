import numpy as np
import pandas as pd
import pytest

from reports.interviewer_call_pattern_report import get_call_pattern_records_by_interviewer_and_date_range, \
    generate_report, InterviewerCallPattern, validate_dataframe, drop_and_return_null_records, \
    drop_and_return_timed_out_records, \
    get_invalid_fields, get_hours_worked, get_call_time_in_seconds, get_percentage_of_hours_on_calls, \
    get_average_calls_per_hour, get_respondents_interviewed, get_number_of_households_completed_successfully, \
    get_average_respondents_interviewed_per_hour, get_percentage_of_call_for_status, \
    convert_call_time_seconds_to_datetime_format, add_invalid_fields_to_report


def test_get_call_pattern_records_by_interviewer_and_date_range_returns_error():
    error, call_pattern_records = get_call_pattern_records_by_interviewer_and_date_range("ricer", "blah", "blah")
    error_message, error_code = error
    assert error_code == 400
    assert error_message == "Invalid date range parameters provided"


def test_generate_report(call_history_dataframe):
    error, result = generate_report(call_history_dataframe)
    assert error is None
    assert result == InterviewerCallPattern(
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
def test_add_invalid_fields_to_report(column_names, interviewer_call_pattern_report, invalid_call_history_dataframe,
                                      call_history_dataframe):
    for col in column_names:
        invalid_call_history_dataframe.loc[
            invalid_call_history_dataframe['questionnaire_id'] == "05cf69af-1a4e-47df-819a-928350fdda5a", col] = np.nan
    add_invalid_fields_to_report(
        interviewer_call_pattern_report,
        invalid_call_history_dataframe,
        call_history_dataframe
    )
    assert interviewer_call_pattern_report.invalid_fields == ", ".join(column_names)


def test_generate_report_returns_error(call_history_dataframe, capsys):
    call_history_dataframe.loc[call_history_dataframe['questionnaire_id'] == '05cf69af-3a4e-47df-819a-928350fdda5a', [
        'call_start_time']] = 'blah'
    generate_report(call_history_dataframe)
    captured = capsys.readouterr()
    assert captured.out == ('Could not calculate get_hours_worked(): Can only use .dt accessor with '
                            'datetimelike values\n')


def test_validate_dataframe_with_no_invalid_data(call_history_dataframe):
    call_history_dataframe.loc[call_history_dataframe['status'].str.contains('Timed out during questionnaire', case=False), [
        'status']] = 'happy'

    error, valid_dataframe, invalid_dataframe = validate_dataframe(call_history_dataframe)
    assert valid_dataframe.columns.to_series().str.islower().all()
    assert error is None
    assert type(valid_dataframe) == pd.DataFrame
    assert len(invalid_dataframe.index) == 0


def test_validate_dataframe_with_invalid_data(call_history_dataframe):
    call_history_dataframe.loc[
        call_history_dataframe["questionnaire_id"] == "05cf69af-3a4e-47df-819a-928350fdda5a", "call_end_time"] = np.nan
    error, valid_dataframe, invalid_dataframe = validate_dataframe(call_history_dataframe)
    assert valid_dataframe.columns.to_series().str.islower().all()
    assert error is None
    assert type(valid_dataframe) == pd.DataFrame
    assert len(invalid_dataframe.index) == 3


def test_validate_dataframe_returns_error(call_history_dataframe):
    call_history_dataframe.loc[
        call_history_dataframe[
            "questionnaire_id"] == "05cf69af-3a4e-47df-819a-928350fdda5a", "number_of_interviews"] = "hey-yo!"
    error, valid_dataframe, invalid_dataframe = validate_dataframe(call_history_dataframe)
    error_message, error_code = error
    assert error_message == "validate_dataframe failed: invalid literal for int() with base 10: \'hey-yo!\'"
    assert error_code == 400
    assert valid_dataframe is None
    assert invalid_dataframe is None


@pytest.mark.parametrize(
    "status_message",
    [
        "Timed out",
    ],
)
def test_drop_and_return_timed_out_records(call_history_dataframe, status_message):
    actual_valid_records, actual_invalid_records = drop_and_return_timed_out_records(call_history_dataframe, status_message)
    assert len(actual_valid_records.index) == 7
    assert len(actual_invalid_records.index) == 1


def test_drop_and_return_null_records(call_history_dataframe, invalid_call_history_dataframe):
    actual_valid_records, actual_invalid_records = drop_and_return_null_records(call_history_dataframe,
                                                                                invalid_call_history_dataframe)
    assert len(actual_valid_records.index) == 8
    assert len(actual_invalid_records.index) == 1


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
    for col in column_names:
        call_history_dataframe.loc[
            call_history_dataframe['questionnaire_id'] == "05cf69af-3a4e-47df-819a-928350fdda5a", col] = np.nan
    assert get_invalid_fields(call_history_dataframe) == "'status' column returned a timed out session"+", ".join(column_names)


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
