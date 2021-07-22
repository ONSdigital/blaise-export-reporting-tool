import numpy as np
import pytest

from interviewer_call_pattern_report.report import *
from models.interviewer_call_pattern import InterviewerCallPattern


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


def test_generate_report_returns_error(call_history_dataframe, capsys):
    call_history_dataframe.loc[call_history_dataframe['questionnaire_id'] == '05cf69af-3a4e-47df-819a-928350fdda5a', [
        'call_start_time']] = 'blah'
    generate_report(call_history_dataframe)
    captured = capsys.readouterr()
    assert captured.out == ('Could not calculate get_hours_worked(): Can only use .dt accessor with '
                            'datetimelike values\n')


def test_validate_dataframe_with_no_invalid_data(call_history_dataframe):
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
    assert len(invalid_dataframe.index) == 2


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


def test_drop_and_return_invalidated_records(call_history_dataframe):
    actual_valid_records, actual_invalid_records = drop_and_return_invalidated_records(call_history_dataframe)
    assert len(actual_valid_records.index) == 8
    assert len(actual_invalid_records.index) == 0


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
    assert get_invalid_fields(call_history_dataframe) == ''.join(column_names)
