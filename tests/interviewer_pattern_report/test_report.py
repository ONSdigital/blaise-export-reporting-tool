import numpy as np
import pytest
from interviewer_pattern_report.report import *

# TODO: Sam, help!
# @mock.patch.object()
# @pytest.mark.parametrize(
#     "interviewer, start_date, end_date, expected",
#     [
#         ("matpal", "2021-01-01", "2021-06-11", InterviewerPatternReport(
#             hours_worked='3:36:40',
#             call_time='0:05:38',
#             hours_on_calls_percentage='2.6%',
#             average_calls_per_hour=6.09,
#             respondents_interviewed=22,
#             households_completed_successfully=0,
#             average_respondents_interviewed_per_hour=6.09,
#             no_contacts_percentage='63.64%',
#             appointments_for_contacts_percentage='0.0%',
#             discounted_invalid_records='1/23',
#             invalid_fields='call_end_time')
#          ),
#     ],
# )
# def test_get_call_pattern_records_by_interviewer_and_date_range(interviewer, start_date, end_date, expected):
#     error, call_pattern_records = get_call_pattern_records_by_interviewer_and_date_range(interviewer, start_date, end_date)
#
#     message, error_code = error
#     assert message is None
#     assert error_code is 200
#     assert call_pattern_records == expected


# @pytest.mark.parametrize(
#     "interviewer, start_date, end_date, expected_message",
#     [
#         ("thorne1", "2021-01-01", "2021-06-11", "No records found for thorne1 from 2021-01-01 to 2021-06-11"),
#         ("matpal", "2020-01-01", "2020-06-11", "No records found for matpal from 2020-01-01 to 2020-06-11"),
#         ("nik", "", "", "Invalid format for date properties provided"),
#     ],
# )
# def test_get_call_pattern_records_by_interviewer_and_date_range_returns_error(interviewer, start_date, end_date, expected_message):
#     error, result = get_call_pattern_records_by_interviewer_and_date_range(interviewer, start_date, end_date)
#     message, status_code = error
#
#     assert status_code == 400
#     assert message == expected_message


def test_generate_report(mock_data):
    assert not generate_report(mock_data)[0]
    assert generate_report(mock_data)[1] == InterviewerPatternReport(
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


def test_generate_report_returns_an_error_message(mock_data, capsys):
    mock_data.loc[mock_data['questionnaire_id'] == '05cf69af-3a4e-47df-819a-928350fdda5a', ['call_start_time']] = 'blah'
    generate_report(mock_data)
    captured = capsys.readouterr()
    assert captured.out == ('Could not calculate get_hours_worked(): Can only use .dt accessor with '
                            'datetimelike values\n')


def test_validate_dataframe(mock_data):
    expected_errors, expected_valid_dataframe, expected_invalid_dataframe = validate_dataframe(mock_data)

    assert expected_valid_dataframe.columns.to_series().str.islower().all()
    assert not expected_errors
    assert type(expected_valid_dataframe) == pd.DataFrame
    assert len(expected_invalid_dataframe.index) == 0


def test_validate_dataframe_returns_invalid_data(mock_data):
    mock_data.loc[mock_data['questionnaire_id'] == '05cf69af-3a4e-47df-819a-928350fdda5a', 'call_end_time'] = np.nan

    expected_errors, expected_valid_dataframe, expected_invalid_dataframe = validate_dataframe(mock_data)
    assert expected_valid_dataframe.columns.to_series().str.islower().all()
    assert not expected_errors
    assert type(expected_valid_dataframe) == pd.DataFrame
    assert len(expected_invalid_dataframe.index) == 2


def test_validate_dataframe_returns_error(mock_data):
    mock_data.loc[mock_data['questionnaire_id'] == '05cf69af-3a4e-47df-819a-928350fdda5a', 'number_of_interviews'] = 'hey-yo!'

    expected_errors, expected_valid_dataframe, expected_invalid_dataframe = validate_dataframe(mock_data)
    assert expected_errors[0] == f"validate_dataframe() failed: invalid literal for int() with base 10: 'hey-yo!'"
    assert expected_errors[1] == 400
    assert not expected_valid_dataframe
    assert not expected_invalid_dataframe


def test_drop_and_return_invalidated_records(mock_data):
    actual_valid_records, actual_invalid_records = drop_and_return_invalidated_records(mock_data)
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
def test_get_invalid_fields(column_names, mock_data):
    for col in column_names:
        mock_data.loc[mock_data['questionnaire_id'] == "05cf69af-3a4e-47df-819a-928350fdda5a", col] = np.nan

    assert get_invalid_fields(mock_data) == ''.join(column_names)
