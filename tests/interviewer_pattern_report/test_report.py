import json
import numpy as np
from interviewer_pattern_report.report import *
from unittest import mock
from interviewer_pattern_report.report import validate_dataframe


# # @mock.patch.object()
# def get_call_pattern_records_by_interviewer_and_date_range():
#     err, call_pattern_records = get_call_pattern_records_by_interviewer_and_date_range("matpal", "2021-01-01", "2021-06-11")
#
#     assert err is None
#     assert call_pattern_records == json.dumps(
#         {
#
#             "hours_worked": "2:27:57",
#             "call_time": "0:02:45",
#             "hours_on_calls_percentage": "1.86%",
#             "average_calls_per_hour": 3.24,
#             "respondents_interviewed": 8,
#             "households_completed_successfully": 1,
#             "average_respondents_interviewed_per_hour": 3.24,
#             "no_contacts_percentage": "50.0%",
#             "appointments_for_contacts_percentage": "0.0%"
#         }
#     )


def test_generate_report(mock_data):
    assert generate_report(mock_data)[0] == None
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
    mock_data.loc[mock_data['questionnaire_id'] == '05cf69af-3a4e-47df-819a-928350fdda5a', ['call_end_time']] = ''

    generate_report(mock_data)
    captured = capsys.readouterr()
    assert captured.out == ('Could not calculate get_hours_worked(): cannot subtract DatetimeArray from '
                            'ndarray\n')


def test_validate_dataframe(mock_data):
    expected_errors, expected_dataframe = validate_dataframe(mock_data)
    assert expected_errors == None
    assert type(expected_dataframe) == pd.DataFrame


def test_validate_dataframe_is_unhappy(mock_data):
    mock_data.loc[mock_data['questionnaire_id'] == '05cf69af-3a4e-47df-819a-928350fdda5a', 'call_start_time'] = np.nan

    expected_errors, expected_dataframe = validate_dataframe(mock_data)
    assert expected_errors == "validate_dataframe() failed: call_start_time has missing values"
    assert expected_dataframe == None
