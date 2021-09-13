from google.api_core.datetime_helpers import DatetimeWithNanoseconds
from reports.interviewer_call_pattern_report_refactor import get_call_pattern_report
import datetime
import pandas as pd


def test_get_call_pattern_report_returns_an_empty_dict_if_no_records_were_found(mocker):
    mocker.patch(
        "reports.interviewer_call_pattern_report_refactor.get_call_history_records",
        return_value=pd.DataFrame(),
    )

    assert get_call_pattern_report() == {}


def test_get_call_pattern_report_returns_hours_worked_when_a_record_is_found(mocker):
    datastore_records = [
        {
            "call_number": 2,
            "call_result": "AnswerService",
            "call_start_time": DatetimeWithNanoseconds(
                2021, 8, 7, 8, 00, 00, tzinfo=datetime.timezone.utc
            ),
            "call_end_time": DatetimeWithNanoseconds(
                2021, 8, 7, 14, 00, 00, tzinfo=datetime.timezone.utc
            ),
            "cohort": None,
            "dial_number": 1,
            "dial_secs": 8,
            "interviewer": "el4president",
            "number_of_interviews": "1",
            "outcome_code": 310,
            "questionnaire_id": "remember-24-01-9dc791f0cb07",
            "questionnaire_name": "OPN2108R",
            "serial_number": 24012022,
            "status": "Finished (No contact)",
            "survey": "OPN",
            "update_info": None,
            "wave": None,
        },
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report_refactor.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))
    result = get_call_pattern_report()
    assert result["hours_worked"] == "6:00:00"


def test_get_call_pattern_report_returns_hours_worked_when_multiple_records_from_a_single_day_are_found(mocker):
    datastore_records = [
        {
            "call_number": 2,
            "call_result": "AnswerService",
            "call_start_time": DatetimeWithNanoseconds(
                2021, 8, 7, 8, 00, 00, tzinfo=datetime.timezone.utc
            ),
            "call_end_time": DatetimeWithNanoseconds(
                2021, 8, 7, 14, 00, 00, tzinfo=datetime.timezone.utc
            ),
            "cohort": None,
            "dial_number": 1,
            "dial_secs": 8,
            "interviewer": "el4president",
            "number_of_interviews": "1",
            "outcome_code": 310,
            "questionnaire_id": "remember-24-01-9dc791f0cb07",
            "questionnaire_name": "OPN2108R",
            "serial_number": 24012022,
            "status": "Finished (No contact)",
            "survey": "OPN",
            "update_info": None,
            "wave": None,
        },
        {
            "call_number": 1,
            "call_result": "AnswerService",
            "call_start_time": DatetimeWithNanoseconds(
                2021, 8, 7, 15, 00, 00, tzinfo=datetime.timezone.utc
            ),
            "call_end_time": DatetimeWithNanoseconds(
                2021, 8, 7, 16, 00, 00, tzinfo=datetime.timezone.utc
            ),
            "cohort": None,
            "dial_number": 1,
            "dial_secs": 8,
            "interviewer": "el4president",
            "number_of_interviews": "1",
            "outcome_code": 310,
            "questionnaire_id": "remember-24-01-9dc791f0cb07",
            "questionnaire_name": "OPN2108R",
            "serial_number": 24012022,
            "status": "Finished (No contact)",
            "survey": "OPN",
            "update_info": None,
            "wave": None,
        },
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report_refactor.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report()
    assert result["hours_worked"] == "8:00:00"


def test_get_call_pattern_report_returns_hours_worked_when_multiple_records_from_multiple_days_are_found(mocker):
    datastore_records = [
        {
            "call_number": 2,
            "call_result": "AnswerService",
            "call_start_time": DatetimeWithNanoseconds(
                2021, 8, 7, 8, 00, 00, tzinfo=datetime.timezone.utc
            ),
            "call_end_time": DatetimeWithNanoseconds(
                2021, 8, 7, 14, 00, 00, tzinfo=datetime.timezone.utc
            ),
            "cohort": None,
            "dial_number": 1,
            "dial_secs": 8,
            "interviewer": "el4president",
            "number_of_interviews": "1",
            "outcome_code": 310,
            "questionnaire_id": "remember-24-01-9dc791f0cb07",
            "questionnaire_name": "OPN2108R",
            "serial_number": 24012022,
            "status": "Finished (No contact)",
            "survey": "OPN",
            "update_info": None,
            "wave": None,
        },
        {
            "call_number": 1,
            "call_result": "AnswerService",
            "call_start_time": DatetimeWithNanoseconds(
                2021, 8, 7, 15, 00, 00, tzinfo=datetime.timezone.utc
            ),
            "call_end_time": DatetimeWithNanoseconds(
                2021, 8, 7, 16, 00, 00, tzinfo=datetime.timezone.utc
            ),
            "cohort": None,
            "dial_number": 1,
            "dial_secs": 8,
            "interviewer": "el4president",
            "number_of_interviews": "1",
            "outcome_code": 310,
            "questionnaire_id": "remember-24-01-9dc791f0cb07",
            "questionnaire_name": "OPN2108R",
            "serial_number": 24012022,
            "status": "Finished (No contact)",
            "survey": "OPN",
            "update_info": None,
            "wave": None,
        },
        {
            "call_number": 1,
            "call_result": "AnswerService",
            "call_start_time": DatetimeWithNanoseconds(
                2021, 8, 9, 15, 00, 00, tzinfo=datetime.timezone.utc
            ),
            "call_end_time": DatetimeWithNanoseconds(
                2021, 8, 9, 16, 00, 00, tzinfo=datetime.timezone.utc
            ),
            "cohort": None,
            "dial_number": 1,
            "dial_secs": 8,
            "interviewer": "el4president",
            "number_of_interviews": "1",
            "outcome_code": 310,
            "questionnaire_id": "remember-24-01-9dc791f0cb07",
            "questionnaire_name": "OPN2108R",
            "serial_number": 24012022,
            "status": "Finished (No contact)",
            "survey": "OPN",
            "update_info": None,
            "wave": None,
        },
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report_refactor.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report()
    assert result["hours_worked"] == "9:00:00"


def test_get_call_pattern_report_ignores_record_when_no_end_call_time_is_found(mocker):
    datastore_records = [
        {
            "call_number": 2,
            "call_result": "AnswerService",
            "call_start_time": DatetimeWithNanoseconds(
                2021, 8, 7, 8, 00, 00, tzinfo=datetime.timezone.utc
            ),
            "call_end_time": "",
            "cohort": None,
            "dial_number": 1,
            "dial_secs": 8,
            "interviewer": "el4president",
            "number_of_interviews": "1",
            "outcome_code": 310,
            "questionnaire_id": "remember-24-01-9dc791f0cb07",
            "questionnaire_name": "OPN2108R",
            "serial_number": 24012022,
            "status": "Finished (No contact)",
            "survey": "OPN",
            "update_info": None,
            "wave": None,
        },
        {
            "call_number": 2,
            "call_result": "AnswerService",
            "call_start_time": DatetimeWithNanoseconds(
                2021, 8, 7, 12, 00, 00, tzinfo=datetime.timezone.utc
            ),
            "call_end_time": pd.NaT,
            "cohort": None,
            "dial_number": 1,
            "dial_secs": 8,
            "interviewer": "el4president",
            "number_of_interviews": "1",
            "outcome_code": 310,
            "questionnaire_id": "remember-24-01-9dc791f0cb07",
            "questionnaire_name": "OPN2108R",
            "serial_number": 24012022,
            "status": "Finished (No contact)",
            "survey": "OPN",
            "update_info": None,
            "wave": None,
        },
        {
            "call_number": 1,
            "call_result": "AnswerService",
            "call_start_time": DatetimeWithNanoseconds(
                2021, 8, 7, 15, 00, 00, tzinfo=datetime.timezone.utc
            ),
            "call_end_time": DatetimeWithNanoseconds(
                2021, 8, 7, 16, 00, 00, tzinfo=datetime.timezone.utc
            ),
            "cohort": None,
            "dial_number": 1,
            "dial_secs": 8,
            "interviewer": "el4president",
            "number_of_interviews": "1",
            "outcome_code": 310,
            "questionnaire_id": "remember-24-01-9dc791f0cb07",
            "questionnaire_name": "OPN2108R",
            "serial_number": 24012022,
            "status": "Finished (No contact)",
            "survey": "OPN",
            "update_info": None,
            "wave": None,
        },
        {
            "call_number": 1,
            "call_result": "AnswerService",
            "call_start_time": DatetimeWithNanoseconds(
                2021, 8, 9, 15, 00, 00, tzinfo=datetime.timezone.utc
            ),
            "call_end_time": DatetimeWithNanoseconds(
                2021, 8, 9, 16, 00, 00, tzinfo=datetime.timezone.utc
            ),
            "cohort": None,
            "dial_number": 1,
            "dial_secs": 8,
            "interviewer": "el4president",
            "number_of_interviews": "1",
            "outcome_code": 310,
            "questionnaire_id": "remember-24-01-9dc791f0cb07",
            "questionnaire_name": "OPN2108R",
            "serial_number": 24012022,
            "status": "Finished (No contact)",
            "survey": "OPN",
            "update_info": None,
            "wave": None,
        },
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report_refactor.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report()
    assert result["hours_worked"] == "2:00:00"


def test_get_call_pattern_report_returns_number_of_records_where_no_end_call_time_is_found(mocker):
    datastore_records = [
        {
            "call_number": 1,
            "call_result": "AnswerService",
            "call_start_time": DatetimeWithNanoseconds(
                2021, 8, 7, 8, 00, 00, tzinfo=datetime.timezone.utc
            ),
            "call_end_time": "",
            "cohort": None,
            "dial_number": 1,
            "dial_secs": 8,
            "interviewer": "el4president",
            "number_of_interviews": "1",
            "outcome_code": 310,
            "questionnaire_id": "remember-24-01-9dc791f0cb07",
            "questionnaire_name": "OPN2108R",
            "serial_number": 24012022,
            "status": "Finished (No contact)",
            "survey": "OPN",
            "update_info": None,
            "wave": None,
        },
        {
            "call_number": 2,
            "call_result": "AnswerService",
            "call_start_time": DatetimeWithNanoseconds(
                2021, 8, 7, 12, 00, 00, tzinfo=datetime.timezone.utc
            ),
            "call_end_time": None,
            "cohort": None,
            "dial_number": 1,
            "dial_secs": 8,
            "interviewer": "el4president",
            "number_of_interviews": "1",
            "outcome_code": 310,
            "questionnaire_id": "remember-24-01-9dc791f0cb07",
            "questionnaire_name": "OPN2108R",
            "serial_number": 24012022,
            "status": "Finished (No contact)",
            "survey": "OPN",
            "update_info": None,
            "wave": None,
        },
        {
            "call_number": 3,
            "call_result": "AnswerService",
            "call_start_time": DatetimeWithNanoseconds(
                2021, 8, 9, 15, 00, 00, tzinfo=datetime.timezone.utc
            ),
            "call_end_time": DatetimeWithNanoseconds(
                2021, 8, 7, 16, 00, 00, tzinfo=datetime.timezone.utc
            ),
            "cohort": None,
            "dial_number": 1,
            "dial_secs": 8,
            "interviewer": "el4president",
            "number_of_interviews": "1",
            "outcome_code": 310,
            "questionnaire_id": "remember-24-01-9dc791f0cb07",
            "questionnaire_name": "OPN2108R",
            "serial_number": 24012022,
            "status": "Finished (No contact)",
            "survey": "OPN",
            "update_info": None,
            "wave": None,
        },
        {
            "call_number": 4,
            "call_result": "AnswerService",
            "call_start_time": DatetimeWithNanoseconds(
                2021, 8, 9, 15, 00, 00, tzinfo=datetime.timezone.utc
            ),
            "call_end_time": DatetimeWithNanoseconds(
                2021, 8, 9, 16, 00, 00, tzinfo=datetime.timezone.utc
            ),
            "cohort": None,
            "dial_number": 1,
            "dial_secs": 8,
            "interviewer": "el4president",
            "number_of_interviews": "1",
            "outcome_code": 310,
            "questionnaire_id": "remember-24-01-9dc791f0cb07",
            "questionnaire_name": "OPN2108R",
            "serial_number": 24012022,
            "status": "Finished (No contact)",
            "survey": "OPN",
            "update_info": None,
            "wave": None,
        },
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report_refactor.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report()
    assert result["discounted_invalid_cases"] == "2/4, 50.00%"


def test_get_call_pattern_report_returns_timed_out_status_when_no_end_call_time_is_found(mocker):
    datastore_records = [
        {
            "call_number": 1,
            "call_result": "AnswerService",
            "call_start_time": DatetimeWithNanoseconds(
                2021, 8, 7, 8, 00, 00, tzinfo=datetime.timezone.utc
            ),
            "call_end_time": None,
            "cohort": None,
            "dial_number": 1,
            "dial_secs": 8,
            "interviewer": "el4president",
            "number_of_interviews": "1",
            "outcome_code": 310,
            "questionnaire_id": "remember-24-01-9dc791f0cb07",
            "questionnaire_name": "OPN2108R",
            "serial_number": 24012022,
            "status": "Timed out",
            "survey": "OPN",
            "update_info": None,
            "wave": None,
        },
        {
            "call_number": 2,
            "call_result": "AnswerService",
            "call_start_time": DatetimeWithNanoseconds(
                2021, 8, 7, 12, 00, 00, tzinfo=datetime.timezone.utc
            ),
            "call_end_time": DatetimeWithNanoseconds(
                2021, 8, 7, 14, 00, 00, tzinfo=datetime.timezone.utc
            ),
            "cohort": None,
            "dial_number": 1,
            "dial_secs": 8,
            "interviewer": "el4president",
            "number_of_interviews": "1",
            "outcome_code": 310,
            "questionnaire_id": "remember-24-01-9dc791f0cb07",
            "questionnaire_name": "OPN2108R",
            "serial_number": 24012022,
            "status": "Finished (No contact)",
            "survey": "OPN",
            "update_info": None,
            "wave": None,
        },
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report_refactor.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report()
    assert result["invalid_fields"] == "'status' column had timed out call status"


def test_get_call_pattern_report_returns_call_time_when_one_record_found(mocker):
    datastore_records = [
        {
            "call_number": 1,
            "call_result": "AnswerService",
            "call_start_time": DatetimeWithNanoseconds(
                2021, 8, 7, 8, 00, 00, tzinfo=datetime.timezone.utc
            ),
            "call_end_time": DatetimeWithNanoseconds(
                2021, 8, 7, 10, 00, 00, tzinfo=datetime.timezone.utc
            ),
            "cohort": None,
            "dial_number": 1,
            "dial_secs": 600,
            "interviewer": "el4president",
            "number_of_interviews": "1",
            "outcome_code": 310,
            "questionnaire_id": "remember-24-01-9dc791f0cb07",
            "questionnaire_name": "OPN2108R",
            "serial_number": 24012022,
            "status": "Timed out",
            "survey": "OPN",
            "update_info": None,
            "wave": None,
        },
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report_refactor.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report()
    assert result["call_time"] == "0:10:00"

def test_get_call_pattern_report_returns_call_time_when_multiple_records_are_found(mocker):
    datastore_records = [
        {
            "call_number": 1,
            "call_result": "AnswerService",
            "call_start_time": DatetimeWithNanoseconds(
                2021, 8, 7, 8, 00, 00, tzinfo=datetime.timezone.utc
            ),
            "call_end_time": DatetimeWithNanoseconds(
                2021, 8, 7, 10, 00, 00, tzinfo=datetime.timezone.utc
            ),
            "cohort": None,
            "dial_number": 1,
            "dial_secs": 600,
            "interviewer": "el4president",
            "number_of_interviews": "1",
            "outcome_code": 310,
            "questionnaire_id": "remember-24-01-9dc791f0cb07",
            "questionnaire_name": "OPN2108R",
            "serial_number": 24012022,
            "status": "Timed out",
            "survey": "OPN",
            "update_info": None,
            "wave": None,
        },
        {
            "call_number": 2,
            "call_result": "AnswerService",
            "call_start_time": DatetimeWithNanoseconds(
                2021, 8, 7, 11, 00, 00, tzinfo=datetime.timezone.utc
            ),
            "call_end_time": DatetimeWithNanoseconds(
                2021, 8, 7, 12, 00, 00, tzinfo=datetime.timezone.utc
            ),
            "cohort": None,
            "dial_number": 1,
            "dial_secs": 600,
            "interviewer": "el4president",
            "number_of_interviews": "1",
            "outcome_code": 310,
            "questionnaire_id": "remember-24-01-9dc791f0cb07",
            "questionnaire_name": "OPN2108R",
            "serial_number": 24012022,
            "status": "Timed out",
            "survey": "OPN",
            "update_info": None,
            "wave": None,
        },
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report_refactor.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report()
    assert result["call_time"] == "0:20:00"

def test_get_call_pattern_report_returns_hours_on_call_as_perecntage_of_worked_time_when_one_record_found(mocker):
    datastore_records = [
        {
            "call_number": 1,
            "call_result": "AnswerService",
            "call_start_time": DatetimeWithNanoseconds(
                2021, 8, 7, 9, 00, 00, tzinfo=datetime.timezone.utc
            ),
            "call_end_time": DatetimeWithNanoseconds(
                2021, 8, 7, 10, 00, 00, tzinfo=datetime.timezone.utc
            ),
            "cohort": None,
            "dial_number": 1,
            "dial_secs": 600,
            "interviewer": "el4president",
            "number_of_interviews": "1",
            "outcome_code": 310,
            "questionnaire_id": "remember-24-01-9dc791f0cb07",
            "questionnaire_name": "OPN2108R",
            "serial_number": 24012022,
            "status": "Timed out",
            "survey": "OPN",
            "update_info": None,
            "wave": None,
        },
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report_refactor.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report()
    assert result["hours_on_call_percentage"] == "16.67%"


def test_get_call_pattern_report_returns_hours_on_call_as_perecntage_of_worked_time_when_multiple_records_are_found(mocker):
    datastore_records = [
        {
            "call_number": 1,
            "call_result": "AnswerService",
            "call_start_time": DatetimeWithNanoseconds(
                2021, 8, 7, 9, 00, 00, tzinfo=datetime.timezone.utc
            ),
            "call_end_time": DatetimeWithNanoseconds(
                2021, 8, 7, 10, 00, 00, tzinfo=datetime.timezone.utc
            ),
            "cohort": None,
            "dial_number": 1,
            "dial_secs": 600,
            "interviewer": "el4president",
            "number_of_interviews": "1",
            "outcome_code": 310,
            "questionnaire_id": "remember-24-01-9dc791f0cb07",
            "questionnaire_name": "OPN2108R",
            "serial_number": 24012022,
            "status": "Timed out",
            "survey": "OPN",
            "update_info": None,
            "wave": None,
        },
        {
            "call_number": 2,
            "call_result": "AnswerService",
            "call_start_time": DatetimeWithNanoseconds(
                2021, 8, 7, 9, 00, 00, tzinfo=datetime.timezone.utc
            ),
            "call_end_time": DatetimeWithNanoseconds(
                2021, 8, 7, 10, 00, 00, tzinfo=datetime.timezone.utc
            ),
            "cohort": None,
            "dial_number": 1,
            "dial_secs": 600,
            "interviewer": "el4president",
            "number_of_interviews": "1",
            "outcome_code": 310,
            "questionnaire_id": "remember-24-01-9dc791f0cb07",
            "questionnaire_name": "OPN2108R",
            "serial_number": 24012022,
            "status": "Timed out",
            "survey": "OPN",
            "update_info": None,
            "wave": None,
        },
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report_refactor.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report()
    assert result["hours_on_call_percentage"] == "33.33%"

def test_get_call_pattern_report_returns_average_calls_per_hour(mocker):
    datastore_records = [
        {
            "call_number": 1,
            "call_result": "AnswerService",
            "call_start_time": DatetimeWithNanoseconds(
                2021, 8, 7, 9, 00, 00, tzinfo=datetime.timezone.utc
            ),
            "call_end_time": DatetimeWithNanoseconds(
                2021, 8, 7, 10, 00, 00, tzinfo=datetime.timezone.utc
            ),
            "cohort": None,
            "dial_number": 1,
            "dial_secs": 600,
            "interviewer": "el4president",
            "number_of_interviews": "1",
            "outcome_code": 310,
            "questionnaire_id": "remember-24-01-9dc791f0cb07",
            "questionnaire_name": "OPN2108R",
            "serial_number": 24012022,
            "status": "Timed out",
            "survey": "OPN",
            "update_info": None,
            "wave": None,
        },
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report_refactor.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report()
    assert result["average_calls_per_hour"] == 1.0


def test_get_call_pattern_report_returns_average_calls_per_hour_when_multiple_records_are_found(mocker):
    datastore_records = [
        {
            "call_number": 1,
            "call_result": "AnswerService",
            "call_start_time": DatetimeWithNanoseconds(
                2021, 8, 7, 9, 00, 00, tzinfo=datetime.timezone.utc
            ),
            "call_end_time": DatetimeWithNanoseconds(
                2021, 8, 7, 10, 00, 00, tzinfo=datetime.timezone.utc
            ),
            "cohort": None,
            "dial_number": 1,
            "dial_secs": 8,
            "interviewer": "el4president",
            "number_of_interviews": "1",
            "outcome_code": 310,
            "questionnaire_id": "remember-24-01-9dc791f0cb07",
            "questionnaire_name": "OPN2108R",
            "serial_number": 24012022,
            "status": "Finished (No contact)",
            "survey": "OPN",
            "update_info": None,
            "wave": None,
        },
        {
            "call_number": 2,
            "call_result": "AnswerService",
            "call_start_time": DatetimeWithNanoseconds(
                2021, 8, 7, 9, 00, 00, tzinfo=datetime.timezone.utc
            ),
            "call_end_time": DatetimeWithNanoseconds(
                2021, 8, 7, 10, 00, 00, tzinfo=datetime.timezone.utc
            ),
            "cohort": None,
            "dial_number": 1,
            "dial_secs": 8,
            "interviewer": "el4president",
            "number_of_interviews": "1",
            "outcome_code": 310,
            "questionnaire_id": "remember-24-01-9dc791f0cb07",
            "questionnaire_name": "OPN2108R",
            "serial_number": 24012022,
            "status": "Finished (No contact)",
            "survey": "OPN",
            "update_info": None,
            "wave": None,
        },
        {
            "call_number": 3,
            "call_result": "AnswerService",
            "call_start_time": DatetimeWithNanoseconds(
                2021, 8, 7, 9, 00, 00, tzinfo=datetime.timezone.utc
            ),
            "call_end_time": DatetimeWithNanoseconds(
                2021, 8, 7, 10, 00, 00, tzinfo=datetime.timezone.utc
            ),
            "cohort": None,
            "dial_number": 1,
            "dial_secs": 8,
            "interviewer": "el4president",
            "number_of_interviews": "1",
            "outcome_code": 310,
            "questionnaire_id": "remember-24-01-9dc791f0cb07",
            "questionnaire_name": "OPN2108R",
            "serial_number": 24012022,
            "status": "Finished (No contact)",
            "survey": "OPN",
            "update_info": None,
            "wave": None,
        },
        {
            "call_number": 4,
            "call_result": "AnswerService",
            "call_start_time": DatetimeWithNanoseconds(
                2021, 8, 7, 9, 00, 00, tzinfo=datetime.timezone.utc
            ),
            "call_end_time": DatetimeWithNanoseconds(
                2021, 8, 7, 10, 00, 00, tzinfo=datetime.timezone.utc
            ),
            "cohort": None,
            "dial_number": 1,
            "dial_secs": 8,
            "interviewer": "el4president",
            "number_of_interviews": "1",
            "outcome_code": 310,
            "questionnaire_id": "remember-24-01-9dc791f0cb07",
            "questionnaire_name": "OPN2108R",
            "serial_number": 24012022,
            "status": "Finished (No contact)",
            "survey": "OPN",
            "update_info": None,
            "wave": None,
        },
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report_refactor.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report()
    assert result["average_calls_per_hour"] == 4.0