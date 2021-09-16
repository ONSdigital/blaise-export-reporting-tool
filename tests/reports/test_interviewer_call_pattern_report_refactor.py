from google.api_core.datetime_helpers import DatetimeWithNanoseconds
from reports.interviewer_call_pattern_report_refactor import get_call_pattern_report
import datetime
import pandas as pd
from tests.helpers.interviewer_call_pattern_helpers import interviewer_call_pattern_report_sample_case, datetime_helper
import pytest

def test_get_call_pattern_report_returns_an_empty_dict_if_no_records_were_found(mocker):
    mocker.patch(
        "reports.interviewer_call_pattern_report_refactor.get_call_history_records",
        return_value=pd.DataFrame(),
    )

    assert get_call_pattern_report() == {}


def test_get_call_pattern_report_returns_hours_worked_when_a_record_is_found(mocker,):
    datastore_records = [interviewer_call_pattern_report_sample_case(
        start_date_time=datetime_helper(day=7, hour=9),
        end_date_time=datetime_helper(day=7, hour=15)
    )]

    mocker.patch(
        "reports.interviewer_call_pattern_report_refactor.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))
    result = get_call_pattern_report()
    assert result["hours_worked"] == "6:00:00"


def test_get_call_pattern_report_returns_hours_worked_when_multiple_records_from_a_single_day_are_found(mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            start_date_time=datetime_helper(day=7, hour=9),
            end_date_time=datetime_helper(day=7, hour=15)
        ),
        interviewer_call_pattern_report_sample_case(
            start_date_time=datetime_helper(day=7, hour=16),
            end_date_time=datetime_helper(day=7, hour=17)
        ),
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report_refactor.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report()
    assert result["hours_worked"] == "8:00:00"


def test_get_call_pattern_report_returns_hours_worked_when_multiple_records_from_multiple_days_are_found(mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            start_date_time=datetime_helper(day=7, hour=8),
            end_date_time=datetime_helper(day=7, hour=10)
        ),
        interviewer_call_pattern_report_sample_case(
            start_date_time=datetime_helper(day=7, hour=11),
            end_date_time=datetime_helper(day=7, hour=12)
        ),
        interviewer_call_pattern_report_sample_case(
            start_date_time=datetime_helper(day=8, hour=9),
            end_date_time=datetime_helper(day=8, hour=11)
        ),
        interviewer_call_pattern_report_sample_case(
            start_date_time=datetime_helper(day=8, hour=12),
            end_date_time=datetime_helper(day=8, hour=14)
        ),
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report_refactor.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report()
    assert result["hours_worked"] == "9:00:00"


def test_get_call_pattern_report_ignores_record_when_no_end_call_time_from_a_single_day_is_found(mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            start_date_time=datetime_helper(day=7, hour=10),
            end_date_time=datetime_helper(day=7, hour=12)
        ),
        interviewer_call_pattern_report_sample_case(
            start_date_time=datetime_helper(day=7, hour=11),
            end_date_time=None
        ),
    ]
    mocker.patch(
        "reports.interviewer_call_pattern_report_refactor.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report()
    assert result["hours_worked"] == "2:00:00"


def test_get_call_pattern_report_ignores_record_when_no_end_call_time_from_multiple_days_are_found(mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            start_date_time=datetime_helper(day=7, hour=10),
            end_date_time=datetime_helper(day=7, hour=12)
        ),
        interviewer_call_pattern_report_sample_case(
            start_date_time=datetime_helper(day=7, hour=11),
            end_date_time=None
        ),
        interviewer_call_pattern_report_sample_case(
            start_date_time=datetime_helper(day=8, hour=10),
            end_date_time=None
        ),
        interviewer_call_pattern_report_sample_case(
            start_date_time=datetime_helper(day=8, hour=11),
            end_date_time=datetime_helper(day=8, hour=12)
        ),
    ]
    mocker.patch(
        "reports.interviewer_call_pattern_report_refactor.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report()
    assert result["hours_worked"] == "3:00:00"

def test_get_call_pattern_report_returns_the_number_and_percentage_of_cases_where_no_end_call_time_is_found(mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            start_date_time=datetime_helper(day=7, hour=10),
            end_date_time=datetime_helper(day=7, hour=12)
        ),
        interviewer_call_pattern_report_sample_case(
            start_date_time=datetime_helper(day=7, hour=11),
            end_date_time=None
        ),
        interviewer_call_pattern_report_sample_case(
            start_date_time=datetime_helper(day=8, hour=10),
            end_date_time=None
        ),
        interviewer_call_pattern_report_sample_case(
            start_date_time=datetime_helper(day=8, hour=11),
            end_date_time=None
        ),
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report_refactor.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report()
    assert result["discounted_invalid_cases"] == "3/4, 75.00%"

# TODO: Clarify scenario requirements
@pytest.mark.skip(reason="need clarification")
def test_get_call_pattern_report_returns_timed_out_status_when_no_end_call_time_is_found(mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            start_date_time=datetime_helper(day=7, hour=10),
            end_date_time=datetime_helper(day=7, hour=12)
        ),
        interviewer_call_pattern_report_sample_case(
            start_date_time=datetime_helper(day=7, hour=11),
            end_date_time=None
        ),
        interviewer_call_pattern_report_sample_case(
            start_date_time=datetime_helper(day=8, hour=10),
            end_date_time=None
        ),
        interviewer_call_pattern_report_sample_case(
            start_date_time=datetime_helper(day=8, hour=11),
            end_date_time=datetime_helper(day=8, hour=12)
        ),
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report_refactor.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report()
    assert result["invalid_fields"] == "'status' column had timed out call status"


def test_get_call_pattern_report_returns_call_time_when_one_record_found(mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            dial_secs=600
        )]

    mocker.patch(
        "reports.interviewer_call_pattern_report_refactor.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report()
    assert result["call_time"] == "0:10:00"

def test_get_call_pattern_report_returns_call_time_when_multiple_records_are_found(mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            dial_secs=600
        ),
        interviewer_call_pattern_report_sample_case(
            dial_secs=300
        )
    ]
    mocker.patch(
        "reports.interviewer_call_pattern_report_refactor.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report()
    assert result["call_time"] == "0:15:00"

def test_get_call_pattern_report_returns_hours_on_call_as_percentage_of_worked_time_when_one_record_is_found(mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            start_date_time=datetime_helper(day=7, hour=10),
            end_date_time=datetime_helper(day=7, hour=11),
            dial_secs=600
        )]

    mocker.patch(
        "reports.interviewer_call_pattern_report_refactor.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report()
    assert result["hours_on_call_percentage"] == "16.67%"


def test_get_call_pattern_report_returns_hours_on_call_as_perecntage_of_worked_time_when_multiple_records_are_found(mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            start_date_time=datetime_helper(day=7, hour=10),
            end_date_time=datetime_helper(day=7, hour=11),
            dial_secs=600
        ),
        interviewer_call_pattern_report_sample_case(
            start_date_time=datetime_helper(day=7, hour=12),
            end_date_time=datetime_helper(day=7, hour=13),
            dial_secs=300
        )
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report_refactor.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report()
    assert result["hours_on_call_percentage"] == "8.33%"

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


def test_get_call_pattern_report_returns_the_number_and_percentage_of_refused_cases_when_case_refusals_are_found(mocker):
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
            "status": "Finished (Non response)",
            "survey": "OPN",
            "update_info": None,
            "wave": None,
        },
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report_refactor.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report()
    assert result["refusals"] == "1/2, 50.00%"

def test_get_call_pattern_report_returns_the_number_and_percentage_cases_with_a_status_of_no_conctact(mocker):
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
            "status": "Completed",
            "survey": "OPN",
            "update_info": None,
            "wave": None,
        },
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report_refactor.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report()
    assert result["no_contact"] == "1/2, 50.00%"


def test_get_call_pattern_report_returns_the_number_and_percentage_cases_with_a_status_of_completed(mocker):
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
            "status": "Completed",
            "survey": "OPN",
            "update_info": None,
            "wave": None,
        },
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
            "status": "Foo",
            "survey": "OPN",
            "update_info": None,
            "wave": None,
        },
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report_refactor.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report()
    assert result["completed_successfully"] == "1/2, 50.00%"


def test_get_call_pattern_report_returns_the_number_and_percentage_of_cases_with_a_status_of_appointment(mocker):
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
            "status": "Completed",
            "survey": "OPN",
            "update_info": None,
            "wave": None,
        },
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
            "status": "Finished (Appointment made)",
            "survey": "OPN",
            "update_info": None,
            "wave": None,
        },
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report_refactor.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report()
    assert result["appointments"] == "1/2, 50.00%"
