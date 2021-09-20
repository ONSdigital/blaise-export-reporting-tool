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


def test_get_call_pattern_report_returns_hours_worked_when_a_record_is_found(mocker, ):
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


def test_get_call_pattern_report_returns_timed_out_message_when_status_is_timed_out_when_there_is_no_end_time(mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            start_date_time=datetime_helper(day=7, hour=10),
            end_date_time=datetime_helper(day=7, hour=12),
        ),
        interviewer_call_pattern_report_sample_case(
            start_date_time=datetime_helper(day=7, hour=11),
            end_date_time=None,
            status="Timed out"
        ),
        interviewer_call_pattern_report_sample_case(
            start_date_time=datetime_helper(day=8, hour=10),
            end_date_time=None,
            status="Timed out"
        ),
        interviewer_call_pattern_report_sample_case(
            start_date_time=datetime_helper(day=8, hour=11),
            end_date_time=datetime_helper(day=8, hour=12),
        ),
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report_refactor.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report()
    assert result["invalid_fields"] == "'status' column had timed out call status"


def test_get_call_pattern_report_returns_message_when_no_end_time_found(mocker):
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
    assert result["invalid_fields"] == "'End call time' column had missing data"


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


def test_get_call_pattern_report_returns_hours_on_call_as_perecntage_of_worked_time_when_multiple_records_are_found(
        mocker):
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
        interviewer_call_pattern_report_sample_case(
            start_date_time=datetime_helper(day=7, hour=10),
            end_date_time=datetime_helper(day=7, hour=11),
            dial_secs=600
        ),]

    mocker.patch(
        "reports.interviewer_call_pattern_report_refactor.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report()
    assert result["average_calls_per_hour"] == 1.0


def test_get_call_pattern_report_returns_average_calls_per_hour_when_multiple_records_are_found(mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            start_date_time=datetime_helper(day=7, hour=10),
            end_date_time=datetime_helper(day=7, hour=11),
            dial_secs=600
        ),
        interviewer_call_pattern_report_sample_case(
            start_date_time=datetime_helper(day=7, hour=10),
            end_date_time=datetime_helper(day=7, hour=11),
            dial_secs=600
        ),
    ]
    mocker.patch(
        "reports.interviewer_call_pattern_report_refactor.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report()
    assert result["average_calls_per_hour"] == 2.0


def test_get_call_pattern_report_returns_the_number_and_percentage_of_refused_cases_when_case_refusals_are_found(
        mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            start_date_time=datetime_helper(day=7, hour=10),
            end_date_time=datetime_helper(day=7, hour=11),
            status="Completed"
        ),
        interviewer_call_pattern_report_sample_case(
            start_date_time=datetime_helper(day=7, hour=10),
            end_date_time=datetime_helper(day=7, hour=11),
            status="Finished (Non response)"
        ),
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report_refactor.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report()
    assert result["refusals"] == "1/2, 50.00%"


def test_get_call_pattern_report_returns_the_number_and_percentage_cases_with_a_status_of_no_contact(mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            start_date_time=datetime_helper(day=7, hour=10),
            end_date_time=datetime_helper(day=7, hour=11),
            status="Completed"
        ),
        interviewer_call_pattern_report_sample_case(
            start_date_time=datetime_helper(day=7, hour=10),
            end_date_time=datetime_helper(day=7, hour=11),
            status="Finished (No contact)"
        ),
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report_refactor.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report()
    assert result["no_contact"] == "1/2, 50.00%"


def test_get_call_pattern_report_returns_the_number_and_percentage_cases_with_a_status_of_completed(mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            start_date_time=datetime_helper(day=7, hour=10),
            end_date_time=datetime_helper(day=7, hour=11),
            status="Completed"
        ),
        interviewer_call_pattern_report_sample_case(
            start_date_time=datetime_helper(day=7, hour=10),
            end_date_time=datetime_helper(day=7, hour=11),
            status="Finished (No contact)"
        ),
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report_refactor.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report()
    assert result["completed_successfully"] == "1/2, 50.00%"


def test_get_call_pattern_report_returns_the_number_and_percentage_of_cases_with_a_status_of_appointment(mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            start_date_time=datetime_helper(day=7, hour=10),
            end_date_time=datetime_helper(day=7, hour=11),
            status="Completed"
        ),
        interviewer_call_pattern_report_sample_case(
            start_date_time=datetime_helper(day=7, hour=10),
            end_date_time=datetime_helper(day=7, hour=11),
            status="Finished (Appointment made)"
        ),
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report_refactor.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report()
    assert result["appointments"] == "1/2, 50.00%"

def test_get_call_pattern_report_returns_reason_for_no_contact_when_a_record_is_found_with_call_result_of_AnswerService(mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            start_date_time=datetime_helper(day=7, hour=10),
            end_date_time=datetime_helper(day=7, hour=11),
            status="Finished (No contact)",
            call_result="AnswerService"
        ),
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report_refactor.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report()
    assert result["no_contact_answer_service"] == "1/1, 100.00%"


def test_get_call_pattern_report_returns_reason_for_no_contact_when_multiple_records_are_found_and_one_record_has_a_call_result_of_AnswerService(mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            start_date_time=datetime_helper(day=7, hour=10),
            end_date_time=datetime_helper(day=7, hour=11),
            status="Finished (No contact)",
            call_result="AnswerService"
        ),
        interviewer_call_pattern_report_sample_case(
            start_date_time=datetime_helper(day=7, hour=10),
            end_date_time=datetime_helper(day=7, hour=11),
        ),
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report_refactor.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report()
    assert result["no_contact_answer_service"] == "1/2, 50.00%"


def test_get_call_pattern_report_returns_reason_for_no_contact_when_a_record_is_found_with_call_result_of_Busy(mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            start_date_time=datetime_helper(day=7, hour=10),
            end_date_time=datetime_helper(day=7, hour=11),
            status="Finished (No contact)",
            call_result="Busy"
        ),
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report_refactor.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report()
    assert result["no_contact_busy"] == "1/1, 100.00%"


def test_get_call_pattern_report_returns_reason_for_no_contact_when_multiple_records_are_found_and_one_record_has_a_call_result_of_Busy(mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            start_date_time=datetime_helper(day=7, hour=10),
            end_date_time=datetime_helper(day=7, hour=11),
            status="Finished (No contact)",
            call_result="Busy"
        ),
        interviewer_call_pattern_report_sample_case(
            start_date_time=datetime_helper(day=7, hour=10),
            end_date_time=datetime_helper(day=7, hour=11),
        ),
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report_refactor.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report()
    assert result["no_contact_busy"] == "1/2, 50.00%"

def test_get_call_pattern_report_returns_reason_for_no_contact_when_a_record_is_found_with_call_result_of_Disconnect(mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            start_date_time=datetime_helper(day=7, hour=10),
            end_date_time=datetime_helper(day=7, hour=11),
            status="Finished (No contact)",
            call_result="Disconnect"
        ),
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report_refactor.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report()
    assert result["no_contact_disconnect"] == "1/1, 100.00%"


def test_get_call_pattern_report_returns_reason_for_no_contact_when_multiple_records_are_found_and_one_record_has_a_call_result_of_Disconnect(mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            start_date_time=datetime_helper(day=7, hour=10),
            end_date_time=datetime_helper(day=7, hour=11),
            status="Finished (No contact)",
            call_result="Disconnect"
        ),
        interviewer_call_pattern_report_sample_case(
            start_date_time=datetime_helper(day=7, hour=10),
            end_date_time=datetime_helper(day=7, hour=11),
        ),
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report_refactor.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report()
    assert result["no_contact_disconnect"] == "1/2, 50.00%"

def test_get_call_pattern_report_returns_reason_for_no_contact_when_a_record_is_found_with_call_result_of_NoAnswer(mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            start_date_time=datetime_helper(day=7, hour=10),
            end_date_time=datetime_helper(day=7, hour=11),
            status="Finished (No contact)",
            call_result="Disconnect"
        ),
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report_refactor.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report()
    assert result["no_contact_disconnect"] == "1/1, 100.00%"