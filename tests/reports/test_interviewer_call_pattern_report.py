import pandas as pd
import pytest

from tests.helpers.interviewer_call_pattern_helpers import interviewer_call_pattern_report_sample_case, datetime_helper
from reports.interviewer_call_pattern_report import *
from models.error_capture import BertException

interviewer = "James"
start_date_as_string = "2021-09-22"
end_date_as_string = "2021-09-22"
survey_tla = "OPN"


def test_get_call_pattern_report_returns_an_empty_dict_if_no_records_were_found(mocker):
    mocker.patch(
        "reports.interviewer_call_pattern_report.get_call_history_records",
        return_value=pd.DataFrame(),
    )

    assert get_call_pattern_report(interviewer, start_date_as_string, end_date_as_string, survey_tla) == {}


def test_get_call_pattern_report_returns_hours_worked_when_a_record_is_found(mocker):
    datastore_records = [interviewer_call_pattern_report_sample_case(
        call_start_time=datetime_helper(day=7, hour=9),
        call_end_time=datetime_helper(day=7, hour=15)
    )]

    mocker.patch(
        "reports.interviewer_call_pattern_report.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))
    result = get_call_pattern_report(interviewer, start_date_as_string, end_date_as_string, survey_tla)
    assert result.hours_worked == "06:00:00"


def test_get_call_pattern_report_returns_hours_worked_when_multiple_records_from_a_single_day_are_found(mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=9),
            call_end_time=datetime_helper(day=7, hour=15)
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=16),
            call_end_time=datetime_helper(day=7, hour=17)
        ),
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report(interviewer, start_date_as_string, end_date_as_string, survey_tla)
    assert result.hours_worked == "08:00:00"


def test_get_call_pattern_report_returns_hours_worked_when_call_time_greater_than_24_hours(mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=0),
            call_end_time=datetime_helper(day=8, hour=1)
        ),
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report(interviewer, start_date_as_string, end_date_as_string, survey_tla)
    assert result.hours_worked == "25:00:00"


def test_get_call_pattern_report_returns_hours_worked_when_multiple_records_from_multiple_days_are_found(mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=8),
            call_end_time=datetime_helper(day=7, hour=10)
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=11),
            call_end_time=datetime_helper(day=7, hour=12)
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=8, hour=9),
            call_end_time=datetime_helper(day=8, hour=11)
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=8, hour=12),
            call_end_time=datetime_helper(day=8, hour=14)
        ),
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report(interviewer, start_date_as_string, end_date_as_string, survey_tla)
    assert result.hours_worked == "09:00:00"


def test_get_call_pattern_report_ignores_record_when_no_start_call_time_from_a_single_day_is_found(mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=10),
            call_end_time=datetime_helper(day=7, hour=12)
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=None,
            call_end_time=datetime_helper(day=7, hour=14)
        ),
    ]
    mocker.patch(
        "reports.interviewer_call_pattern_report.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report(interviewer, start_date_as_string, end_date_as_string, survey_tla)
    assert result.hours_worked == "02:00:00"


def test_get_call_pattern_report_ignores_record_when_no_end_call_time_from_a_single_day_is_found(mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=10),
            call_end_time=datetime_helper(day=7, hour=12)
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=11),
            call_end_time=None
        ),
    ]
    mocker.patch(
        "reports.interviewer_call_pattern_report.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report(interviewer, start_date_as_string, end_date_as_string, survey_tla)
    assert result.hours_worked == "02:00:00"


def test_get_call_pattern_report_ignores_record_when_no_start_call_time_from_multiple_days_are_found(mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=9),
            call_end_time=datetime_helper(day=7, hour=12)
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=None,
            call_end_time=datetime_helper(day=7, hour=14)
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=None,
            call_end_time=datetime_helper(day=8, hour=10)
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=8, hour=11),
            call_end_time=datetime_helper(day=8, hour=12)
        ),
    ]
    mocker.patch(
        "reports.interviewer_call_pattern_report.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report(interviewer, start_date_as_string, end_date_as_string, survey_tla)
    assert result.hours_worked == "04:00:00"


def test_get_call_pattern_report_ignores_record_when_no_end_call_time_from_multiple_days_are_found(mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=10),
            call_end_time=datetime_helper(day=7, hour=12)
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=11),
            call_end_time=None
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=8, hour=10),
            call_end_time=None
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=8, hour=11),
            call_end_time=datetime_helper(day=8, hour=12)
        ),
    ]
    mocker.patch(
        "reports.interviewer_call_pattern_report.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report(interviewer, start_date_as_string, end_date_as_string, survey_tla)
    assert result.hours_worked == "03:00:00"


def test_get_call_pattern_report_returns_the_number_and_percentage_of_cases_where_no_start_call_time_is_found(mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=10),
            call_end_time=datetime_helper(day=7, hour=12)
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=None,
            call_end_time=datetime_helper(day=7, hour=14)
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=None,
            call_end_time=datetime_helper(day=8, hour=10)
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=None,
            call_end_time=datetime_helper(day=8, hour=11)
        ),
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report(interviewer, start_date_as_string, end_date_as_string, survey_tla)
    assert result.discounted_invalid_cases == 3


def test_get_call_pattern_report_returns_the_number_and_percentage_of_cases_where_no_end_call_time_is_found(mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=10),
            call_end_time=datetime_helper(day=7, hour=12)
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=11),
            call_end_time=None
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=8, hour=10),
            call_end_time=None
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=8, hour=11),
            call_end_time=None
        ),
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report(interviewer, start_date_as_string, end_date_as_string, survey_tla)
    assert result.discounted_invalid_cases == 3


def test_get_call_pattern_report_returns_expected_message_when_no_start_time_found(mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=10),
            call_end_time=datetime_helper(day=7, hour=12)
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=None,
            call_end_time=datetime_helper(day=7, hour=14)
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=None,
            call_end_time=datetime_helper(day=8, hour=10)
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=8, hour=11),
            call_end_time=datetime_helper(day=8, hour=12)
        ),
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report(interviewer, start_date_as_string, end_date_as_string, survey_tla)
    assert result.invalid_fields == "'call_start_time' column had missing data"


def test_get_call_pattern_report_returns_no_message_when_no_invalid_records_are_found(mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=10),
            call_end_time=datetime_helper(day=7, hour=12)
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=13),
            call_end_time=datetime_helper(day=7, hour=14)
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=8, hour=9),
            call_end_time=datetime_helper(day=8, hour=10)
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=8, hour=11),
            call_end_time=datetime_helper(day=8, hour=12)
        ),
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report(interviewer, start_date_as_string, end_date_as_string, survey_tla)
    assert result.invalid_fields == ""


def test_get_call_pattern_report_returns_expected_message_when_no_end_time_found(mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=10),
            call_end_time=datetime_helper(day=7, hour=12)
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=11),
            call_end_time=None
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=8, hour=10),
            call_end_time=None
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=8, hour=11),
            call_end_time=datetime_helper(day=8, hour=12)
        ),
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report(interviewer, start_date_as_string, end_date_as_string, survey_tla)
    assert result.invalid_fields == "'call_end_time' column had missing data"


def test_get_call_pattern_report_returns_expected_message_when_no_start_or_end_time_found(mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=10),
            call_end_time=datetime_helper(day=7, hour=12)
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=11),
            call_end_time=None
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=None,
            call_end_time=None
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=8, hour=11),
            call_end_time=datetime_helper(day=8, hour=12)
        ),
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report(interviewer, start_date_as_string, end_date_as_string, survey_tla)
    list_of_reasons = result.invalid_fields.split(", ")

    assert len(list_of_reasons) == 2
    assert "'call_start_time' column had missing data" in list_of_reasons
    assert "'call_end_time' column had missing data" in list_of_reasons


def test_get_call_pattern_report_returns_call_time_when_one_record_found(mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            dial_secs=600
        )]

    mocker.patch(
        "reports.interviewer_call_pattern_report.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report(interviewer, start_date_as_string, end_date_as_string, survey_tla)
    assert result.call_time == "00:10:00"


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
        "reports.interviewer_call_pattern_report.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report(interviewer, start_date_as_string, end_date_as_string, survey_tla)
    assert result.call_time == "00:15:00"


def test_get_call_pattern_report_returns_hours_on_call_as_percentage_of_worked_time_when_one_record_is_found(mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=10),
            call_end_time=datetime_helper(day=7, hour=11),
            dial_secs=600
        )]

    mocker.patch(
        "reports.interviewer_call_pattern_report.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report(interviewer, start_date_as_string, end_date_as_string, survey_tla)
    assert result.hours_on_calls_percentage == 16.67


def test_get_call_pattern_report_returns_hours_on_call_as_perecntage_of_worked_time_when_multiple_records_are_found(
        mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=10),
            call_end_time=datetime_helper(day=7, hour=11),
            dial_secs=600
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=12),
            call_end_time=datetime_helper(day=7, hour=13),
            dial_secs=300
        )
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report(interviewer, start_date_as_string, end_date_as_string, survey_tla)
    assert result.hours_on_calls_percentage == 8.33


def test_get_call_pattern_report_returns_average_calls_per_hour(mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=10),
            call_end_time=datetime_helper(day=7, hour=11),
            dial_secs=600
        ), ]

    mocker.patch(
        "reports.interviewer_call_pattern_report.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report(interviewer, start_date_as_string, end_date_as_string, survey_tla)
    assert result.average_calls_per_hour == 1.0


def test_get_call_pattern_report_returns_average_calls_per_hour_when_multiple_records_are_found(mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=10),
            call_end_time=datetime_helper(day=7, hour=11),
            dial_secs=600
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=10),
            call_end_time=datetime_helper(day=7, hour=11),
            dial_secs=600
        ),
    ]
    mocker.patch(
        "reports.interviewer_call_pattern_report.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report(interviewer, start_date_as_string, end_date_as_string, survey_tla)
    assert result.average_calls_per_hour == 2.0


def test_get_call_pattern_report_returns_the_number_and_percentage_of_refused_cases_when_case_refusals_are_found(
        mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=10),
            call_end_time=datetime_helper(day=7, hour=11),
            status="Completed"
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=10),
            call_end_time=datetime_helper(day=7, hour=11),
            status="Finished (Non response)"
        ),
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report(interviewer, start_date_as_string, end_date_as_string, survey_tla)
    assert result.refusals == 1


def test_get_call_pattern_report_returns_the_number_and_percentage_cases_with_a_status_of_no_contact(mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=10),
            call_end_time=datetime_helper(day=7, hour=11),
            status="Completed"
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=10),
            call_end_time=datetime_helper(day=7, hour=11),
            status="Finished (No contact)"
        ),
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report(interviewer, start_date_as_string, end_date_as_string, survey_tla)
    assert result.no_contacts == 1


def test_get_call_pattern_report_returns_the_number_and_percentage_cases_with_a_status_of_completed(mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=10),
            call_end_time=datetime_helper(day=7, hour=11),
            status="Completed"
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=10),
            call_end_time=datetime_helper(day=7, hour=11),
            status="Finished (No contact)"
        ),
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report(interviewer, start_date_as_string, end_date_as_string, survey_tla)
    assert result.completed_successfully == 1


def test_get_call_pattern_report_returns_the_number_and_percentage_of_cases_with_a_status_of_appointment(mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=10),
            call_end_time=datetime_helper(day=7, hour=11),
            status="Completed"
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=10),
            call_end_time=datetime_helper(day=7, hour=11),
            status="Finished (Appointment made)"
        ),
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report(interviewer, start_date_as_string, end_date_as_string, survey_tla)
    assert result.appointments_for_contacts == 1


def test_get_call_pattern_report_returns_the_number_and_percentage_of_cases_with_a_status_of_AnswerService(mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=10),
            call_end_time=datetime_helper(day=7, hour=11),
            status="Finished (No contact)",
            call_result="AnswerService"
        ),
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report(interviewer, start_date_as_string, end_date_as_string, survey_tla)
    assert result.no_contact_answer_service == 1


def test_get_call_pattern_report_returns_the_number_and_percentage_of_cases_with_a_status_of_AnswerService_when_multiple_records_are_found(mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=10),
            call_end_time=datetime_helper(day=7, hour=11),
            status="Finished (No contact)",
            call_result="AnswerService"
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=12),
            call_end_time=datetime_helper(day=7, hour=13),
            status="Finished (No contact)",
            call_result="Busy"
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=14),
            call_end_time=datetime_helper(day=7, hour=15),
        ),
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report(interviewer, start_date_as_string, end_date_as_string, survey_tla)
    assert result.no_contact_answer_service == 1


def test_get_call_pattern_report_returns_the_number_and_percentage_of_cases_with_a_status_of_Busy(mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=10),
            call_end_time=datetime_helper(day=7, hour=11),
            status="Finished (No contact)",
            call_result="Busy"
        ),
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report(interviewer, start_date_as_string, end_date_as_string, survey_tla)
    assert result.no_contact_busy == 1


def test_get_call_pattern_report_returns_the_number_and_percentage_of_cases_with_a_status_of_Busy_when_multiple_records_are_found(
        mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=10),
            call_end_time=datetime_helper(day=7, hour=11),
            status="Finished (No contact)",
            call_result="AnswerService"
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=12),
            call_end_time=datetime_helper(day=7, hour=13),
            status="Finished (No contact)",
            call_result="Busy"
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=14),
            call_end_time=datetime_helper(day=7, hour=15),
        ),
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report(interviewer, start_date_as_string, end_date_as_string, survey_tla)
    assert result.no_contact_busy == 1


def test_get_call_pattern_report_returns_the_number_and_percentage_of_cases_with_a_status_of_Disconnect(mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=10),
            call_end_time=datetime_helper(day=7, hour=11),
            status="Finished (No contact)",
            call_result="Disconnect"
        ),
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report(interviewer, start_date_as_string, end_date_as_string, survey_tla)
    assert result.no_contact_disconnect == 1


def test_get_call_pattern_report_returns_the_number_and_percentage_of_cases_with_a_status_of_Disconnect_when_multiple_records_are_found(mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=10),
            call_end_time=datetime_helper(day=7, hour=11),
            status="Finished (No contact)",
            call_result="AnswerService"
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=12),
            call_end_time=datetime_helper(day=7, hour=13),
            status="Finished (No contact)",
            call_result="Disconnect"
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=14),
            call_end_time=datetime_helper(day=7, hour=15),
        ),
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report(interviewer, start_date_as_string, end_date_as_string, survey_tla)
    assert result.no_contact_disconnect == 1



def test_get_call_pattern_report_returns_the_number_and_percentage_of_cases_with_a_status_of_NoAnswer(mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=10),
            call_end_time=datetime_helper(day=7, hour=11),
            status="Finished (No contact)",
            call_result="NoAnswer",
        ),
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report(interviewer, start_date_as_string, end_date_as_string, survey_tla)
    assert result.no_contact_no_answer == 1


def test_get_call_pattern_report_returns_the_number_and_percentage_of_cases_with_a_status_of_NoAnswer_when_multiple_records_are_found(
        mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=10),
            call_end_time=datetime_helper(day=7, hour=11),
            status="Finished (No contact)",
            call_result="AnswerService",
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=12),
            call_end_time=datetime_helper(day=7, hour=13),
            status="Finished (No contact)",
            call_result="NoAnswer",
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=14),
            call_end_time=datetime_helper(day=7, hour=15),
        ),
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report(interviewer, start_date_as_string, end_date_as_string, survey_tla)
    assert result.no_contact_no_answer == 1


def test_get_call_pattern_report_returns_the_number_and_percentage_of_cases_with_a_status_of_Other(
        mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=10),
            call_end_time=datetime_helper(day=7, hour=11),
            status="Finished (No contact)",
            call_result="Others",
        ),
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report(interviewer, start_date_as_string, end_date_as_string, survey_tla)
    assert result.no_contact_other == 1


def test_get_call_pattern_report_returns_the_number_and_percentage_of_cases_with_a_status_of_Other_when_multiple_records_are_found(
        mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=10),
            call_end_time=datetime_helper(day=7, hour=11),
            status="Finished (No contact)",
            call_result="AnswerService",
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=12),
            call_end_time=datetime_helper(day=7, hour=13),
            status="Finished (No contact)",
            call_result="Others",
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=14),
            call_end_time=datetime_helper(day=7, hour=15),
        ),
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report(interviewer, start_date_as_string, end_date_as_string, survey_tla)
    assert result.no_contact_other == 1


def test_get_call_pattern_report_returns_expected_when_call_time_is_greater_than_hours_worked(mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=10),
            call_end_time=datetime_helper(day=7, hour=11),
            dial_secs=3600,
            status="Timed out during questionnaire"
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=10),
            call_end_time=datetime_helper(day=7, hour=11),
            dial_secs=3600
        )
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report(interviewer, start_date_as_string, end_date_as_string, survey_tla)
    assert result.hours_worked == "01:00:00"
    assert result.call_time == "01:00:00"
    assert result.invalid_fields == "'status' column had timed out call status"
    assert result.discounted_invalid_cases == 1


def test_get_call_pattern_report_returns_expected_output_when_all_data_is_valid(mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=10),
            call_end_time=datetime_helper(day=7, hour=11),
            dial_secs=600,
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=13),
            call_end_time=datetime_helper(day=7, hour=14),
            dial_secs=1200,
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=16),
            call_end_time=datetime_helper(day=7, hour=17),
            dial_secs=600,
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=8, hour=9),
            call_end_time=datetime_helper(day=8, hour=10),
            dial_secs=300,
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=8, hour=12),
            call_end_time=datetime_helper(day=8, hour=13),
            dial_secs=600,
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=8, hour=15),
            call_end_time=datetime_helper(day=8, hour=16),
            dial_secs=1200,
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=9, hour=10),
            call_end_time=datetime_helper(day=9, hour=11),
            dial_secs=600,
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=9, hour=13),
            call_end_time=datetime_helper(day=9, hour=14),
            dial_secs=300,
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=9, hour=16),
            call_end_time=datetime_helper(day=9, hour=17),
            dial_secs=600,
        ),
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report(interviewer, start_date_as_string, end_date_as_string, survey_tla)
    assert result.hours_worked == "21:00:00"
    assert result.call_time == "01:40:00"
    assert result.hours_on_calls_percentage == 7.94
    assert result.average_calls_per_hour == 0.43
    assert result.refusals == 0
    assert result.no_contacts == 0
    assert result.completed_successfully == 9
    assert result.appointments_for_contacts == 0
    assert result.no_contact_answer_service == 0
    assert result.no_contact_busy == 0
    assert result.no_contact_disconnect == 0
    assert result.no_contact_no_answer == 0
    assert result.no_contact_other == 0
    assert result.discounted_invalid_cases == 0
    assert result.invalid_fields == ""


def test_get_valid_records_returns_an_empty_dataframe_when_no_valid_records_are_found():
    arrange = pd.DataFrame([{
        "call_start_time": datetime_helper(day=7, hour=9),
        "call_end_time": None,
    }])
    result = get_valid_records(arrange)
    assert result.empty


def test_get_valid_records_returns_an_empty_dataframe_when_a_record_is_found_with_a_timed_out_status():
    arrange = pd.DataFrame([{
        "call_start_time": datetime_helper(day=7, hour=9),
        "call_end_time": datetime_helper(day=7, hour=10),
        "status": "Timed out"
    }])
    result = get_valid_records(arrange)
    assert result.empty


def test_get_call_pattern_report_returns_expected_output_when_invalid_data_are_found(mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=8, hour=11),
            call_end_time=datetime_helper(day=8, hour=13),
            dial_secs=600,
            status="Completed"
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=8, hour=15),
            call_end_time=datetime_helper(day=8, hour=17),
            dial_secs=600,
            status="Completed"
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=9, hour=11),
            call_end_time=datetime_helper(day=9, hour=13),
            dial_secs=600,
            status="Completed"
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=9, hour=15),
            call_end_time=datetime_helper(day=9, hour=17),
            dial_secs=600,
            status="Completed"
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=10, hour=11),
            call_end_time=datetime_helper(day=10, hour=13),
            dial_secs=600,
            status="Completed"
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=10, hour=15),
            call_end_time=datetime_helper(day=10, hour=17),
            dial_secs=600,
            status="Completed"
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=None,
            call_end_time=datetime_helper(day=8, hour=10),
            dial_secs=600,
            status="Error"
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=9, hour=9),
            call_end_time=None,
            dial_secs=600,
            status="Error"
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=10, hour=9),
            call_end_time=None,
            dial_secs=600,
            status="Timed out"
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=10, hour=11),
            call_end_time=datetime_helper(day=10, hour=13),
            dial_secs=600,
            status="Timed out during questionnaire"
        ),
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report(interviewer, start_date_as_string, end_date_as_string, survey_tla)
    assert result.total_valid_cases == 6
    assert result.hours_worked == "18:00:00"
    assert result.call_time == "01:00:00"
    assert result.hours_on_calls_percentage == 5.56
    assert result.average_calls_per_hour == 0.33
    assert result.refusals == 0
    assert result.no_contacts == 0
    assert result.completed_successfully == 6
    assert result.appointments_for_contacts == 0
    assert result.no_contact_answer_service == 0
    assert result.no_contact_busy == 0
    assert result.no_contact_disconnect == 0
    assert result.no_contact_no_answer == 0
    assert result.no_contact_other == 0
    assert result.discounted_invalid_cases == 4

    list_of_reasons = result.invalid_fields.split(", ")

    assert len(list_of_reasons) == 3
    assert "'call_start_time' column had missing data" in list_of_reasons
    assert "'call_end_time' column had missing data" in list_of_reasons
    assert "'status' column had timed out call status" in list_of_reasons


def test_get_call_pattern_report_returns_expected_output_when_only_invalid_data_are_found(mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=9, hour=9),
            call_end_time=None,
            dial_secs=600,
            status="Error"
        ),
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report(interviewer, start_date_as_string, end_date_as_string, survey_tla)
    assert result.discounted_invalid_cases == 1

    list_of_reasons = result.invalid_fields.split(",")

    assert len(list_of_reasons) == 1
    assert "'call_end_time' column had missing data" in list_of_reasons


def test_get_call_pattern_report_returns_a_single_reason_message_when_no_call_end_time_is_found_and_status_is_not_timed_out(mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=10),
            call_end_time=datetime_helper(day=7, hour=12),
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=11),
            call_end_time=None,
            status="Completed"
        ),
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report(interviewer, start_date_as_string, end_date_as_string, survey_tla)
    assert result.invalid_fields == "'call_end_time' column had missing data"


def test_get_call_pattern_report_returns_multiple_reason_messages_when_no_call_end_time_is_found_and_status_is_timed_out(mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=10),
            call_end_time=datetime_helper(day=7, hour=12),
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=11),
            call_end_time=None,
            status="Timed out"
        ),
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report(interviewer, start_date_as_string, end_date_as_string, survey_tla)
    list_of_reasons = result.invalid_fields.split(", ")

    assert len(list_of_reasons) == 2
    assert "'call_end_time' column had missing data" in list_of_reasons
    assert "'status' column had timed out call status" in list_of_reasons


def test_get_call_pattern_report_returns_unique_reasons_when_multiple_cases_with_similar_conditions_are_found(mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=10),
            call_end_time=datetime_helper(day=7, hour=12),
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=11),
            call_end_time=None,
            status="Timed out"
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=11),
            call_end_time=None,
            status="Timed out"
        ),
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report(interviewer, start_date_as_string, end_date_as_string, survey_tla)
    list_of_reasons = result.invalid_fields.split(", ")

    assert len(list_of_reasons) == 2
    assert "'call_end_time' column had missing data" in list_of_reasons
    assert "'status' column had timed out call status" in list_of_reasons


def test_get_call_pattern_report_returns_total_valid_records_found(mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(),
        interviewer_call_pattern_report_sample_case(),
        interviewer_call_pattern_report_sample_case(),
    ]

    mocker.patch("reports.interviewer_call_pattern_report.get_call_history_records",
                 return_value=pd.DataFrame(datastore_records))
    result = get_call_pattern_report(interviewer, start_date_as_string, end_date_as_string, survey_tla)

    assert result.total_valid_cases == 3


@pytest.mark.parametrize(
    "dial_secs, expected",
    [
        ([600, 600], 1200),
        ([100, 100, 200], 400),
        ([200, 400, 400, 200], 1200),
    ],
)
def test_calculate_call_time_in_seconds(dial_secs, expected):
    assert calculate_call_time_in_seconds(pd.DataFrame({"dial_secs": dial_secs})) == expected


def test_calculate_call_time_in_seconds_raises_error_when_no_dial_secs_column():
    arrange = pd.DataFrame([{
        "secs": 800,
    }])

    with pytest.raises(BertException) as excinfo:
        calculate_call_time_in_seconds(arrange)
    assert "calculate_call_time_in_seconds failed" in excinfo.value.message


@pytest.mark.parametrize(
    "call_start_time, call_end_time, expected",
    [
        (datetime_helper(day=7, hour=9), datetime_helper(day=7, hour=10), 3600),
        (datetime_helper(day=7, hour=11), datetime_helper(day=7, hour=13), 7200),
        (datetime_helper(day=7, hour=14), datetime_helper(day=7, hour=17), 10800),
    ],
)
def test_calculate_hours_worked_in_seconds(call_start_time, call_end_time, expected):
    assert calculate_hours_worked_in_seconds(pd.DataFrame([interviewer_call_pattern_report_sample_case(
        call_start_time=call_start_time,
        call_end_time=call_end_time,
    )])) == expected


def test_calculate_hours_worked_in_seconds_raises_error_when_no_call_end_time_column():
    arrange = pd.DataFrame([{
        "call_start_time": datetime_helper(day=7, hour=9),
        "end_time": datetime_helper(day=7, hour=10),
    }])

    with pytest.raises(BertException) as excinfo:
        calculate_hours_worked_in_seconds(arrange)
    assert "calculate_hours_worked_in_seconds failed" in excinfo.value.message


@pytest.mark.parametrize(
    "call_start_time, call_end_time, expected",
    [
        (datetime_helper(day=7, hour=9), datetime_helper(day=7, hour=10), 1.00),
        (datetime_helper(day=7, hour=11), datetime_helper(day=7, hour=13), 0.50),
        (datetime_helper(day=7, hour=14), datetime_helper(day=7, hour=17), 0.33),
    ],
)
def test_calculate_average_calls_per_hour(call_start_time, call_end_time, expected):
    assert calculate_average_calls_per_hour(pd.DataFrame([interviewer_call_pattern_report_sample_case(
        call_start_time=call_start_time,
        call_end_time=call_end_time,
    )])) == expected


@pytest.mark.parametrize(
    "status_1, status_2, status_3, status_to_test, expected",
    [
        ("Completed", "Completed", "Timed out", "Completed", 2),
        ("Completed", "Timed out", "Timed out", "Completed", 1),
        ("Completed", "Completed", "Timed out", "Timed out", 1),
    ],
)
def test_number_of_records_which_has_status(status_1, status_2, status_3, status_to_test, expected):
    arrange = pd.DataFrame([
        interviewer_call_pattern_report_sample_case(status=status_1),
        interviewer_call_pattern_report_sample_case(status=status_2),
        interviewer_call_pattern_report_sample_case(status=status_3),
    ])
    assert number_of_records_which_has_status(arrange, status_to_test) == expected


def test_number_of_records_which_has_status_raises_error_when_no_status_column_found():
    arrange = pd.DataFrame([{
        "call_status": "Completed"
    }])

    with pytest.raises(BertException) as excinfo:
        number_of_records_which_has_status(arrange, "Completed")
    assert "number_of_records_which_has_status failed" in excinfo.value.message


def test_calculate_hours_on_call_percentage_returns_a_value_to_two_dp_when_value_is_divisible_by_ten(mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=10),
            call_end_time=datetime_helper(day=7, hour=11),
            dial_secs=1800
        ),
    ]

    mocker.patch(
        "reports.interviewer_call_pattern_report.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report(interviewer, start_date_as_string, end_date_as_string, survey_tla)
    assert result.hours_on_calls_percentage == 50.0


def test_percentages_equal_one_hundred(mocker):
    datastore_records = [
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=10),
            call_end_time=datetime_helper(day=7, hour=11),
            dial_secs=1800,
            status="Completed",
            call_result="Questionnaire",
            outcome_code=120,
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=10),
            call_end_time=datetime_helper(day=7, hour=11),
            dial_secs=1800,
            status="Completed",
            call_result="Questionnaire",
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=10),
            call_end_time=datetime_helper(day=7, hour=11),
            dial_secs=1800,
            status="Finished (No contact)",
            call_result="Disconnect",
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=10),
            call_end_time=datetime_helper(day=7, hour=11),
            dial_secs=1800,
            status="Finished (Non response)",
            call_result="NonRespons",
        ),
        interviewer_call_pattern_report_sample_case(
            call_start_time=datetime_helper(day=7, hour=10),
            call_end_time=datetime_helper(day=7, hour=11),
            dial_secs=1800,
            status="Finished (Appointment made)",
            call_result="Appointment",
        )]

    mocker.patch(
        "reports.interviewer_call_pattern_report.get_call_history_records",
        return_value=pd.DataFrame(datastore_records))

    result = get_call_pattern_report(interviewer, start_date_as_string, end_date_as_string, survey_tla)

    def calculate_percentage(numerator: int, denominator: int) -> float:
        return numerator/denominator * 100

    completed_successfully_percentage = calculate_percentage(result.completed_successfully, result.total_valid_cases)
    appointments_for_contacts_percentage = calculate_percentage(result.appointments_for_contacts, result.total_valid_cases)
    no_contacts_percentage = calculate_percentage(result.no_contacts, result.total_valid_cases)
    refusals_percentage = calculate_percentage(result.refusals, result.total_valid_cases)
    webnudge_percentage = calculate_percentage(result.webnudge, result.total_valid_cases)

    assert (completed_successfully_percentage + appointments_for_contacts_percentage +
            no_contacts_percentage + refusals_percentage + webnudge_percentage) == 100


def test_webnudge():
    arrange = pd.DataFrame([
        interviewer_call_pattern_report_sample_case(call_result="WebNudge"),
    ])
    assert webnudge(arrange) == 1


def test_webnudge_raises_bert_exception():
    with pytest.raises(BertException) as err:
        webnudge(pd.DataFrame())

    assert err.value.message == "webnudge() failed: 'call_result'"
    assert err.value.code == 400


@pytest.mark.parametrize(
    "status, call_result, expected",
    [
        ("Completed", "Questionnaire", 1),
        ("Completed", "WebNudge", 0),
    ],
)
def test_total_records_with_completed_status_should_drop_completed_if_webnudges(status, call_result, expected):
    arrange = pd.DataFrame([
        interviewer_call_pattern_report_sample_case(status=status, call_result=call_result),
    ])
    assert total_records_with_status(arrange, "Completed") == expected
