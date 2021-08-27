import numpy as np
import pandas as pd
import pytest

from functions.validate_call_pattern_report import get_invalid_fields
from models.interviewer_call_pattern_model import InterviewerCallPattern, InterviewerCallPatternWithNoValidData
from reports.interviewer_call_pattern_report import (
    generate_report, create_dataframe, results_for_calls_with_status,
    get_call_pattern_report, get_invalid_data, no_contact_breakdown,
    validate_dataframe
)


@pytest.mark.skip("")
def test_get_call_pattern_report_returns_an_empty_dict_if_no_records_were_found(mocker):
    mocker.patch("reports.interviewer_call_pattern_report.get_call_history_records", return_value=[])

    assert get_call_pattern_report("el4president", "2022-24-01", "2022-24-01", "LMS") == {}


@pytest.mark.skip("eurgh!")
def test_get_call_pattern_report_returns_a_report_with_only_the_discounted_information_when_data_is_completely_invalid(
        call_history_with_all_invalid_records, dataframe_with_all_invalid_fields, mocker):
    mock_interviewer_name = 'el4president'
    mock_date = '2022-01-24'

    mocker.patch("reports.interviewer_call_pattern_report.get_call_history_records",
                 return_value=call_history_with_all_invalid_records)
    mocker.patch("reports.interviewer_call_pattern_report.validate_dataframe",
                 return_value=(pd.DataFrame(), dataframe_with_all_invalid_fields))

    numerator = len(dataframe_with_all_invalid_fields.index)
    denominator = len(call_history_with_all_invalid_records)
    percentage = 100 * numerator / denominator

    assert get_call_pattern_report(
        mock_interviewer_name, mock_date, mock_date, None) == InterviewerCallPatternWithNoValidData(
        discounted_invalid_cases=f"{numerator}/{denominator}, {percentage}%",
        invalid_fields=get_invalid_fields(dataframe_with_all_invalid_fields)
    )


@pytest.mark.skip("eurgh!")
def test_get_call_pattern_report(call_history_records, mocker):
    mocker.patch("reports.interviewer_call_pattern_report.get_call_history_records", return_value=call_history_records)

    assert get_call_pattern_report(
        "el4president",
        "2022-24-01",
        "2022-24-01",
        "LOL"

    ) == InterviewerCallPattern(
        hours_worked='0:16:53',
        call_time='0:16:11',
        hours_on_calls_percentage='95.85%',
        average_calls_per_hour=56.86,
        respondents_interviewed=17,
        average_respondents_interviewed_per_hour=60.41,
        refusals='1/17, 5.88%',
        no_contacts='14/17, 82.35%',
        completed_successfully='1/17, 5.88%',
        appointments_for_contacts='0/17, 0.0%',
        no_contact_answer_service='1/14, 7.14%',
        no_contact_busy='5/14, 35.71%',
        no_contact_disconnect='3/14, 21.43%',
        no_contact_no_answer='5/14, 35.71%',
        no_contact_other='0/14, 0.0%',
        discounted_invalid_cases='1/17, 5.882352941176471%',
        invalid_fields="'call_end_time' column had missing data")


def test_create_and_parse_dataframe_returns_a_dataframe(call_history_records):
    df = create_dataframe(call_history_records)
    assert type(df) == pd.DataFrame


def test_create_and_parse_dataframe_returns_a_dataframe_with_lower_case_headers(call_history_records):
    df = create_dataframe(call_history_records)
    assert df.columns.str.islower


@pytest.mark.skip("huff")
def test_create_and_parse_dataframe_has_converted_necessary_columns_to_the_correct_types(call_history_records):
    df = create_dataframe(call_history_records)
    assert df['number_of_interviews'].dtype == np.int32
    assert df['dial_secs'].dtype == np.float64
    assert str(df['call_start_time'].dtype) == 'datetime64[ns, UTC]'
    assert str(df['call_end_time'].dtype) == 'datetime64[ns, UTC]'


def test_generate_report(valid_dataframe):
    assert generate_report(valid_dataframe, 10) == InterviewerCallPattern(hours_worked='0:48:05', call_time='0:12:50', hours_on_calls_percentage='26.69%', average_calls_per_hour=12.48, respondents_interviewed=10, average_respondents_interviewed_per_hour=12.48, refusals='2/10, 20.0%', no_contacts='0/10, 0.0%', completed_successfully='2/10, 20.0%', appointments_for_contacts='6/10, 60.0%', no_contact_answer_service='n/a', no_contact_busy='n/a', no_contact_disconnect='n/a', no_contact_no_answer='n/a', no_contact_other='n/a', discounted_invalid_cases='0', invalid_fields='n/a')


@pytest.mark.skip('this is awful to test')
def test_generate_invalid_report():
    pass


@pytest.mark.skip("eurgh!")
@pytest.mark.parametrize(
    "invalid_dataframe, denominator, expected",
    [
        (pytest.lazy_fixture("dataframe_with_all_invalid_fields"), 8, "4/8, 50.0%"),
        (pytest.lazy_fixture("call_history_dataframe_with_timed_out_questionnaire"), 1, "1/1, 100.0%"),
    ],
)
def test_get_discounted_records_returns_correct_discounted_invalid_cases(
        interviewer_call_pattern_report,
        invalid_dataframe, denominator, expected):
    actual_report = get_invalid_data(
        interviewer_call_pattern_report,
        invalid_dataframe, denominator)

    assert actual_report.discounted_invalid_cases == expected


@pytest.mark.skip("eurgh!")
@pytest.mark.parametrize(
    "invalid_dataframe, expected",
    [
        (pytest.lazy_fixture("dataframe_with_all_invalid_fields"),
         "'status' column had timed out call status, 'call_end_time' column had missing data, 'number_of_interviews' column had missing data"),
        (pytest.lazy_fixture("call_history_dataframe_with_timed_out_questionnaire"),
         "'status' column had timed out call status"),
    ],
)
def test_get_discounted_records_returns_correct_invalid_fields(
        interviewer_call_pattern_report,
        invalid_dataframe, expected):
    actual_report = get_invalid_data(
        interviewer_call_pattern_report,
        invalid_dataframe, 10)

    assert actual_report.invalid_fields == expected


@pytest.mark.skip("eurgh!")
def test_get_discounted_records_returns_original_report(interviewer_call_pattern_report):
    assert get_invalid_data(
        interviewer_call_pattern_report,
        pd.DataFrame(),
        10
    ) == interviewer_call_pattern_report


def test_status_results_total_one_hundred_percent(dataframe_with_some_invalid_fields):
    denominator = len(dataframe_with_some_invalid_fields.index)
    valid_dataframe, discounted_records, discounted_fields = validate_dataframe(dataframe_with_some_invalid_fields)

    def get_percentage_from(the_string):
        percentage = the_string.split("%")[0].split(",")[1].strip()
        return float(percentage)

    non_response = get_percentage_from(results_for_calls_with_status('status', 'non response', valid_dataframe, denominator))
    no_contact = get_percentage_from(results_for_calls_with_status('status', 'no contact', valid_dataframe, denominator))
    questionnaire_completed = get_percentage_from(results_for_calls_with_status('status', 'questionnaire|completed', valid_dataframe, denominator))
    appointment_made = get_percentage_from(results_for_calls_with_status('status', 'appointment made', valid_dataframe, denominator))

    total = discounted_records.split("/")
    discounted = 100 * int(total[0]) / int(total[1].split(",")[0])

    assert (non_response + no_contact + questionnaire_completed + appointment_made + discounted) == pytest.approx(100)


def test_no_contact_results_total_one_hundred_percent(status_dataframe):
    no_contact_dataframe = status_dataframe[status_dataframe["status"].str.contains('no contact', case=False, na=False)]

    def get_percentage_from(the_string):
        percentage = the_string.split("%")[0].split(",")[1].strip()
        return float(percentage)

    answerservice = get_percentage_from(no_contact_breakdown('answerservice', no_contact_dataframe))
    busy = get_percentage_from(no_contact_breakdown('busy', no_contact_dataframe))
    disconnect = get_percentage_from(no_contact_breakdown('disconnect', no_contact_dataframe))
    noanswer = get_percentage_from(no_contact_breakdown('noanswer', no_contact_dataframe))
    other = get_percentage_from(no_contact_breakdown('other', no_contact_dataframe))

    assert answerservice + busy + disconnect + noanswer + other == pytest.approx(100)