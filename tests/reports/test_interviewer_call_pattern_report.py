import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from models.interviewer_call_pattern_model import InterviewerCallPattern
from reports.interviewer_call_pattern_report import (
    get_call_pattern_report, create_dataframe, validate_dataframe,
    invalid_data_found, get_invalid_data, generate_report, get_hours_worked,
    get_call_time_in_seconds, convert_call_time_seconds_to_datetime_format,
    get_percentage_of_hours_on_calls, get_total_seconds_from_string,
    get_invalid_fields, results_for_calls_with_status, get_average_calls_per_hour, no_contact_breakdown,
    get_respondents_interviewed, get_average_respondents_interviewed_per_hour)


def test_get_call_pattern_report_returns_an_empty_dict_if_no_records_were_found(mocker):
    mocker.patch("reports.interviewer_call_pattern_report.get_call_history_records", return_value=[])

    assert get_call_pattern_report("el4president", "2022-24-01", "2022-24-01", "LMS") == {}


def test_get_call_pattern_report_returns_a_report_with_only_the_discounted_information_when_data_is_completely_invalid(
        call_history_with_all_invalid_records, dataframe_with_all_invalid_fields, mocker):
    mock_interviewer_name = 'el4president'
    mock_date = '2022-01-24'
    mock_discounted_invalid_cases = "4/4, 100.0%"
    mock_invalid_fields = "'status' column had timed out call status, 'call_end_time' column had missing data, 'number_of_interviews' column had missing data"

    mocker.patch("reports.interviewer_call_pattern_report.get_call_history_records",
                 return_value=call_history_with_all_invalid_records)
    mocker.patch("reports.interviewer_call_pattern_report.validate_dataframe",
                 return_value=(pd.DataFrame(), mock_discounted_invalid_cases, mock_invalid_fields))

    actual_report = get_call_pattern_report(
        mock_interviewer_name,
        mock_date,
        mock_date,
        "LMS")

    numerator = len(dataframe_with_all_invalid_fields.index)
    denominator = len(call_history_with_all_invalid_records)
    percentage = 100 * numerator / denominator

    assert actual_report.discounted_invalid_cases == f"{numerator}/{denominator}, {percentage}%"
    assert actual_report.invalid_fields == get_invalid_fields(dataframe_with_all_invalid_fields)


def test_get_call_pattern_report(call_history_records, mocker):
    mocker.patch("reports.interviewer_call_pattern_report.get_call_history_records", return_value=call_history_records)

    assert get_call_pattern_report(
        "el4president",
        "2022-24-01",
        "2022-24-01",
        "LOL") == InterviewerCallPattern(
        hours_worked='0:12:05',
        call_time='0:10:17',
        hours_on_calls_percentage='85.1%',
        average_calls_per_hour=74.48,
        respondents_interviewed=16,
        average_respondents_interviewed_per_hour=79.45,
        refusals='1/16, 6.25%',
        no_contacts='14/16, 87.5%',
        completed_successfully='0/16, 0.0%',
        appointments_for_contacts='0/16, 0.0%',
        no_contact_answer_service='1/14, 7.14%',
        no_contact_busy='5/14, 35.71%',
        no_contact_disconnect='3/14, 21.43%',
        no_contact_no_answer='5/14, 35.71%',
        no_contact_other='0/14, 0.0%',
        discounted_invalid_cases='1/16, 6.25%',
        invalid_fields="'call_end_time' column had missing data")


def test_create_dataframe_returns_a_dataframe(call_history_records):
    df = create_dataframe(call_history_records)
    assert type(df) == pd.DataFrame


def test_validate_dataframe_drops_invalid_data_and_returns_information(missing_dataframe):
    valid_dataframe, discounted_records, discounted_fields = validate_dataframe(missing_dataframe)
    assert len(valid_dataframe.index) == 2
    assert discounted_records == "1/3, 33.33%"
    assert discounted_fields == "'call_end_time' column had missing data"


def test_validate_dataframe_returns_valid_dataframe_with_lower_case_column_names(valid_dataframe):
    valid_dataframe, x, y = validate_dataframe(valid_dataframe)
    assert type(valid_dataframe) == pd.DataFrame
    assert all(i.islower() for i in list(valid_dataframe.columns))


def test_validate_dataframe_returns_valid_dataframe_with_no_discounted_records_or_fields(valid_dataframe):
    new_valid_dataframe, discounted_records, discounted_fields = validate_dataframe(valid_dataframe)
    assert_frame_equal(left=new_valid_dataframe, right=valid_dataframe, check_dtype=False)
    assert discounted_records == ""
    assert discounted_fields == ""


def test_invalid_data_found_returns_false(valid_dataframe):
    assert invalid_data_found(valid_dataframe) is False


def test_invalid_data_found_returns_true(missing_dataframe):
    assert invalid_data_found(missing_dataframe) is True


def test_generate_report(valid_dataframe):
    assert generate_report(valid_dataframe, 10) == InterviewerCallPattern(hours_worked='0:48:05', call_time='0:12:50',
                                                                          hours_on_calls_percentage='26.69%',
                                                                          average_calls_per_hour=12.48,
                                                                          respondents_interviewed=10,
                                                                          average_respondents_interviewed_per_hour=12.48,
                                                                          refusals='2/10, 20.0%',
                                                                          no_contacts='0/10, 0.0%',
                                                                          completed_successfully='2/10, 20.0%',
                                                                          appointments_for_contacts='6/10, 60.0%',
                                                                          no_contact_answer_service='n/a',
                                                                          no_contact_busy='n/a',
                                                                          no_contact_disconnect='n/a',
                                                                          no_contact_no_answer='n/a',
                                                                          no_contact_other='n/a',
                                                                          discounted_invalid_cases='0',
                                                                          invalid_fields='n/a')


@pytest.mark.parametrize(
    "invalid_dataframe, expected_totals, expected_fields",
    [
        (pytest.lazy_fixture("dataframe_with_all_invalid_fields"),
         "4/4, 100.0%",
         "'status' column had timed out call status, 'call_end_time' column had missing data, 'number_of_interviews' column had missing data"),
        (pytest.lazy_fixture("dataframe_with_some_invalid_fields"),
         "3/10, 30.0%",
         "'call_start_time' column had missing data, 'call_end_time' column had missing data, 'number_of_interviews' column had missing data"),
        (pytest.lazy_fixture("missing_dataframe"),
         "1/3, 33.33%",
         "'call_end_time' column had missing data"),
        (pytest.lazy_fixture("call_history_dataframe_with_timed_out_questionnaire"),
         "1/1, 100.0%",
         "'status' column had timed out call status"),
    ],
)
def test_get_invalid_data_returns_correct_discounted_information(
        interviewer_call_pattern_report,
        invalid_dataframe,
        expected_totals, expected_fields):
    df, actual_totals, actual_fields = get_invalid_data(
        invalid_dataframe)

    assert actual_totals == expected_totals
    assert actual_fields == expected_fields


def test_get_hours_worked(valid_dataframe):
    assert get_hours_worked(valid_dataframe) == "0:48:05"


def test_get_call_time_in_seconds(valid_dataframe):
    assert get_call_time_in_seconds(valid_dataframe) == 770


def test_call_time_is_less_than_hours_worked(valid_dataframe):
    assert get_call_time_in_seconds(valid_dataframe) < get_total_seconds_from_string(
        get_hours_worked(valid_dataframe))


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
        ("00:00:01", 1),
        ("00:00:10", 10),
        ("00:01:00", 60),
        ("01:00:00", 3600),
        ("10:00:00", 36000),
    ],
)
def test_get_total_seconds_from_string(hours_worked, expected):
    assert get_total_seconds_from_string(hours_worked) == expected


@pytest.mark.parametrize(
    "hours_worked, expected",
    [
        ("10:00:00", 1.0),
        ("05:00:00", 2.0),
        ("02:30:00", 4),
    ],
)
def test_get_average_calls_per_hour(hours_worked, expected, valid_dataframe):
    assert get_average_calls_per_hour(valid_dataframe, hours_worked) == expected


def test_get_respondents_interviewed(valid_dataframe):
    assert get_respondents_interviewed(valid_dataframe) == 10


@pytest.mark.parametrize(
    "hours_worked, expected",
    [
        ("10:00:00", 1),
        ("5:00:00", 2.0),
        ("2:00:00", 5.0),
    ],
)
def test_get_average_respondents_interviewed_per_hour(hours_worked, expected, valid_dataframe):
    assert get_average_respondents_interviewed_per_hour(valid_dataframe, hours_worked) == expected


def test_status_results_total_one_hundred_percent(dataframe_with_some_invalid_fields):
    denominator = len(dataframe_with_some_invalid_fields.index)
    valid_dataframe, discounted_records, discounted_fields = validate_dataframe(dataframe_with_some_invalid_fields)

    def get_percentage_from(the_string):
        percentage = the_string.split("%")[0].split(",")[1].strip()
        return float(percentage)

    non_response = get_percentage_from(
        results_for_calls_with_status('status', 'non response', valid_dataframe, denominator))
    no_contact = get_percentage_from(
        results_for_calls_with_status('status', 'no contact', valid_dataframe, denominator))
    questionnaire_completed = get_percentage_from(
        results_for_calls_with_status('status', 'questionnaire|completed', valid_dataframe, denominator))
    appointment_made = get_percentage_from(
        results_for_calls_with_status('status', 'appointment made', valid_dataframe, denominator))

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
