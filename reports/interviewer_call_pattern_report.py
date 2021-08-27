import logging

import pandas as pd

from functions.interviewer_call_pattern_data_functions import (
    get_hours_worked, get_call_time_in_seconds,
    hours_on_calls, average_calls_per_hour, convert_seconds_to_datetime_format,
    respondents_interviewed, average_respondents_interviewed_per_hour, get_no_contacts_breakdown, get_call_statuses)
from functions.validate_call_pattern_report import (
    validate_dataframe, get_invalid_fields)
from models.error_capture import BertException
from models.interviewer_call_pattern_model import InterviewerCallPattern, InterviewerCallPatternWithNoValidData
from reports.interviewer_call_history_report import get_call_history_records

COLUMNS_TO_VALIDATE = ["call_start_time", "call_end_time", "number_of_interviews"]

logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)


def get_call_pattern_report(
        interviewer_name, start_date_string, end_date_string, survey_tla):

    call_history_records = get_call_history_records(
        interviewer_name, start_date_string, end_date_string, survey_tla
    )
    if not call_history_records:
        return {}

    call_history_dataframe = create_and_parse_call_history_dataframe(call_history_records)
    original_number_of_records = len(call_history_dataframe.index)

    valid_dataframe, invalid_dataframe = validate_dataframe(call_history_dataframe)

    if valid_dataframe.empty:
        report = generate_invalid_report(call_history_dataframe, invalid_dataframe)
        return report

    report = generate_report(valid_dataframe, original_number_of_records)
    report = get_no_contacts_breakdown(report, valid_dataframe)
    report = get_discounted_records(report, invalid_dataframe, original_number_of_records)

    return report


def create_and_parse_call_history_dataframe(call_history):
    try:
        df = pd.DataFrame(data=call_history)
        df.columns.str.lower()
        df = df.astype({"number_of_interviews": "int32", "dial_secs": "float64"}, errors='ignore')
        pd.to_datetime(df['call_end_time'], format='%YYYY-%mm-%dd hh:mm:ss', errors='ignore')
    except Exception as err:
        raise BertException(f"create_and_parse_dataframe failed: {err}", 400)
    return df


def generate_report(valid_dataframe, original_number_of_records):
    hours_worked = get_hours_worked(valid_dataframe)
    total_call_seconds = get_call_time_in_seconds(valid_dataframe)

    status_results = get_call_statuses(valid_dataframe, original_number_of_records)

    return InterviewerCallPattern(
        hours_worked=hours_worked,
        call_time=convert_seconds_to_datetime_format(total_call_seconds),
        hours_on_calls=hours_on_calls(hours_worked, total_call_seconds),
        average_calls_per_hour=average_calls_per_hour(valid_dataframe, hours_worked),
        respondents_interviewed=respondents_interviewed(valid_dataframe),
        average_respondents_interviewed_per_hour=average_respondents_interviewed_per_hour(valid_dataframe, hours_worked),
        refusals=status_results["non response"],
        no_contacts=status_results["no contact"],
        completed_successfully=status_results["questionnaire|completed"],
        appointments_for_contacts=status_results["appointment"]
    )


def generate_invalid_report(invalid_dataframe, call_history_dataframe):
    discounted_records = len(invalid_dataframe.index)
    records = len(call_history_dataframe.index)
    percentage = 100 * discounted_records / records
    discounted_fields = get_invalid_fields(call_history_dataframe)

    return InterviewerCallPatternWithNoValidData(
        discounted_invalid_cases=f"{discounted_records}/{records}, {percentage}%",
        invalid_fields=discounted_fields
    )


def get_discounted_records(report, invalid_dataframe, denominator):
    terminator = len(invalid_dataframe.index)
    percentage = 100 * terminator / denominator

    if invalid_dataframe.empty:
        return report

    report.discounted_invalid_cases = f"{terminator}/{denominator}, {percentage}%"
    report.invalid_fields = get_invalid_fields(invalid_dataframe)
    return report
