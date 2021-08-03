import pandas as pd
import numpy as np

from data_sources.datastore import get_call_history_records_by_interviewer_and_date_range
from interviewer_call_pattern_report.derived_variables import *
from models.interviewer_call_pattern import InterviewerCallPattern
from models.error_capture import BertException

COLUMNS_TO_VALIDATE = ["call_start_time", "call_end_time", "number_of_interviews"]


def get_invalid_fields(data):
    data = data.filter(COLUMNS_TO_VALIDATE)
    return ", ".join(data.columns[data.isna().any()].tolist())


def generate_report(valid_call_history_dataframe):
    try:
        hours_worked = get_hours_worked(valid_call_history_dataframe)
        total_call_seconds = get_call_time_in_seconds(valid_call_history_dataframe)
        report = InterviewerCallPattern(
            hours_worked=hours_worked,
            call_time=convert_call_time_seconds_to_datetime_format(total_call_seconds),
            hours_on_calls_percentage=get_percentage_of_hours_on_calls(hours_worked, total_call_seconds),
            average_calls_per_hour=get_average_calls_per_hour(valid_call_history_dataframe, hours_worked),
            respondents_interviewed=get_respondents_interviewed(valid_call_history_dataframe),
            households_completed_successfully=get_number_of_households_completed_successfully(
                "numberwang", valid_call_history_dataframe),
            average_respondents_interviewed_per_hour=get_average_respondents_interviewed_per_hour(
                valid_call_history_dataframe, hours_worked),
            no_contacts_percentage=get_percentage_of_call_for_status(
                "no contact", valid_call_history_dataframe),
            appointments_for_contacts_percentage=get_percentage_of_call_for_status(
                "appointment made", valid_call_history_dataframe),
        )
    except ZeroDivisionError as err:
        raise BertException(f"generate_report failed with a ZeroDivisionError: {err}", 400)
    except Exception as err:
        raise BertException(f"generate_report failed: {err}", 400)
    return report


def drop_and_return_invalidated_records(dataframe):
    valid_records = dataframe.copy()
    valid_records['number_of_interviews'].replace('', np.nan, inplace=True)

    valid_records.dropna(subset=COLUMNS_TO_VALIDATE, inplace=True)
    invalid_records = dataframe[~dataframe.index.isin(valid_records.index)]

    valid_records.reset_index(drop=True, inplace=True)
    invalid_records.reset_index(drop=True, inplace=True)

    return valid_records, invalid_records


def validate_dataframe(data):
    invalid_data = pd.DataFrame()
    valid_data = data.copy()
    valid_data.columns = valid_data.columns.str.lower()

    missing_data_found = valid_data.filter(COLUMNS_TO_VALIDATE).isna().any().any()
    if missing_data_found:
        valid_data, invalid_data = drop_and_return_invalidated_records(data)
    try:
        valid_data = valid_data.astype({"number_of_interviews": "int32", "dial_secs": "float64"})
    except Exception as err:
        raise BertException(f"validate_dataframe failed: {err}", 400)
    return valid_data, invalid_data


def create_dataframe(call_history):
    try:
        result = pd.DataFrame(data=call_history)
    except Exception as err:
        raise BertException(f"create_dataframe failed: {err}", 400)
    return result


def add_invalid_fields_to_report(report, invalid_dataframe, call_history_dataframe):
    report.discounted_invalid_records = f"{len(invalid_dataframe.index)}/{len(call_history_dataframe.index)}"
    report.invalid_fields = f"{get_invalid_fields(invalid_dataframe)}"


def get_call_pattern_records_by_interviewer_and_date_range(interviewer_name, start_date_string, end_date_string):
    print(f"Getting call pattern data for {interviewer_name} between {start_date_string} and {end_date_string}")

    call_history_records = get_call_history_records_by_interviewer_and_date_range(
        interviewer_name, start_date_string, end_date_string
    )

    if not call_history_records:
        return {}

    call_history_dataframe = create_dataframe(call_history_records)
    valid_dataframe, invalid_dataframe = validate_dataframe(call_history_dataframe)
    report = generate_report(valid_dataframe)

    if not invalid_dataframe.empty:
        add_invalid_fields_to_report(report, invalid_dataframe, call_history_dataframe)

    return report
