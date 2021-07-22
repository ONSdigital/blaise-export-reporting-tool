import pandas as pd

from data_sources.datastore import get_call_history_records_by_interviewer_and_date_range
from interviewer_call_pattern_report.derived_variables import *
from models.interviewer_call_pattern import InterviewerCallPattern

COLUMNS_TO_VALIDATE = ["call_start_time", "call_end_time", "number_of_interviews"]


def get_invalid_fields(data):
    data = data.filter(COLUMNS_TO_VALIDATE)
    return "".join(data.columns[data.isna().any()].tolist())


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
        return (f"generate_report failed with a ZeroDivisionError: {err}", 400), None
    except Exception as err:
        return (f"generate_report failed: {err}", 400), None
    return None, report


def drop_and_return_invalidated_records(dataframe):
    valid_records = dataframe.dropna(subset=COLUMNS_TO_VALIDATE)
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
        valid_data = drop_and_return_invalidated_records(data)[0]
        invalid_data = drop_and_return_invalidated_records(data)[1]
    try:
        valid_data = valid_data.astype({"number_of_interviews": "int32", "dial_secs": "float64"})
    except Exception as err:
        return (f"validate_dataframe failed: {err}", 400), None, None
    return None, valid_data, invalid_data


def create_dataframe(call_history):
    try:
        result = pd.DataFrame(data=call_history)
    except Exception as err:
        return (f"create_dataframe failed: {err}", 400), None
    return None, result


def get_call_pattern_records_by_interviewer_and_date_range(interviewer_name, start_date_string, end_date_string):
    call_history_records_error, call_history_records = get_call_history_records_by_interviewer_and_date_range(
        interviewer_name, start_date_string, end_date_string
    )
    if call_history_records_error:
        return call_history_records_error, None
    if not call_history_records:
        return None, {}
    create_dataframe_error, call_history_dataframe = create_dataframe(call_history_records)
    if create_dataframe_error:
        return create_dataframe_error, None
    validate_dataframe_error, valid_dataframe, invalid_dataframe = validate_dataframe(call_history_dataframe)
    if validate_dataframe_error:
        return validate_dataframe_error, None
    generate_report_error, report = generate_report(valid_dataframe)
    if generate_report_error:
        return generate_report_error, None
    if not invalid_dataframe.empty:
        report.discounted_invalid_records = f"{len(invalid_dataframe.index)}/{len(call_history_dataframe.index)}"
        report.invalid_fields = f"{get_invalid_fields(call_history_dataframe)}"
    return None, report
