"""Functionality to produce MI Call Pattern Report."""
import datetime
import pandas as pd
import numpy as np

from google.cloud import datastore


columns_to_check_for_nulls = ["call_start_time", "call_end_time"]

# TODO: filter records by interviewer, start_date, end_date and survey

# interviewer_name, start_date_string, end_date_string, survey_tla
def get_call_pattern_report():
    records = get_call_history_records()
    if records.empty:
        return {}

    valid_records = get_valid_records(records)

    hours_worked_in_seconds = calculate_hours_worked_in_seconds(valid_records)
    call_time_in_seconds = calculate_call_time_in_seconds(valid_records)

    return {
        "hours_worked": str(datetime.timedelta(seconds=hours_worked_in_seconds)),
        "call_time": str(datetime.timedelta(seconds=call_time_in_seconds)),
        "hours_on_call_percentage": calculate_hours_on_call_percentage(call_time_in_seconds, hours_worked_in_seconds),
        "average_calls_per_hour": calculate_average_calls_per_hour(hours_worked_in_seconds, len(valid_records)),
        "refusals": percentage_of_records_with_status(records, "Finished (Non response)"),
        "no_contact": percentage_of_records_with_status(records, "Finished (No contact)"),
        "completed_successfully": percentage_of_records_with_status(records, "Completed"),
        "appointments": percentage_of_records_with_status(records, "Finished (Appointment made)"),
        "no_contact_answer_service": percentage_of_no_contact_records_with_call_result(records, "AnswerService"),
        "no_contact_busy": percentage_of_no_contact_records_with_call_result(records, "Busy"),
        "no_contact_disconnect": percentage_of_no_contact_records_with_call_result(records, "Disconnect"),
        "no_contact_no_answer": percentage_of_no_contact_records_with_call_result(records, "NoAnswer"),
        "no_contact_other": percentage_of_no_contact_records_with_call_result(records, "Others"),
        "discounted_invalid_cases": percentage_of_invalid_records(valid_records, records),
        "invalid_fields": ",".join(provide_reasons_for_invalid_records(records)),
    }


def get_call_history_records(interviewer_name, start_date_string, end_date_string, survey_tla) -> pd.DataFrame:
    client = datastore.Client()
    query = client.query(kind="CallHistory")

    return pd.DataFrame(list(query.fetch()))


def format_fraction_and_percentage_as_string(numerator: int, denominator: int) -> str:
    """Format the result as a string displaying both the fraction and percentage values.

    Example: 2/4, 50%
    """
    if denominator == 0:
        return "0/0, 100.00%"
    percentage = format(numerator / denominator * 100, '.2f')

    return f"{numerator}/{denominator}, {percentage}%"


def calculate_hours_worked_in_seconds(records: pd.DataFrame) -> int:
    daily_call_history_by_date = records.groupby(
        [records['call_start_time'].dt.date]).agg({'call_start_time': min, 'call_end_time': max})
    daily_call_history_by_date['hours_worked'] = daily_call_history_by_date['call_end_time'] - daily_call_history_by_date['call_start_time']

    return daily_call_history_by_date['hours_worked'].sum().total_seconds()


def provide_reasons_for_invalid_records(records: pd.DataFrame) -> list:
    reasons = []
    if records[records["status"].str.contains("Timed out", case=False)].any().any():
        reasons.append("'status' column had timed out call status")

    for field in columns_to_check_for_nulls:
        if len(records[records[field].isna()]) > 0:
            reasons.append(f"'{field}' column had missing data")
    return reasons


def calculate_call_time_in_seconds(records: pd.DataFrame) -> int:
    return round(records['dial_secs'].sum())


def calculate_hours_on_call_percentage(call_time_in_seconds: int, hours_worked_in_seconds: int) -> str:
    hours_on_call_percentage = round(call_time_in_seconds / hours_worked_in_seconds * 100, 2)

    return f"{hours_on_call_percentage}%"


def calculate_average_calls_per_hour(hours_worked_in_seconds: int, number_of_valid_records: int) -> float:
    hours_worked = hours_worked_in_seconds / 3600

    return round(number_of_valid_records / float(hours_worked), 2)


def number_of_records_which_has_status(records: pd.DataFrame, status: str) -> int:
    return len(records.loc[records["status"] == status])


def percentage_of_records_with_status(records: pd.DataFrame, status: str) -> str:
    number_of_records_with_status = number_of_records_which_has_status(records, status)

    return str(format_fraction_and_percentage_as_string(number_of_records_with_status, len(records)))


def percentage_of_no_contact_records_with_call_result(records: pd.DataFrame, call_result: str) -> str:
    number_of_records_with_call_result = len(records.loc[
                                                 (records["status"] == "Finished (No contact)") &
                                                 (records["call_result"] == call_result)])

    number_of_records_with_no_contact = number_of_records_which_has_status(records, "Finished (No contact)")

    return str(format_fraction_and_percentage_as_string(number_of_records_with_call_result, number_of_records_with_no_contact))


def percentage_of_invalid_records(valid_records: pd.DataFrame, records: pd.DataFrame):
    total_number_of_records = len(records)
    total_number_of_invalid_records = total_number_of_records - len(valid_records)

    return str(format_fraction_and_percentage_as_string(total_number_of_invalid_records, total_number_of_records))


def get_valid_records(records: pd.DataFrame) -> pd.DataFrame:
    records = records.replace("", np.nan).fillna(value=np.nan)
    valid_records = records.drop(records.loc[records['status'].str.contains('Timed out', case=False)].index)
    valid_records = valid_records.dropna(subset=columns_to_check_for_nulls)

    return valid_records
