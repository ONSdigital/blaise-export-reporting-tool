"""Functionality to produce MI Call Pattern Report."""
import datetime
import pandas as pd
import numpy as np

from google.cloud import datastore


def get_call_pattern_report():
    records = get_call_history_records()
    if records.empty:
        return {}

    records = replace_empty_strings_with_nans(records)

    hours_worked_in_seconds = calculate_hours_worked_in_seconds(records)
    call_time = calculate_call_time(records)
    hours_on_call_percentage = calculate_hours_on_call_percentage(records)
    average_calls_per_hour = calculate_average_calls_per_hour(records)
    refusals = count_records_with_status(records, "Finished (Non response)")
    no_contact = count_records_with_status(records, "Finished (No contact)")
    completed_successfully = count_records_with_status(records, "Completed")
    appointments = count_records_with_status(records, "Finished (Appointment made)")
    no_contact_answer_service = count_records_with_finished_status_and_call_result(records, "AnswerService")
    no_contact_busy = count_records_with_finished_status_and_call_result(records, "Busy")
    no_contact_disconnect = count_records_with_finished_status_and_call_result(records, "Disconnect")
    no_contact_no_answer = count_records_with_finished_status_and_call_result(records, "NoAnswer")
    no_contact_other = count_records_with_finished_status_and_call_result(records, "Others")
    number_of_invalid_records = calculate_number_of_invalid_records(records)
    reasons_for_invalid_fields = provide_reasons_for_invalid_records(records)

    return {
        "hours_worked": str(datetime.timedelta(seconds=hours_worked_in_seconds)),
        "call_time": str(datetime.timedelta(seconds=call_time)),
        "hours_on_call_percentage": f"{hours_on_call_percentage}%",
        "average_calls_per_hour": round(average_calls_per_hour, 2),
        "refusals": format_fraction_and_percentage_as_string(refusals, len(records)),
        "no_contact": format_fraction_and_percentage_as_string(no_contact, len(records)),
        "completed_successfully": format_fraction_and_percentage_as_string(completed_successfully, len(records)),
        "appointments": format_fraction_and_percentage_as_string(appointments, len(records)),
        "no_contact_answer_service": format_fraction_and_percentage_as_string(no_contact_answer_service, len(records)),
        "no_contact_busy": format_fraction_and_percentage_as_string(no_contact_busy, len(records)),
        "no_contact_disconnect": format_fraction_and_percentage_as_string(no_contact_disconnect, len(records)),
        "no_contact_no_answer": format_fraction_and_percentage_as_string(no_contact_no_answer, len(records)),
        "no_contact_other": format_fraction_and_percentage_as_string(no_contact_other, len(records)),
        "discounted_invalid_cases": format_fraction_and_percentage_as_string(number_of_invalid_records, len(records)),
        "invalid_fields": ",".join(reasons_for_invalid_fields),
    }


def get_call_history_records():
    client = datastore.Client()
    query = client.query(kind="CallHistory")
    return pd.DataFrame(list(query.fetch()))


def replace_empty_strings_with_nans(records: pd.DataFrame) -> pd.DataFrame:
    """Fill all cells containing empty strings or None values with NAN values.

    NAN is easier to filter than empty strings when selecting the valid and invalid records.
    """
    return records.replace("", np.nan).fillna(value=np.nan)


def format_fraction_and_percentage_as_string(numerator: int, denominator: int) -> str:
    """Format the result as a string displaying both the fraction and percentage values.

    Example: 2/4, 50%
    """
    percentage = format(numerator / denominator * 100, '.2f')
    return f"{numerator}/{denominator}, {percentage}%"


def calculate_hours_worked_in_seconds(records: pd.DataFrame) -> int:
    records = records.dropna(subset=["call_end_time"])
    records = records.loc[records["status"] != "Timed out during questionnaire"]

    # Group records by date.
    daily_call_history_by_date = records.groupby(
        [records['call_start_time'].dt.date]).agg({'call_start_time': min, 'call_end_time': max})

    # Subtract the first call time from the last call time.
    daily_call_history_by_date['hours_worked'] = daily_call_history_by_date['call_end_time'] - \
                                                 daily_call_history_by_date['call_start_time']

    # Sum total hours.
    return daily_call_history_by_date['hours_worked'].sum().total_seconds()


def calculate_number_of_invalid_records(records: pd.DataFrame) -> int:
    return len(records.loc[(records["call_start_time"].isna()) |
                           (records["call_end_time"].isna()) |
                           (records["status"] == "Timed out during questionnaire")])


def provide_reasons_for_invalid_records(records: pd.DataFrame) -> list:
    """."""
    reasons = []
    if len(records[records["call_start_time"].isna()]) > 0:
        reasons.append("'Start call time' column had missing data")
    if len(records[records["call_end_time"].isna()]) > 0:
        reasons.append(provide_reason_for_no_call_end_time(records))
    if len(records[records["status"] == "Timed out during questionnaire"]) > 0:
        reasons.append("'status' column returned a timed out session")
    return reasons


def provide_reason_for_no_call_end_time(records) -> str:
    unique_statuses = records['status'].unique()

    if 'Timed out' in unique_statuses:
        return "'status' column had timed out call status"
    return "'End call time' column had missing data"


def calculate_call_time(records: pd.DataFrame) -> int:
    records = records.loc[records["status"] != "Timed out during questionnaire"]
    return round(records['dial_secs'].sum())


def calculate_hours_on_call_percentage(records: pd.DataFrame) -> float:
    call_time_in_seconds = round(records['dial_secs'].sum())

    hours_worked_in_seconds = calculate_hours_worked_in_seconds(records)

    return round(call_time_in_seconds / hours_worked_in_seconds * 100, 2)


def calculate_average_calls_per_hour(records) -> float:
    hours_worked = calculate_hours_worked_in_seconds(records) / 3600
    number_of_valid_records = records["call_end_time"].count()

    return number_of_valid_records / float(hours_worked)


def count_records_with_status(records, status) -> int:
    return len(records.loc[records["status"] == status])


def count_records_with_finished_status_and_call_result(records, call_result) -> int:
    return len(records.loc[
                   (records["status"] == "Finished (No contact)") &
                   (records["call_result"] == call_result)])
