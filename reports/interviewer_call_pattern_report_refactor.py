from google.cloud import datastore
import datetime
import pandas as pd
import numpy as np


def get_call_pattern_report():
    records = get_call_history_records()
    if records.empty:
        return {}

    records = replace_empty_strings_with_nans(records)

    hours_worked_in_seconds = calculate_hours_worked_in_seconds(records)
    number_of_invalid_records = calculate_number_of_invalid_records(records)
    reason_for_invalid_fields = provide_reason_for_invalid_records(records)
    call_time = calculate_call_time(records)
    hours_on_call_percentage = calculate_hours_on_call_percentage(records)
    average_calls_per_hour = calculate_average_calls_per_hour(records)
    refusals = calculate_refusals(records)
    no_contact = calculate_no_contact(records)
    completed_successfully = calculate_completed_successfully(records)
    appointments = calculate_appointments_made(records)

    return {
        "hours_worked": str(datetime.timedelta(seconds=hours_worked_in_seconds)),
        "discounted_invalid_cases": format_fraction_and_percentage_as_string(number_of_invalid_records, len(records)),
        "invalid_fields": reason_for_invalid_fields,
        "call_time": str(datetime.timedelta(seconds=call_time)),
        "hours_on_call_percentage": f"{hours_on_call_percentage}%",
        "average_calls_per_hour": average_calls_per_hour,
        "refusals": format_fraction_and_percentage_as_string(refusals, len(records)),
        "no_contact": format_fraction_and_percentage_as_string(no_contact, len(records)),
        "completed_successfully": format_fraction_and_percentage_as_string(completed_successfully, len(records)),
        "appointments": format_fraction_and_percentage_as_string(appointments, len(records)),
    }


def get_call_history_records():
    client = datastore.Client()
    query = client.query(kind="CallHistory")
    return pd.DataFrame(list(query.fetch()))


def replace_empty_strings_with_nans(records: pd.DataFrame) -> pd.DataFrame:
    """Fill all cells containing empty strings or None values with NAN values.

    NAN is easier to filter when selecting the valid and invalid records.
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

    # Group records by date.
    daily_call_history_by_date = records.groupby(
        [records['call_start_time'].dt.date]).agg({'call_start_time': min, 'call_end_time': max})

    # Subtract the first call time from the last call time.
    daily_call_history_by_date['hours_worked'] = daily_call_history_by_date['call_end_time'] - \
                                                 daily_call_history_by_date['call_start_time']

    # Sum total hours.
    return daily_call_history_by_date['hours_worked'].sum().total_seconds()


def calculate_number_of_invalid_records(records: pd.DataFrame) -> str:
    return len(records[records["call_end_time"].isna()])


def provide_reason_for_invalid_records(records: pd.DataFrame) -> str:
    """."""
    invalid_records = records[records["call_end_time"].isna()]
    status = invalid_records['status'].unique()

    if 'Timed out' in status:
        return "'status' column had timed out call status"
    return ""

def calculate_call_time(records: pd.DataFrame) -> str:
    return round(records['dial_secs'].sum())


def calculate_hours_on_call_percentage(records: pd.DataFrame) -> float:
    call_time_in_seconds = round(records['dial_secs'].sum())

    hours_worked_in_seconds = calculate_hours_worked_in_seconds(records)

    return round(call_time_in_seconds / hours_worked_in_seconds * 100, 2)


def calculate_average_calls_per_hour(records) -> float:
    hours_worked = calculate_hours_worked_in_seconds(records) / 3600
    number_of_valid_records = records["call_end_time"].count()

    return number_of_valid_records / float(hours_worked)


def calculate_refusals(records) -> int:
    records = records.dropna(subset=["call_end_time"])
    return len(records.loc[records["status"] == "Finished (Non response)"])

def calculate_no_contact(records) -> int:
    records = records.dropna(subset=["call_end_time"])
    return len(records.loc[records["status"] == "Finished (No contact)"])

def calculate_completed_successfully(records) -> int:
    records = records.dropna(subset=["call_end_time"])
    return len(records.loc[records["status"] == "Completed"])


def calculate_appointments_made(records) -> int:
    records = records.dropna(subset=["call_end_time"])
    return len(records.loc[records["status"] == "Finished (Appointment made)"])
