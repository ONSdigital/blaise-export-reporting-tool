"""Produce an interviewer call pattern report given an interviewer, period of time, and optionally filter by survey.
"""

import datetime

import numpy as np
import pandas as pd
from google.cloud import datastore

from functions.date_functions import parse_date_string_to_datetime
from models.interviewer_call_pattern_model import InterviewerCallPatternRefactored, InterviewerCallPatternWithNoValidData
from models.error_capture import BertException

columns_to_check_for_nulls = ["call_start_time", "call_end_time"]


def get_call_pattern_report_refactor(
        interviewer_name: str,
        start_date_string: str,
        end_date_string: str,
        survey_tla: str,
) -> object:
    """Return interviewer call pattern report for a given interviewer, period of time, and optionally filter by survey.

    Args:
        interviewer_name: Name of interviewer to report on.
        start_date_string: Report start date in YYYY-MM-DD format.
        end_date_string: Report end date in YYYY-MM-DD format.
        survey_tla: Survey to report on (e.g. OPN, LMS).

    Returns:
        dict: MI interviewer call pattern report.
    """
    records = get_call_history_records(interviewer_name, start_date_string, end_date_string, survey_tla)

    print(f"Calculating call pattern data for interviewer '{interviewer_name}'")

    if records.empty:
        return {}

    if no_valid_records(records):
        return InterviewerCallPatternWithNoValidData(
            discounted_invalid_cases=percentage_of_invalid_records(records),
            invalid_fields=",".join(provide_reasons_for_invalid_records(records))
        )

    return InterviewerCallPatternRefactored(
        hours_worked=str(calculate_hours_worked_as_datetime(records)),
        call_time=str(calculate_call_time_as_datetime(records)),
        hours_on_calls_percentage=calculate_hours_on_call_percentage(records),
        average_calls_per_hour=calculate_average_calls_per_hour(records),
        refusals=percentage_of_records_with_status(records, "Finished (Non response)"),
        no_contacts=percentage_of_records_with_status(records, "Finished (No contact)"),
        completed_successfully=percentage_of_records_with_status(records, "Completed"),
        appointments_for_contacts=percentage_of_records_with_status(records, "Finished (Appointment made)"),
        no_contact_answer_service=percentage_of_no_contact_records_with_call_result(records, "AnswerService"),
        no_contact_busy=percentage_of_no_contact_records_with_call_result(records, "Busy"),
        no_contact_disconnect=percentage_of_no_contact_records_with_call_result(records, "Disconnect"),
        no_contact_no_answer=percentage_of_no_contact_records_with_call_result(records, "NoAnswer"),
        no_contact_other=percentage_of_no_contact_records_with_call_result(records, "Others"),
        discounted_invalid_cases=percentage_of_invalid_records(records),
        invalid_fields=",".join(provide_reasons_for_invalid_records(records))
    )


def get_call_history_records(
        interviewer_name: str,
        start_date_string: str,
        end_date_string: str,
        survey_tla: str,
) -> pd.DataFrame:
    """Query datastore and return a pandas dataframe.
    Args:
        interviewer_name: Name of interviewer to report on.
        start_date_string: Report start date in YYYY-MM-DD format.
        end_date_string: Report end date in YYYY-MM-DD format.
        survey_tla: Survey to report on (i.e. OPN, LMS).
    Returns:
        A pd.DataFrame of an individual's call history. Each row represents a call made to a respondent.
    Raises:
        BertException: get_call_history_records failed
    """
    try:
        start_date = parse_date_string_to_datetime(start_date_string)
        end_date = parse_date_string_to_datetime(end_date_string, True)

        client = datastore.Client()
        query = client.query(kind="CallHistory")
        query.add_filter("interviewer", "=", interviewer_name)
        query.add_filter("call_start_time", ">=", start_date)
        query.add_filter("call_start_time", "<=", end_date)

        if survey_tla is not None:
            query.add_filter("survey", "=", survey_tla)

        query.order = ["call_start_time"]
        return pd.DataFrame(list(query.fetch()))

    except Exception as err:
        raise BertException(f"get_call_history_records failed: {err}", 400)


def calculate_hours_worked_as_datetime(records: pd.DataFrame) -> str:
    """Return hours worked in datetime format.

    Args:
        records: An individual's call history. Each row represents a call made to a respondent.

    Returns:
        The time worked in a datetime (HH:MM:SS) format.
    """
    valid_records = get_valid_records(records)
    hours_worked_in_seconds = calculate_hours_worked_in_seconds(valid_records)

    return convert_timedelta_to_hhmmss_as_string(datetime.timedelta(seconds=hours_worked_in_seconds))


def calculate_call_time_as_datetime(records: pd.DataFrame) -> datetime:
    """Return call time in datetime format.

    Args:
        records: An individual's call history. Each row represents a call made to a respondent.

    Returns:
        The time spent on call in datetime (HH:MM:SS) format.
    """
    valid_records = get_valid_records(records)
    call_time_in_seconds = calculate_call_time_in_seconds(valid_records)

    return convert_timedelta_to_hhmmss_as_string(datetime.timedelta(seconds=call_time_in_seconds))


def calculate_hours_on_call_percentage(records: pd.DataFrame, ) -> str:
    """Calculate and return the percentage of time spent on call during a working shift.

    Args:
        records: An individual's call history. Each row represents a call made to a respondent.

    Returns:
        The percentage of time spent on call.
    """
    valid_records = get_valid_records(records)

    hours_worked_in_seconds = calculate_hours_worked_in_seconds(valid_records)
    call_time_in_seconds = calculate_call_time_in_seconds(valid_records)
    hours_on_call_percentage = round(call_time_in_seconds / hours_worked_in_seconds * 100, 2)

    return f"{hours_on_call_percentage}%"


def calculate_average_calls_per_hour(records: pd.DataFrame) -> float:
    """Calculate and return the average number of calls made per hour worked.

    Args:
        records: An individual's call history. Each row represents a call made to a respondent.

    Returns:
        The average number of calls made per hour.
    """
    valid_records = get_valid_records(records)

    hours_worked_in_seconds = calculate_hours_worked_in_seconds(valid_records)
    number_of_valid_records = len(valid_records)
    hours_worked = hours_worked_in_seconds / 3600

    return round(number_of_valid_records / float(hours_worked), 2)


def percentage_of_records_with_status(records: pd.DataFrame, status: str) -> str:
    """Calculate the number of records with a user-defined status and return the result in a fraction, percentage
    format.

    Args:
        records: An individual's call history. Each row represents a call made to a respondent.
        status: The user-defined status to be searched for (i.e. 'Completed').

    Returns:
        The number of records with the user-defined status in a fraction, percentage string format (i.e. 2/4, 50%).
    """
    number_of_records_with_status = number_of_records_which_has_status(records, status)

    return str(format_fraction_and_percentage_as_string(number_of_records_with_status, len(records)))


def percentage_of_no_contact_records_with_call_result(records: pd.DataFrame, call_result: str) -> str:
    """Calculate the number of records with a status of 'no_contact' and a user-defined 'call_result' and return the
    result in a fraction, percentage string format.

    Args:
        records: An individual's call history. Each row represents a call made to a respondent.
        call_result: The user-defined call_result to be searched for (i.e. 'NoAnswer').

    Returns:
        The number of records with the user-defined call_result in a fraction, percentage string format (i.e. 2/4, 50%).
    """
    number_of_records_with_call_result = len(records.loc[
                                                 (records["status"] == "Finished (No contact)") &
                                                 (records["call_result"] == call_result)])

    number_of_records_with_no_contact = number_of_records_which_has_status(records, "Finished (No contact)")

    return str(
        format_fraction_and_percentage_as_string(number_of_records_with_call_result, number_of_records_with_no_contact)
    )


def percentage_of_invalid_records(records: pd.DataFrame) -> str:
    """Calculate the number of invalid records and return the value in a fraction, percentage format.

    Invalid records are records with empty data or Nan values in fields defined within the global variable
    'columns_to_check_for_nulls'.

    Args:
        records: An individual's call history. Each row represents a call made to a respondent.

    Returns:
        The number of invalid records found in a fraction, percentage format (i.e. 2/4, 50%).
    """
    valid_records = get_valid_records(records)

    total_number_of_records = len(records)
    total_number_of_invalid_records = total_number_of_records - len(valid_records)

    return str(format_fraction_and_percentage_as_string(total_number_of_invalid_records, total_number_of_records))


def provide_reasons_for_invalid_records(records: pd.DataFrame) -> list:
    """Return a list of unique reasons for invalid records.

    Args:
        records: An individual's call history. Each row represents a call made to a respondent.

    Returns:
        The unique reasons for records being invalid.
    """
    reasons = []
    if records.status.str.contains("Timed out", case=False).any():
        reasons.append("'status' column had timed out call status")

    for field in columns_to_check_for_nulls:
        if records[field].isna().any():
            reasons.append(f"'{field}' column had missing data")

    return reasons


def get_valid_records(records: pd.DataFrame) -> pd.DataFrame:
    """Calculate the number of invalid records and return the value in a fraction, percentage format.

    Valid records are records with no empty data or Nan values in fields defined within the global variable
    'columns_to_check_for_nulls'.

    Args:
        records: An individual's call history. Each row represents a call made to a respondent.

    Returns:
        All valid records in an individual's call history.

    Raises:
        BertException: get_valid_records failed
    """
    try:
        records = records.replace("", np.nan).fillna(value=np.nan)
        valid_records = records.dropna(subset=columns_to_check_for_nulls)
        if valid_records.empty:
            return pd.DataFrame()
        valid_records = valid_records.drop(valid_records.loc[valid_records['status'].str.contains('Timed out', case=False)].index)

        return valid_records

    except Exception as err:
        raise BertException(f"get_valid_records failed: {err}", 400)


def calculate_hours_worked_in_seconds(records: pd.DataFrame) -> int:
    """Return hours worked in seconds.

    Hours worked is calculated as the time difference between the first and last call made during a single day.

    Args:
        records: An individual's call history. Each row represents a call made to a respondent.

    Returns:
        The number of hours worked in seconds.

    Raises:
        BertException: calculate_hours_worked_in_seconds failed:
    """
    try:
        daily_call_history_by_date = records.groupby(
            [records['call_start_time'].dt.date]).agg({'call_start_time': min, 'call_end_time': max})
        daily_call_history_by_date['hours_worked'] = (
                daily_call_history_by_date['call_end_time'] - daily_call_history_by_date['call_start_time']
        )

        return daily_call_history_by_date['hours_worked'].sum().total_seconds()
    except Exception as err:
        raise BertException(f"calculate_hours_worked_in_seconds failed: {err}", 400)


def calculate_call_time_in_seconds(records: pd.DataFrame) -> int:
    """Return call time in seconds.

    Args:
        records: An individual's call history. Each row represents a call made to a respondent.

    Returns:
        The time on call in seconds.

    Raises:
        BertException: calculate_call_time_in_seconds failed
    """
    try:
        return round(records['dial_secs'].sum())
    except Exception as err:
        raise BertException(f"calculate_call_time_in_seconds failed: {err}", 400)


def format_fraction_and_percentage_as_string(numerator: int, denominator: int) -> str:
    """Return a string in a fraction, percentage format.

    Args:
        numerator: The number of records of interest (i.e. valid records).
        denominator: The total number of records made by an interviewer.

    Returns:
        The result in a fraction, percentage format (i.e. 2/4, 50%)
    """
    if denominator == 0:
        return "0/0, 0.00%"
    percentage = format(numerator / denominator * 100, '.2f')

    return f"{numerator}/{denominator}, {percentage}%"


def number_of_records_which_has_status(records: pd.DataFrame, status: str) -> int:
    """Return number of records that have a user-defined status.

    Args:
        records: An an individual's call history. Each row represents a call made to a respondent.
        status: A user-defined status (i.e. 'Completed').

    Returns:
        The number of records with user-defined status.

    Raises:
        BertException: number_of_records_which_has_status failed
    """
    try:
        return len(records.loc[records["status"] == status])
    except Exception as err:
        raise BertException(f"number_of_records_which_has_status failed: {err}", 400)


def convert_timedelta_to_hhmmss_as_string(td: datetime) -> str:
    """Convert a timedelta object td to a string in HH:MM:SS format.
    """
    hours, remainder = divmod(td.total_seconds(), 3600)
    minutes, seconds = divmod(remainder, 60)
    return f'{int(hours):02}:{int(minutes):02}:{int(seconds):02}'


def no_valid_records(records:pd.DataFrame) -> pd.DataFrame:
    """."""
    valid_records = get_valid_records(records)
    return valid_records.empty