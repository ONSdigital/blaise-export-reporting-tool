"""Produce the MI Call Pattern Report."""
import datetime
import pandas as pd
import numpy as np

from google.cloud import datastore
from functions.date_functions import parse_date_string_to_datetime
from models.error_capture import BertException

columns_to_check_for_nulls = ["call_start_time", "call_end_time"]


def get_call_pattern_report(interviewer_name: str, start_date_string: str, end_date_string: str, survey_tla: str) -> dict:
    """Return MI call pattern report.

    :parameter
    interviewer_name: str
        name of interviewer to report on
    start_date_string: str
        start date in YYYY-MM-DD format
    end_date_string: str
        end date in YYYY-MM-DD format
    survey_tla: str
        survey to report on (i.e. OPN, LMS)

    :return
    dict
        MI call pattern report

    example
        hours_worked: str (HH:MM:SS)
        call_time: str (HH:MM:SS)
        hours_on_call_percentage: str(float)
        average_calls_per_hour: str(float)
        refusals: str
        no_contact: str
        completed_successfully: str
        appointments: str
        no_contact_answer_service: str
        no_contact_busy: str
        no_contact_disconnect: str
        no_contact_no_answer: str
        no_contact_other: str
        discounted_invalid_cases: str
        invalid_fields: str
    """
    records = get_call_history_records(interviewer_name, start_date_string, end_date_string, survey_tla)
    if records.empty:
        return {}

    return {
        "hours_worked": str(calculate_hours_worked_as_datetime(records)),
        "call_time": str(calculate_call_time_as_datetime(records)),
        "hours_on_call_percentage": calculate_hours_on_call_percentage(records),
        "average_calls_per_hour": calculate_average_calls_per_hour(records),
        "refusals": percentage_of_records_with_status(records, "Finished (Non response)"),
        "no_contact": percentage_of_records_with_status(records, "Finished (No contact)"),
        "completed_successfully": percentage_of_records_with_status(records, "Completed"),
        "appointments": percentage_of_records_with_status(records, "Finished (Appointment made)"),
        "no_contact_answer_service": percentage_of_no_contact_records_with_call_result(records, "AnswerService"),
        "no_contact_busy": percentage_of_no_contact_records_with_call_result(records, "Busy"),
        "no_contact_disconnect": percentage_of_no_contact_records_with_call_result(records, "Disconnect"),
        "no_contact_no_answer": percentage_of_no_contact_records_with_call_result(records, "NoAnswer"),
        "no_contact_other": percentage_of_no_contact_records_with_call_result(records, "Others"),
        "discounted_invalid_cases": percentage_of_invalid_records(records),
        "invalid_fields": ",".join(provide_reasons_for_invalid_records(records)),
    }


def get_call_history_records(interviewer_name: str, start_date_string: str, end_date_string: str, survey_tla: str) -> pd.DataFrame:
    """Query datastore and return a pandas dataframe.

    :parameter
    interviewer_name: str
        name of interviewer to report on
    start_date_string: str
        start date in YYYY-MM-DD format
    end_date_string: str
        end date in YYYY-MM-DD format
    survey_tla: str
        survey to report on (i.e. OPN, LMS)

    :return
    pd.DataFrame
        individual call history

    :raise
    BertException
        when datastore query fails
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


def calculate_hours_worked_as_datetime(records: pd.DataFrame) -> datetime:
    """Return hours worked in datetime format.

    :parameter
    records: pd.DataFrame
        individual call history

    :returns
    datetime
        seconds worked in datetime format
    """
    valid_records = get_valid_records(records)
    hours_worked_in_seconds = calculate_hours_worked_in_seconds(valid_records)

    return datetime.timedelta(seconds=hours_worked_in_seconds)


def calculate_call_time_as_datetime(records: pd.DataFrame) -> datetime:
    """Return call time in datetime format.

    :parameter
    records: pd.DataFrame
        individual call history

    :returns
    datetime
        call time in datetime format
    """
    valid_records = get_valid_records(records)
    call_time_in_seconds = calculate_call_time_in_seconds(valid_records)

    return datetime.timedelta(seconds=call_time_in_seconds)


def calculate_hours_on_call_percentage(records: pd.DataFrame,) -> str:
    """Return hours on call as a percentage of hours worked.

    :parameter
    records: pd.DataFrame
        individual call history

    :return
    str
        percentage of time spent on call whilst working
    """

    valid_records = get_valid_records(records)

    hours_worked_in_seconds = calculate_hours_worked_in_seconds(valid_records)
    call_time_in_seconds = calculate_call_time_in_seconds(valid_records)
    hours_on_call_percentage = round(call_time_in_seconds / hours_worked_in_seconds * 100, 2)

    return f"{hours_on_call_percentage}%"


def calculate_average_calls_per_hour(records: pd.DataFrame) -> float:
    """Return average number of calls made per hour worked.

    :parameter
    records: pd.DataFrame
        individual call history

    :return
    float
        average number of calls made per hour
    """
    valid_records = get_valid_records(records)
    hours_worked_in_seconds = calculate_hours_worked_in_seconds(valid_records)

    number_of_valid_records = len(valid_records)
    hours_worked = hours_worked_in_seconds / 3600

    return round(number_of_valid_records / float(hours_worked), 2)


def percentage_of_records_with_status(records: pd.DataFrame, status: str) -> str:
    """Calculate number of records with a user-defined status and return result in fraction, percentage string format.

    :parameter
    records: pd.DataFrame
        individual call history
    status: str
        user-defined status to be searched for (i.e. 'Completed')

    :return
    string
        the number of records with the user-defined status in fraction, percentage string format

    :example
        2/4, 50%
    """
    number_of_records_with_status = number_of_records_which_has_status(records, status)

    return str(format_fraction_and_percentage_as_string(number_of_records_with_status, len(records)))


def percentage_of_no_contact_records_with_call_result(records: pd.DataFrame, call_result: str) -> str:
    """Calculate number of records with a status of 'no_contact' and user-defined 'call_result' and return result in
    fraction, percentage string format.

    :parameter
    records: pd.DataFrame
        individual call history
    status: str
        user-defined call_result to be searched for (i.e. 'NoAnswer')

    :return
    string
        the number of records with the user-defined call_result in fraction, percentage string format

    :example
        2/4, 50%
    """
    number_of_records_with_call_result = len(records.loc[
                                                 (records["status"] == "Finished (No contact)") &
                                                 (records["call_result"] == call_result)])

    number_of_records_with_no_contact = number_of_records_which_has_status(records, "Finished (No contact)")

    return str(format_fraction_and_percentage_as_string(number_of_records_with_call_result, number_of_records_with_no_contact))


def percentage_of_invalid_records(records: pd.DataFrame) -> str:
    """Calculate the number of invalid records and return in fraction, percentage string format.

    :notes
        invalid records are records with empty cells or Nan values in fields defined in the global variable
        'columns_to_check_for_nulls'

    :parameter
    records: pd.DataFrame
        individual call history

    :return
    string
        the number of invalid records in fraction, percentage string format

    :example
        2/4, 50%
    """
    valid_records = get_valid_records(records)
    total_number_of_records = len(records)
    total_number_of_invalid_records = total_number_of_records - len(valid_records)

    return str(format_fraction_and_percentage_as_string(total_number_of_invalid_records, total_number_of_records))


def provide_reasons_for_invalid_records(records: pd.DataFrame) -> list:
    """Return a list of unique reasons for invalid records.

    :parameter
    records: pd.DataFrame
        individual call history

    :return
    list
        unique reasons for invalid records
    """
    reasons = []
    if records.status.str.contains("Timed out", case=False).any():
        reasons.append("'status' column had timed out call status")

    for field in columns_to_check_for_nulls:
        if records[field].isna().any():
            reasons.append(f"'{field}' column had missing data")

    return reasons


def get_valid_records(records: pd.DataFrame) -> pd.DataFrame:
    """Calculate the number of invalid records and return in fraction, percentage string format.

    :notes
        invalid records are records with empty cells or Nan values in fields defined in the global variable
        'columns_to_check_for_nulls' and a status of 'Timed out'

    :parameter
    records: pd.DataFrame
        individual call history

    :return
    pd.DataFrame
        valid individual call history records

    :raise
    BertException
        when valid records can not be obtained
    """
    try:
        records = records.replace("", np.nan).fillna(value=np.nan)
        valid_records = records.drop(records.loc[records['status'].str.contains('Timed out', case=False)].index)
        valid_records = valid_records.dropna(subset=columns_to_check_for_nulls)

        return valid_records

    except Exception as err:
        raise BertException(f"get_valid_records failed: {err}", 400)


def calculate_hours_worked_in_seconds(records: pd.DataFrame) -> int:
    """Return hours worked in seconds.

    :parameter
    records: pd.DataFrame
        individual call history

    :return
    int
        hours worked in seconds

    :raise
    BertException
        when hours worked can not be calculated
    """
    try:
        daily_call_history_by_date = records.groupby(
            [records['call_start_time'].dt.date]).agg({'call_start_time': min, 'call_end_time': max})
        daily_call_history_by_date['hours_worked'] = daily_call_history_by_date['call_end_time'] - daily_call_history_by_date['call_start_time']

        return daily_call_history_by_date['hours_worked'].sum().total_seconds()
    except Exception as err:
        raise BertException(f"calculate_hours_worked_in_seconds failed: {err}", 400)


def calculate_call_time_in_seconds(records: pd.DataFrame) -> int:
    """Return call time in seconds.

    :parameter
    records: pd.DataFrame
        individual call history

    :return
    int
        time on call in seconds

    :raise
    BertException
        when call time can not be calculated
    """
    try:
        return round(records['dial_secs'].sum())
    except Exception as err:
        raise BertException(f"calculate_call_time_in_seconds failed: {err}", 400)


def format_fraction_and_percentage_as_string(numerator: int, denominator: int) -> str:
    """Return a string in fraction, percentage format.

    :parameter
    numerator: int

    denominator: int

    :return
    str
        result in fraction, percentage format

    :example
        2/4, 50%
    """
    if denominator == 0:
        return "0/0, 100.00%"
    percentage = format(numerator / denominator * 100, '.2f')

    return f"{numerator}/{denominator}, {percentage}%"


def number_of_records_which_has_status(records: pd.DataFrame, status: str) -> int:
    """Return number of records that have a user-defined status.

    :parameter
    records: pd.DataFrame
        individual call history
    status: str
        user-defined status (i.e. 'Completed')

    :return
    int
        number of records with user-defined status
    """
    try:
        return len(records.loc[records["status"] == status])
    except Exception as err:
        raise BertException(f"number_of_records_which_has_status failed: {err}", 400)