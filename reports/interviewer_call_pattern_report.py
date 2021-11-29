import datetime

import numpy as np
import pandas as pd
from google.cloud import datastore

from functions.date_functions import parse_date_string_to_datetime

from models.interviewer_call_pattern_model import (
    InterviewerCallPattern, InterviewerCallPatternWithNoValidData
)
from models.error_capture import BertException

columns_to_check_for_nulls = ["call_start_time", "call_end_time"]


def get_call_pattern_report(interviewer_name: str, start_date_string: str,
                            end_date_string: str, survey_tla: str) -> object:
    records = get_call_history_records(interviewer_name, start_date_string, end_date_string, survey_tla)

    print(f"Calculating call pattern data for interviewer '{interviewer_name}'")

    if records.empty:
        return {}

    if no_valid_records_are_found(records):
        return InterviewerCallPatternWithNoValidData(
            discounted_invalid_cases=percentage_of_invalid_records(records),
            invalid_fields=", ".join(provide_reasons_for_invalid_records(records))
        )

    return InterviewerCallPattern(
        total_valid_cases=len(get_valid_records(records)),
        hours_worked=str(calculate_hours_worked_as_datetime(records)),
        call_time=str(calculate_call_time_as_datetime(records)),
        hours_on_calls_percentage=calculate_hours_on_call_percentage(records),
        average_calls_per_hour=calculate_average_calls_per_hour(records),
        refusals=total_records_with_status(records, "Finished (Non response)"),
        no_contacts=total_records_with_status(records, "Finished (No contact)"),
        completed_successfully=total_records_with_status(records, "Completed"),
        appointments_for_contacts=total_records_with_status(records, "Finished (Appointment made)"),
        no_contact_answer_service=total_no_contact_records_with_call_result(records, "AnswerService"),
        no_contact_busy=total_no_contact_records_with_call_result(records, "Busy"),
        no_contact_disconnect=total_no_contact_records_with_call_result(records, "Disconnect"),
        no_contact_no_answer=total_no_contact_records_with_call_result(records, "NoAnswer"),
        no_contact_other=total_no_contact_records_with_call_result(records, "Others"),
        discounted_invalid_cases=percentage_of_invalid_records(records),
        invalid_fields=", ".join(provide_reasons_for_invalid_records(records))
    )


def get_call_history_records(
        interviewer_name: str, start_date_string: str,
        end_date_string: str, survey_tla: str,
) -> pd.DataFrame:
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
    valid_records = get_valid_records(records)
    hours_worked_in_seconds = calculate_hours_worked_in_seconds(valid_records)

    return convert_timedelta_to_hhmmss_as_string(datetime.timedelta(seconds=hours_worked_in_seconds))


def calculate_call_time_as_datetime(records: pd.DataFrame) -> datetime:
    valid_records = get_valid_records(records)
    call_time_in_seconds = calculate_call_time_in_seconds(valid_records)

    return convert_timedelta_to_hhmmss_as_string(datetime.timedelta(seconds=call_time_in_seconds))


def calculate_hours_on_call_percentage(records: pd.DataFrame, ) -> float:
    valid_records = get_valid_records(records)

    hours_worked_in_seconds = calculate_hours_worked_in_seconds(valid_records)
    call_time_in_seconds = calculate_call_time_in_seconds(valid_records)

    return round(call_time_in_seconds / hours_worked_in_seconds * 100, 2)


def calculate_average_calls_per_hour(records: pd.DataFrame) -> float:
    valid_records = get_valid_records(records)

    hours_worked_in_seconds = calculate_hours_worked_in_seconds(valid_records)
    number_of_valid_records = len(valid_records)
    hours_worked = hours_worked_in_seconds / 3600

    return round(number_of_valid_records / float(hours_worked), 2)


def total_records_with_status(records: pd.DataFrame, status: str) -> int:
    return number_of_records_which_has_status(get_valid_records(records), status)


def total_no_contact_records_with_call_result(records: pd.DataFrame, call_result: str) -> int:
    valid_records = get_valid_records(records)
    number_of_records_with_call_result = len(valid_records.loc[
                                                 (valid_records["status"] == "Finished (No contact)") &
                                                 (valid_records["call_result"] == call_result)])

    return number_of_records_with_call_result


def percentage_of_invalid_records(records: pd.DataFrame) -> int:
    valid_records = get_valid_records(records)

    if len(valid_records) == len(records):
        return 0

    return len(records) - len(valid_records)


def provide_reasons_for_invalid_records(records: pd.DataFrame) -> list[str]:
    reasons = []
    if records.status.str.contains("Timed out", case=False).any():
        reasons.append("'status' column had timed out call status")

    for field in columns_to_check_for_nulls:
        if records[field].isna().any():
            reasons.append(f"'{field}' column had missing data")

    return reasons


def get_valid_records(records: pd.DataFrame) -> pd.DataFrame:
    try:
        records = records.replace("", np.nan).fillna(value=np.nan)
        valid_records = records.dropna(subset=columns_to_check_for_nulls)
        if valid_records.empty:
            return pd.DataFrame()
        valid_records = valid_records.drop(
            valid_records.loc[valid_records['status'].str.contains('Timed out', case=False)].index)

        return valid_records

    except Exception as err:
        raise BertException(f"get_valid_records failed: {err}", 400)


def calculate_hours_worked_in_seconds(records: pd.DataFrame) -> int:
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
    try:
        return round(records['dial_secs'].sum())
    except Exception as err:
        raise BertException(f"calculate_call_time_in_seconds failed: {err}", 400)


def number_of_records_which_has_status(records: pd.DataFrame, status: str) -> int:
    try:
        return len(records.loc[records["status"] == status])
    except Exception as err:
        raise BertException(f"number_of_records_which_has_status failed: {err}", 400)


def convert_timedelta_to_hhmmss_as_string(td: datetime) -> str:
    hours, remainder = divmod(td.total_seconds(), 3600)
    minutes, seconds = divmod(remainder, 60)
    return f'{int(hours):02}:{int(minutes):02}:{int(seconds):02}'


def no_valid_records_are_found(records:pd.DataFrame) -> bool:
    return get_valid_records(records).empty
