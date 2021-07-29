import datetime

import numpy as np
import pandas as pd

from models.interviewer_call_pattern_model import InterviewerCallPattern
from reports.interviewer_call_history_report import get_call_history_records_by_interviewer_and_date_range

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


def get_hours_worked(call_history_dataframe):
    try:
        # group by date
        daily_call_history_by_date = call_history_dataframe.groupby(
            [call_history_dataframe['call_start_time'].dt.date]).agg({'call_start_time': min, 'call_end_time': max})

        # subtract first call time from last call time
        daily_call_history_by_date['hours_worked'] = daily_call_history_by_date['call_end_time'] - \
                                                     daily_call_history_by_date['call_start_time']

        # sum total hours
        total_hours = daily_call_history_by_date['hours_worked'].sum()
    except Exception as err:
        print(f"Could not calculate get_hours_worked(): {err}")
        raise

    # return sum total in fancy format
    return str(datetime.timedelta(seconds=total_hours.total_seconds()))


def get_call_time_in_seconds(call_history_dataframe):
    try:
        result = round(call_history_dataframe['dial_secs'].sum())
    except Exception as err:
        print(f"Could not calculate get_call_time_in_seconds(): {err}")
        raise

    return result


def convert_call_time_seconds_to_datetime_format(seconds):
    try:
        result = str(datetime.timedelta(seconds=seconds))
    except Exception as err:
        print(f"Could not convert_call_time_seconds_to_datetime_format(): {err}")
        raise

    return result


def get_total_seconds_from_string(hours_worked):
    try:
        h, m, s = hours_worked.split(':')
        result = int(h) * 3600 + int(m) * 60 + int(s)
    except Exception as err:
        print(f"Could not calculate get_total_seconds_from_string(): {err}")
        raise
    return result


def limit_two_decimal_places(float_value):
    return float("{:.2f}".format(float_value))


def get_percentage_of_hours_on_calls(hours_worked, total_call_seconds):
    try:
        value = 100 * float(total_call_seconds) / float(get_total_seconds_from_string(hours_worked))
        result = f"{limit_two_decimal_places(value)}%"
    except Exception as err:
        print(f"Could not calculate get_percentage_of_hours_on_calls(): {err}")
        raise
    return result


def get_average_calls_per_hour(call_history_dataframe, string_hours_worked):
    try:
        integer_hours_worked = get_total_seconds_from_string(string_hours_worked) / 3600
        total_calls = len(call_history_dataframe.index)

        result = limit_two_decimal_places(total_calls / integer_hours_worked)
    except Exception as err:
        print(f"Could not calculate get_average_calls_per_hour(): {err}")
        raise
    return result


def get_respondents_interviewed(call_history_dataframe):
    try:
        result = round(call_history_dataframe['number_of_interviews'].sum())
    except Exception as err:
        print(f"Could not calculate get_respondents_interviewed(): {err}")
        raise
    return result


def get_number_of_households_completed_successfully(status, call_history_dataframe):
    try:
        result = len(call_history_dataframe.loc[call_history_dataframe['status'].str.contains(status, case=False)])
    except Exception as err:
        print(f"Could not calculate get_percentage_of_call_for_status(): {err}")
        raise
    return result


def get_average_respondents_interviewed_per_hour(call_history_dataframe, string_hours_worked):
    try:
        integer_hours_worked = get_total_seconds_from_string(string_hours_worked) / 3600
        respondents_interviewed = call_history_dataframe['number_of_interviews'].sum()

        result = limit_two_decimal_places(respondents_interviewed / integer_hours_worked)
    except Exception as err:
        print(f"Could not calculate get_average_respondents_interviewed_per_hour(): {err}")
        raise
    return result


def get_percentage_of_call_for_status(status, call_history_dataframe):
    try:
        numerator = call_history_dataframe.loc[call_history_dataframe['status'].str.contains(status, case=False)]

        value = 100 * len(numerator.index) / len(call_history_dataframe.index)
        result = f"{limit_two_decimal_places(value)}%"
    except Exception as err:
        print(f"Could not calculate get_percentage_of_call_for_status(): {err}")
        raise
    return result
