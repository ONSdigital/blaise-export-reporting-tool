import datetime

import numpy as np
import pandas as pd

from models.error_capture import BertException
from models.interviewer_call_pattern_model import InterviewerCallPattern, InterviewerCallPatternWithNoValidData
from reports.interviewer_call_history_report import get_call_history_records

COLUMNS_TO_VALIDATE = ["call_start_time", "call_end_time", "number_of_interviews"]


def get_call_pattern_records_by_interviewer_and_date_range(interviewer_name, start_date_string,
                                                           end_date_string, survey_tla):
    print(f"""Getting call pattern data for interviewer '{interviewer_name}' 
    between '{start_date_string}' and '{end_date_string}'""")

    call_history_records = get_call_history_records(
        interviewer_name, start_date_string, end_date_string, survey_tla
    )
    if not call_history_records:
        return {}

    call_history_dataframe = create_dataframe(call_history_records)
    valid_dataframe, discounted_records, discounted_fields = validate_dataframe(call_history_dataframe)

    if valid_dataframe.empty:
        report = InterviewerCallPatternWithNoValidData()
        report.discounted_invalid_records = discounted_records
        report.invalid_fields = discounted_fields
        return report

    report = generate_report(valid_dataframe, discounted_records, discounted_fields)
    return report


def create_dataframe(call_history):
    try:
        result = pd.DataFrame(data=call_history)
    except Exception as err:
        raise BertException(f"create_dataframe failed: {err}", 400)
    return result


def validate_dataframe(data):
    valid_data = data.copy()
    discounted_records = ""
    discounted_fields = ""

    valid_data.columns = valid_data.columns.str.lower()
    if invalid_data_found(valid_data):
        valid_data, discounted_records, discounted_fields = get_invalid_data(data)

    try:
        valid_data = valid_data.astype({"number_of_interviews": "int32", "dial_secs": "float64"})
    except Exception as err:
        raise BertException(f"validate_dataframe failed: {err}", 400)
    return valid_data, discounted_records, discounted_fields


def invalid_data_found(data):
    timed_out_data_found = data.loc[data['status'].str.contains('Timed out', case=False)].any().any()
    missing_data_found = data.filter(COLUMNS_TO_VALIDATE).isna().any().any()

    if timed_out_data_found or missing_data_found:
        return True

    return False


def get_invalid_data(data):
    valid_records = data.copy()
    valid_records['number_of_interviews'].replace('', np.nan, inplace=True)

    valid_records.drop(valid_records.loc[valid_records['status'].str.contains('Timed out', case=False)].index,
                       inplace=True)
    valid_records.dropna(subset=COLUMNS_TO_VALIDATE, inplace=True)

    invalid_records = data[~data.index.isin(valid_records.index)]
    valid_records.reset_index(drop=True, inplace=True)
    invalid_records.reset_index(drop=True, inplace=True)

    discounted_records = f"{len(invalid_records.index)}/{len(data.index)}"
    discounted_fields = get_invalid_fields(invalid_records)

    return valid_records, discounted_records, discounted_fields


def generate_report(valid_call_history_dataframe, discounted_records=None, discounted_fields=None):
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

    if discounted_records is not None:
        report.discounted_invalid_records = discounted_records
        report.invalid_fields = discounted_fields

    return report


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
        raise BertException(f"Could not calculate get_hours_worked(): {err}", 400)

    # return sum total in fancy format
    return str(datetime.timedelta(seconds=total_hours.total_seconds()))


def get_call_time_in_seconds(call_history_dataframe):
    try:
        result = round(call_history_dataframe['dial_secs'].sum())
    except Exception as err:
        raise BertException(f"Could not calculate get_call_time_in_seconds(): {err}", 400)
    return result


def convert_call_time_seconds_to_datetime_format(seconds):
    try:
        result = str(datetime.timedelta(seconds=seconds))
    except Exception as err:
        raise BertException(f"Could not convert_call_time_seconds_to_datetime_format(): {err}", 400)
    return result


def get_percentage_of_hours_on_calls(hours_worked, total_call_seconds):
    try:
        value = 100 * float(total_call_seconds) / float(get_total_seconds_from_string(hours_worked))
        result = f"{limit_two_decimal_places(value)}%"
    except Exception as err:
        raise BertException(f"Could not calculate get_percentage_of_hours_on_calls(): {err}", 400)
    return result


def get_total_seconds_from_string(hours_worked):
    try:
        h, m, s = hours_worked.split(':')
        result = int(h) * 3600 + int(m) * 60 + int(s)
    except Exception as err:
        raise BertException(f"Could not calculate get_total_seconds_from_string(): {err}", 400)
    return result


def limit_two_decimal_places(float_value):
    try:
        result = float("{:.2f}".format(float_value))
    except Exception as err:
        raise BertException(f"Could not convert {float_value} to float: {err}")
    return result


def get_average_calls_per_hour(call_history_dataframe, string_hours_worked):
    try:
        integer_hours_worked = get_total_seconds_from_string(string_hours_worked) / 3600
        total_calls = len(call_history_dataframe.index)

        result = limit_two_decimal_places(total_calls / integer_hours_worked)
    except Exception as err:
        raise BertException(f"Could not calculate get_average_calls_per_hour(): {err}", 400)
    return result


def get_respondents_interviewed(call_history_dataframe):
    try:
        result = round(call_history_dataframe['number_of_interviews'].sum())
    except Exception as err:
        raise BertException(f"Could not calculate get_respondents_interviewed(): {err}", 400)
    return result


def get_number_of_households_completed_successfully(status, call_history_dataframe):
    try:
        result = len(call_history_dataframe.loc[call_history_dataframe['status'].str.contains(status, case=False)])
    except Exception as err:
        raise BertException(f"Could not calculate get_percentage_of_call_for_status(): {err}", 400)
    return result


def get_average_respondents_interviewed_per_hour(call_history_dataframe, string_hours_worked):
    try:
        integer_hours_worked = get_total_seconds_from_string(string_hours_worked) / 3600
        respondents_interviewed = call_history_dataframe['number_of_interviews'].sum()
        result = limit_two_decimal_places(respondents_interviewed / integer_hours_worked)
    except Exception as err:
        raise BertException(f"Could not calculate get_average_respondents_interviewed_per_hour(): {err}", 400)
    return result


def get_percentage_of_call_for_status(status, call_history_dataframe):
    try:
        numerator = call_history_dataframe.loc[call_history_dataframe['status'].str.contains(status, case=False)]

        value = 100 * len(numerator.index) / len(call_history_dataframe.index)
        result = f"{limit_two_decimal_places(value)}%"
    except Exception as err:
        raise BertException(f"Could not calculate get_percentage_of_call_for_status(): {err}", 400)
    return result


def get_invalid_fields(data):
    invalid_fields = []

    if data.loc[data['status'].str.contains('Timed out', case=False)].any().any():
        invalid_fields.append("'status' column had timed out call status")

    data = data.filter(COLUMNS_TO_VALIDATE)
    for field in data.columns[data.isna().any()]:
        invalid_fields.append(f"'{field}' column had missing data")
    return ", ".join(invalid_fields)
