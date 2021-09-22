import datetime
import numpy as np
import pandas as pd

from models.error_capture import BertException
from models.interviewer_call_pattern_model import InterviewerCallPattern, InterviewerCallPatternWithNoValidData
from reports.interviewer_call_history_report import get_call_history_records

COLUMNS_TO_VALIDATE = ["call_start_time", "call_end_time", "number_of_interviews"]


def get_call_pattern_report(interviewer_name, start_date_string, end_date_string, survey_tla):
    print(
        f"Getting call pattern data for interviewer '{interviewer_name}' between '{start_date_string}' and '{end_date_string}'")
    call_history_records = get_call_history_records(
        interviewer_name, start_date_string, end_date_string, survey_tla)
    if not call_history_records:
        return {}

    original_number_of_records = len(call_history_records)
    call_history_dataframe = create_dataframe(call_history_records)
    valid_dataframe, discounted_records, discounted_fields = validate_dataframe(call_history_dataframe)

    if valid_dataframe.empty:
        report = InterviewerCallPatternWithNoValidData()
        report.discounted_invalid_cases = discounted_records
        report.invalid_fields = discounted_fields
        return report

    report = generate_report(valid_dataframe, original_number_of_records, discounted_records, discounted_fields)
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

    numerator = len(invalid_records.index)
    denominator = len(data.index)
    percentage = round(100 * numerator / denominator, 2)

    discounted_records = f"{numerator}/{denominator}, {percentage}%"
    discounted_fields = get_invalid_fields(invalid_records)

    return valid_records, discounted_records, discounted_fields


def generate_report(df, original_number_of_records, discounted_records=None, discounted_fields=None):
    hours_worked = get_hours_worked(df)
    total_call_seconds = get_call_time_in_seconds(df)

    report = InterviewerCallPattern(
        hours_worked=hours_worked,
        call_time=convert_call_time_seconds_to_datetime_format(total_call_seconds),
        hours_on_calls_percentage=get_percentage_of_hours_on_calls(hours_worked, total_call_seconds),
        average_calls_per_hour=get_average_calls_per_hour(df, hours_worked),
        respondents_interviewed=get_respondents_interviewed(df),
        average_respondents_interviewed_per_hour=get_average_respondents_interviewed_per_hour(
            df, hours_worked),
        completed_successfully=results_for_calls_with_status('status', 'completed', df, original_number_of_records),
        no_contacts=results_for_calls_with_status('status', 'no contact', df, original_number_of_records),
        appointments_for_contacts=results_for_calls_with_status('status', 'appointment', df, original_number_of_records),
        refusals=results_for_calls_with_status('status', 'non response', df, original_number_of_records),
        no_contact_answer_service=no_contact_breakdown('answerservice', df),
        no_contact_busy=no_contact_breakdown('busy', df),
        no_contact_disconnect=no_contact_breakdown('disconnect', df),
        no_contact_no_answer=no_contact_breakdown('noanswer', df),
        no_contact_other=no_contact_breakdown('others', df),
    )

    if discounted_records is not None:
        report.discounted_invalid_cases = discounted_records
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
        value = round(100 * float(total_call_seconds) / float(get_total_seconds_from_string(hours_worked)), 2)
        result = f"{value}%"
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


def get_average_calls_per_hour(call_history_dataframe, string_hours_worked):
    try:
        integer_hours_worked = get_total_seconds_from_string(string_hours_worked) / 3600
        total_calls = len(call_history_dataframe.index)

        result = round(total_calls / integer_hours_worked, 2)
    except Exception as err:
        raise BertException(f"Could not calculate get_average_calls_per_hour(): {err}", 400)
    return result


def get_respondents_interviewed(call_history_dataframe):
    try:
        result = round(call_history_dataframe['number_of_interviews'].sum())
    except Exception as err:
        raise BertException(f"Could not calculate get_respondents_interviewed(): {err}", 400)
    return result


def get_average_respondents_interviewed_per_hour(call_history_dataframe, string_hours_worked):
    try:
        integer_hours_worked = get_total_seconds_from_string(string_hours_worked) / 3600
        respondents_interviewed = call_history_dataframe['number_of_interviews'].sum()
        result = round(respondents_interviewed / integer_hours_worked, 2)
    except Exception as err:
        raise BertException(f"Could not calculate get_average_respondents_interviewed_per_hour(): {err}", 400)
    return result


def get_invalid_fields(data):
    invalid_fields = []

    if data.loc[data['status'].str.contains('Timed out', case=False)].any().any():
        invalid_fields.append("'status' column had timed out call status")

    data = data.filter(COLUMNS_TO_VALIDATE)
    for field in data.columns[data.isna().any()]:
        invalid_fields.append(f"'{field}' column had missing data")
    return ", ".join(invalid_fields)


def results_for_calls_with_status(column_name, status, df, denominator):
    try:
        numerator = df[column_name].str.contains(status, case=False, na=False).sum()
        percentage = 100 * numerator / denominator
    except Exception as err:
        raise BertException(f"Could not calculate the total for status containing '{status}': {err}", 400)
    return f"{numerator}/{denominator}, {round(percentage, 2)}%"
