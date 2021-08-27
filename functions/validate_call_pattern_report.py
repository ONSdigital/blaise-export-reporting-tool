import datetime
import pandas as pd
import numpy as np

from functions.interviewer_call_pattern_data_functions import get_hours_worked, get_call_time_in_seconds, get_total_seconds_from_string

from models.error_capture import BertException

COLUMNS_TO_VALIDATE = ["call_start_time", "call_end_time", "number_of_interviews"]


def validate_dataframe(data):
    if invalid_data_found(data) or call_time_is_less_than_hours_worked(data):
        return handle_invalid_data(data)
    return data, pd.DataFrame()


def invalid_data_found(data):
    timed_out_questionnaire_found = data.loc[data['status'].str.contains('Timed out', case=False)].any().any()
    missing_data_found = data.filter(COLUMNS_TO_VALIDATE).isna().any().any()

    if timed_out_questionnaire_found or missing_data_found:
        return True

    return False


def call_time_is_less_than_hours_worked(data):
    hours_worked = get_hours_worked(data)
    total_call_seconds = get_call_time_in_seconds(data)

    if total_call_seconds > get_total_seconds_from_string(
            hours_worked):
        return True


def handle_invalid_data(data):
    valid_dataframe = create_valid_dataframe(data)

    invalid_dataframe = data[~data.index.isin(valid_dataframe.index)]
    valid_dataframe.reset_index(drop=True, inplace=True)
    invalid_dataframe.reset_index(drop=True, inplace=True)

    if call_time_is_less_than_hours_worked(valid_dataframe):
        raise BertException(f"Hours worked ({get_hours_worked(valid_dataframe)}) cannot be less than time spent on calls ({datetime.timedelta(seconds=get_call_time_in_seconds(valid_dataframe))}). Please review the Call History data", 400)

    return valid_dataframe, invalid_dataframe


def create_valid_dataframe(data):
    valid_df = data.copy()
    valid_df[COLUMNS_TO_VALIDATE].replace('', np.nan, inplace=True)
    valid_df.dropna(subset=COLUMNS_TO_VALIDATE, inplace=True)
    valid_df.drop(valid_df.loc[valid_df['status'].str.contains(
        'Timed out', case=False)].index, inplace=True)

    return valid_df


def get_invalid_fields(data):
    invalid_fields = []

    if data.loc[data['status'].str.contains('Timed out', case=False)].any().any():
        invalid_fields.append("'status' column had timed out call status")

    data = data.filter(COLUMNS_TO_VALIDATE)
    for field in data.columns[data.isna().any()]:
        invalid_fields.append(f"'{field}' column had missing data")
    return ", ".join(invalid_fields)
