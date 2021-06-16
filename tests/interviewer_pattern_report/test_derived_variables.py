import datetime

import pandas as pd
from google.api_core.datetime_helpers import DatetimeWithNanoseconds

from interviewer_pattern_report.derived_variables import get_hours_worked, get_total_call_seconds, \
    get_percentage_of_time_on_calls


def test_get_hours_worked(mock_data):
    assert get_hours_worked(mock_data) == "15:00:00"


def test_get_total_call_seconds():
    data = [
        {
            'dial_secs': 60
        },
        {
            'dial_secs': 60
        },
        {
            'dial_secs': 60
        },
        {
            'dial_secs': 60
        }
    ]
    df_data = pd.DataFrame(data)
    assert get_total_call_seconds(df_data) == 240


def test_get_percentage_of_time_on_calls():
    hours_worked = "1:00:30"
    total_call_seconds = "1815"
    assert get_percentage_of_time_on_calls(hours_worked, total_call_seconds) == 50
