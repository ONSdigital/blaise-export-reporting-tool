import datetime

import pandas as pd
from google.api_core.datetime_helpers import DatetimeWithNanoseconds

from interviewer_pattern_report.derived_variables import get_hours_worked, get_total_call_seconds


def test_get_hours_worked():
    data = [
        {
            'call_start_time': DatetimeWithNanoseconds(2021, 1, 1, 10, 00, 00, tzinfo=datetime.timezone.utc),
            'call_end_time': DatetimeWithNanoseconds(2021, 1, 1, 10, 30, 00, tzinfo=datetime.timezone.utc)
        },
        {
            'call_start_time': DatetimeWithNanoseconds(2021, 1, 11, 00, 00, 00, tzinfo=datetime.timezone.utc),
            'call_end_time': DatetimeWithNanoseconds(2021, 1, 1, 11, 30, 00, tzinfo=datetime.timezone.utc)
        },
        {
            'call_start_time': DatetimeWithNanoseconds(2021, 1, 2, 10, 00, 00, tzinfo=datetime.timezone.utc),
            'call_end_time': DatetimeWithNanoseconds(2021, 1, 2, 11, 00, 00, tzinfo=datetime.timezone.utc)
        },
        {
            'call_start_time': DatetimeWithNanoseconds(2021, 1, 5, 14, 00, 00, tzinfo=datetime.timezone.utc),
            'call_end_time': DatetimeWithNanoseconds(2021, 1, 5, 16, 00, 00, tzinfo=datetime.timezone.utc)
        }
    ]
    df_data = pd.DataFrame(data)
    assert get_hours_worked(df_data) == "4:30:00"


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
