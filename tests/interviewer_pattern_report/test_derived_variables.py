import datetime

from google.api_core.datetime_helpers import DatetimeWithNanoseconds

from interviewer_pattern_report.derived_variables import get_hours_worked, get_total_call_seconds


def test_hours_worked_is_calculated_correctly():
    results = [
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
    assert get_hours_worked(results) == "4:30:00"


def test_get_call_time():
    results = [
        {
            'call_start_time': DatetimeWithNanoseconds(2021, 1, 1, 10, 00, 00, tzinfo=datetime.timezone.utc),
            'call_end_time': DatetimeWithNanoseconds(2021, 1, 1, 10, 30, 00, tzinfo=datetime.timezone.utc),
            'dialsecs': 60
        },
        {
            'call_start_time': DatetimeWithNanoseconds(2021, 1, 11, 00, 00, 00, tzinfo=datetime.timezone.utc),
            'call_end_time': DatetimeWithNanoseconds(2021, 1, 1, 11, 30, 00, tzinfo=datetime.timezone.utc),
            'dialsecs': 60
        },
        {
            'call_start_time': DatetimeWithNanoseconds(2021, 1, 2, 10, 00, 00, tzinfo=datetime.timezone.utc),
            'call_end_time': DatetimeWithNanoseconds(2021, 1, 2, 11, 00, 00, tzinfo=datetime.timezone.utc),
            'dialsecs': 60
        },
        {
            'call_start_time': DatetimeWithNanoseconds(2021, 1, 5, 14, 00, 00, tzinfo=datetime.timezone.utc),
            'call_end_time': DatetimeWithNanoseconds(2021, 1, 5, 16, 00, 00, tzinfo=datetime.timezone.utc),
            'dialsecs': 60
        }
    ]
    assert get_total_call_seconds(results) == 240
