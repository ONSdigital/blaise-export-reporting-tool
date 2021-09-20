import datetime
from google.api_core.datetime_helpers import DatetimeWithNanoseconds


def datetime_helper(day, hour):
    return DatetimeWithNanoseconds(2021, 8, day, hour, 00, 00, tzinfo=datetime.timezone.utc)


def interviewer_call_pattern_report_sample_case(
        start_date_time=datetime_helper(7, 9),
        end_date_time=datetime_helper(7, 15),
        dial_secs=8,
        status="Completed",
        call_result="Questionnaire"
):
    return {
        "call_start_time": start_date_time,
        "call_end_time": end_date_time,
        "dial_secs": dial_secs,
        "status": status,
        "call_result": call_result
    }
