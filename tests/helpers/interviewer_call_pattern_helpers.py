import datetime
from google.api_core.datetime_helpers import DatetimeWithNanoseconds


def datetime_helper(day, hour):
    return DatetimeWithNanoseconds(2021, 8, day, hour, 00, 00, tzinfo=datetime.timezone.utc)


def interviewer_call_pattern_report_sample_case(
        call_start_time=datetime_helper(7, 9),
        call_end_time=datetime_helper(7, 15),
        dial_secs=8,
        status="Completed",
        call_result="Questionnaire",
):

    return {
        "call_start_time": call_start_time,
        "call_end_time": call_end_time,
        "dial_secs": dial_secs,
        "status": status,
        "call_result": call_result,
        "interviewer": "James",
        "survey_tla": "OPN",
        "start_date_as_string": "2021-09-22",
        "end_date_as_string": "2021-09-22",
    }
