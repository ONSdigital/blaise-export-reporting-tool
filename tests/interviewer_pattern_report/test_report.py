import json
from interviewer_pattern_report.report import get_call_pattern_records_by_interviewer_and_date_range
from unittest import mock


# @mock.patch.object()
# def test_get_call_pattern_records_by_interviewer_and_date_range_is_happy():
#     err, call_pattern_records = get_call_pattern_records_by_interviewer_and_date_range("matpal", "2021-01-01", "2021-06-11")
#
#     assert err is None
#     assert call_pattern_records == json.dumps(
#         {
#             "hours_worked": "3:36:40",
#             "call_time": 338,
#             "hours_on_calls_percentage": "2.6%",
#             "average_calls_per_hour": 6.369230769230769,
#             "respondents_interviewed": 23,
#             "households_completed_successfully": "0.0%",
#             "average_respondents_interviewed_per_hour": 6.369230769230769,
#             "no_contacts_percentage": "60.86956521739131%",
#             "appointments_for_contacts_percentage": "0.0%"
#         }
#     )
#

def test_generate_report():
    pass


def test_generate_report_returns_error():
    pass