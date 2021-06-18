import json
from interviewer_pattern_report.report import get_call_pattern_records_by_interviewer_and_date_range
from unittest import mock


# # @mock.patch.object()
# def test_get_call_pattern_records_by_interviewer_and_date_range_is_happy():
#     err, call_pattern_records = get_call_pattern_records_by_interviewer_and_date_range("matpal", "2021-01-01", "2021-06-11")

#     assert err is None
#     assert call_pattern_records == json.dumps(
#         {
#             "HoursWorked": "3:36:40",
#             "CallTime": 338,
#             "HoursOnCallsPercentage": "2.6%",
#             "AverageCallsPerHour": 6.369230769230769,
#             "RespondentsInterviewed": 23,
#             "HouseholdsCompletedSuccessfully": "0.0%",
#             "AverageRespondentsInterviewedPerHour": 6.369230769230769,
#             "NoContactsPercentage": "60.86956521739131%",
#             "AppointmentsForContactsPercentage": "0.0%"
#         }
#     )
