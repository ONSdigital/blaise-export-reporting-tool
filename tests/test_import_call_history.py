import pytest

from import_call_history import append_case_data_to_dials
from models.call_history import CallHistory


def test_append_case_data_to_dials():
    # List of call_history objects
    call_history = [
        CallHistory(
            questionnaire_id="05cf69af-3a4e-47df-819a-928350fdda5a",
            serial_number="1001031",
            call_number=1,
            dial_number=1,
            busy_dials=0,
            call_start_time="2021-05-12 13:17:56.8191819",
            call_end_time="2021-05-12 13:18:06.1431819",
            dial_secs=9,
            status="Finished (No contact)",
            interviewer="Edwin",
            call_result="Busy",
            update_info=None,
            appointment_info=None,
            questionnaire_name="LMS2101_AA1",
            wave=1,
            cohort="AA",
            number_of_interviews=None,
            outcome_code=None,
            survey="LMS",
        )
    ]
    # JSON data of Blaise case data
    cases = [
        {
            "qhAdmin.HOut": 310,
            "qHousehold.QHHold.HHSize": 2,
            "qiD.Serial_Number": "1001031",
            "Stage": "202102",
            "questionnaire_name": "LMS2101_AA1",
        }
    ]

    merged_data = append_case_data_to_dials(call_history, cases)

    assert merged_data[0].outcome_code == 310
    assert merged_data[0].number_of_interviews == 2
    assert merged_data == [
        CallHistory(
            questionnaire_id="05cf69af-3a4e-47df-819a-928350fdda5a",
            serial_number="1001031",
            call_number=1,
            dial_number=1,
            busy_dials=0,
            call_start_time="2021-05-12 13:17:56.8191819",
            call_end_time="2021-05-12 13:18:06.1431819",
            dial_secs=9,
            status="Finished (No contact)",
            interviewer="Edwin",
            call_result="Busy",
            update_info=None,
            appointment_info=None,
            questionnaire_name="LMS2101_AA1",
            survey="LMS",
            wave=1,
            cohort="AA",
            number_of_interviews=2,
            outcome_code=310,
        )
    ]
