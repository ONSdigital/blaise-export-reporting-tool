from dataclasses import dataclass
from datetime import datetime


@dataclass
class MICallHistory:
    questionnaire_id: str
    serial_number: str
    internal_key: int
    call_number: int
    dial_number: int
    interviewer: str
    dial_result: int
    dial_line_number: int
    seconds_dial: int
    end_time: str = ""
    questionnaire_name: str = ""
    dial_date: str = ""
    dial_time: str = ""
    entry_priority: int = ""
    exit_priority: int = ""
    appointment_type: str = ""
    seconds_interview: datetime = ""
    outcome_code: int = ""

    def generate_dial_date_and_time_fields(self, start_datetime, end_datetime):
        self.dial_date = start_datetime.strftime("%Y%m%d")
        self.dial_time = start_datetime.strftime("%H:%M:%S")
        if end_datetime is not None:
            self.end_time = end_datetime.strftime("%H:%M:%S")
        pass

    # class MICallHistory:
    #     primary_key: str = ""    -- PrimaryKeyValue
    #     internal_key: int = ""   -- Id
    #     sample_month: str = ""   -- questionaireName
    #     dial_date: datetime = ""   -- StartTime split
    #     dial_time: datetime = ""   -- StartTime
    #     call_number: int = ""   -- CallNumber
    #     dial_number: int = ""   -- DialNumber
    #     interviewer: str = ""   -- Interviewer
    #     entry_priority: int = ""   -- ?
    #     exit_priority: int = ""   -- ?
    #     dial_result: int = ""   -- DialResult
    #     dial_line_number: int = ""   -- DialedNumber
    #     appointment_type: str = ""   -- AppointmentInfo -- XML
    #     end_time: datetime = ""   -- EndTime
    #     seconds_dial: datetime = ""   -- dialsecs count-
    #     seconds_interview: datetime = ""   -- ?
    #     outcome_code: int = ""   -- HOUT
