from dataclasses import dataclass, fields
from datetime import datetime

from models.database_base_model import DataBaseBase


@dataclass
class MiHubCallHistory:
    questionnaire_name: str = ""
    questionnaire_id: str = None
    serial_number: str = None
    dial_date: str = ""
    dial_time: str = ""
    end_time: str = ""
    seconds_interview: int = None
    call_number: int = None
    dial_number: int = None
    interviewer: str = None
    dial_result: int = None
    dial_line_number: int = None
    appointment_type: str = ""
    outcome_code: int = ""

    def generate_dial_date_and_time_fields(self, start_datetime, end_datetime):
        self.dial_date = start_datetime.strftime("%Y%m%d")
        self.dial_time = start_datetime.strftime("%H:%M:%S")
        if end_datetime is not None:
            self.end_time = end_datetime.strftime("%H:%M:%S")
        pass

    @classmethod
    def fields(cls):
        return [field.name for field in fields(cls)]


@dataclass
class CatiMiHubCallHistoryTable(DataBaseBase):
    InstrumentId: str
    PrimaryKeyValue: str
    Id: int
    StartTime: datetime
    CallNumber: int
    DialNumber: int
    Interviewer: str
    DialResult: str
    DialedNumber: str
    AppointmentInfo: str
    EndTime: datetime

    @classmethod
    def table_name(cls):
        return "cati.DialHistory"

    @classmethod
    def extra_fields(cls):
        return ["ABS(TIME_TO_SEC(TIMEDIFF(EndTime, StartTime))) as dial_secs"]
