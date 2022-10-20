from dataclasses import dataclass, fields
from datetime import datetime
from typing import Optional, Union

from models.database_base_model import DatabaseBase


@dataclass
class MiHubCallHistory:
    questionnaire_name: str = ""
    questionnaire_id: Optional[str] = None
    serial_number: Optional[str] = None
    dial_date: str = ""
    dial_time: str = ""
    end_time: str = ""
    seconds_interview: Optional[int] = None
    call_number: Optional[int] = None
    dial_number: Optional[int] = None
    interviewer: Optional[str] = None
    dial_result: Optional[int] = None
    dial_line_number: Optional[int] = None
    appointment_type: str = ""
    outcome_code: Union[int, str] = ""

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
class CatiMiHubCallHistoryTable(DatabaseBase):
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

    @staticmethod
    def get_outcome_code():
        return """ExtractValue(`AdditionalData`, '/Fields/Field[@Name="QHAdmin.HOut"]/@Value') AS OutcomeCode"""

    @classmethod
    def table_name(cls):
        return "cati.DialHistory"

    @classmethod
    def extra_fields(cls):
        return [
            "ABS(TIME_TO_SEC(TIMEDIFF(EndTime, StartTime))) as dial_secs",
            cls.get_outcome_code(),
        ]
