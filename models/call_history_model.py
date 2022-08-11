import datetime
from dataclasses import dataclass
from typing import Optional

from models.database_base_model import DataBaseBase


@dataclass
class CallHistory:
    questionnaire_id: str
    serial_number: int
    call_number: str
    dial_number: str
    busy_dials: int
    call_start_time: datetime.datetime
    call_end_time: datetime.datetime
    dial_secs: int
    status: str
    interviewer: str
    call_result: str
    update_info: str
    appointment_info: str
    questionnaire_name: str = ""
    survey: str = ""
    wave: Optional[int] = None
    cohort: Optional[str] = None
    number_of_interviews: Optional[int] = None
    outcome_code: Optional[int] = None

    def generate_questionnaire_details(self, questionnaire_name):
        self.questionnaire_name = questionnaire_name
        self.survey = questionnaire_name[0:3]
        if self.survey == "LMS" and questionnaire_name[-1:].isnumeric():
            self.wave = int(questionnaire_name[-1:])
            self.cohort = questionnaire_name[-3:-1]


@dataclass
class CatiCallHistoryTable(DataBaseBase):
    InstrumentId: str
    PrimaryKeyValue: str
    CallNumber: int
    DialNumber: int
    BusyDials: int
    StartTime: datetime.datetime
    EndTime: datetime.datetime
    Status: str
    Interviewer: str
    DialResult: str
    UpdateInfo: str
    AppointmentInfo: str

    @staticmethod
    def get_outcome_code():
        return """ExtractValue(`AdditionalData`, '/Fields/Field[@Name="QHAdmin.HOut"]/@Value') AS OutcomeCode"""

    @staticmethod
    def dial_secs():
        return "ABS(TIME_TO_SEC(TIMEDIFF(EndTime, StartTime))) as DialSecs"

    @classmethod
    def table_name(cls):
        return "cati.DialHistory"

    @classmethod
    def extra_fields(cls):
        return [cls.dial_secs(), cls.get_outcome_code()]
