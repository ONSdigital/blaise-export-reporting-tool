import datetime
from dataclasses import dataclass

from models.database_base_model import DataBaseBase


@dataclass
class CallHistory:
    questionnaire_id: str
    serial_number: int
    call_number: str
    dial_number: str
    busy_dials: int
    call_start_time: datetime
    call_end_time: datetime
    dial_secs: int
    status: str
    interviewer: str
    call_result: str
    update_info: str
    appointment_info: str
    questionnaire_name: str = ""
    survey: str = ""
    wave: int = None
    cohort: str = None
    number_of_interviews: int = None
    outcome_code: int = None

    def generate_questionnaire_details(self, questionnaire_name):
        self.questionnaire_name = questionnaire_name
        self.survey = questionnaire_name[0:3]
        if self.survey == "LMS":
            self.wave: int = int(questionnaire_name[len(questionnaire_name) - 1:])
            self.cohort: str = questionnaire_name[len(questionnaire_name) - 3: -1]


@dataclass
class CatiCallHistoryTable(DataBaseBase):
    InstrumentId: str
    PrimaryKeyValue: str
    CallNumber: int
    DialNumber: int
    BusyDials: int
    StartTime: datetime
    EndTime: datetime
    Status: str
    Interviewer: str
    DialResult: str
    UpdateInfo: str
    AppointmentInfo: str
    AdditionalData: str

    @classmethod
    def webnudge(cls):
        return """CASE WHEN AdditionalData LIKE '%<Field Name="QHAdmin.HOut" Status="Response" Value="110"%' THEN 110 END AS outcome_code"""

    @classmethod
    def dial_secs(cls):
        return "ABS(TIME_TO_SEC(TIMEDIFF(EndTime, StartTime))) as dial_secs"

    @classmethod
    def table_name(cls):
        return "cati.DialHistory"

    @classmethod
    def extra_fields(cls):
        return [cls.dial_secs(), cls.webnudge()]



