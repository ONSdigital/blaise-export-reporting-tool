import datetime
from dataclasses import dataclass, fields

from models.database_base_model import DataBaseBase


@dataclass
class AppointmentResourcePlanning:
    questionnaire_name: str = ""
    questionnaire_id: str = ""
    appointment_time: str = ""
    appointment_language: str = ""
    total: int = None

    @classmethod
    def fields(cls):
        return [field.name for field in fields(cls)]


@dataclass
class CatiAppointmentResourcePlanningTable(DataBaseBase):
    InstrumentId: str
    AppointmentStartTime: datetime
    GroupName: str

    @classmethod
    def table_name(cls):
        return "cati.DaybatchCaseInfo"
