from dataclasses import dataclass, fields

from models.database_base_model import DataBaseBase


@dataclass
class AppointmentResourcePlanning:
    questionnaire_name: str = ""
    appointment_time: str = ""
    appointment_language: str = ""
    total: int = None

    @classmethod
    def fields(cls):
        return [field.name for field in fields(cls)]


@dataclass
class CatiAppointmentResourcePlanningTable(DataBaseBase):
    InstrumentId: str
    AppointmentStartDate: str
    AppointmentStartTime: str
    GroupName: str
    AppointmentType: int

    @classmethod
    def get_appointments_for_date(cls, config, date):
        query = f"""
        SELECT InstrumentId, TIME_FORMAT(AppointmentStartTime, "%H:%i") AS AppointmentTime,
        CASE WHEN GroupName = "" THEN "English" WHEN GroupName = "TNS" THEN "Other" WHEN GroupName = "WLS" THEN "Welsh" END AS AppointmentLanguage,
        COUNT(*) AS Total
        FROM {cls.table_name()}
        WHERE AppointmentType != "0"
        AND AppointmentStartDate LIKE "{date}%"
        GROUP BY InstrumentId, AppointmentTime, GroupName
        ORDER BY AppointmentTime ASC, AppointmentLanguage ASC
        """
        return cls.query(config, query)

    @classmethod
    def table_name(cls):
        return "cati.DaybatchCaseInfo"
