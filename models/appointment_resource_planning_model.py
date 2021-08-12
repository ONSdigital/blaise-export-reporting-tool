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
            SELECT DaybatchCaseInfo.InstrumentId, TIME_FORMAT(AppointmentStartTime, "%H:%i") AS AppointmentTime,
            CASE
                WHEN GroupName = "TNS"
                    OR SelectFields LIKE '%<Field FieldName="QDataBag.IntGroup">TNS</Field>%'
                    OR AdditionalData LIKE '%<Field Name="QDataBag.IntGroup" Status="Response" Value="\\'TNS\\'"/>%' THEN "Other"
                WHEN GroupName = "WLS"
                    OR SelectFields LIKE '%<Field FieldName="QDataBag.IntGroup">WLS</Field>%'
                    OR AdditionalData LIKE '%<Field Name="QDataBag.IntGroup" Status="Response" Value="\\'WLS\\'"/>%' THEN "Welsh"
            ELSE "English"
            END AS AppointmentLanguage,
            COUNT(*) AS Total
            FROM {cls.table_name()}
            LEFT JOIN DialHistory ON DialHistory.InstrumentId = DaybatchCaseInfo.InstrumentId
                AND DialHistory.PrimaryKeyValue = DaybatchCaseInfo.PrimaryKeyValue
            WHERE AppointmentType != "0"
                AND AppointmentStartDate LIKE "{date}%"
            GROUP BY InstrumentId, AppointmentTime, AppointmentLanguage
            ORDER BY AppointmentTime ASC, AppointmentLanguage ASC
        """
        return cls.query(config, query)

    @classmethod
    def table_name(cls):
        return "cati.DaybatchCaseInfo"
