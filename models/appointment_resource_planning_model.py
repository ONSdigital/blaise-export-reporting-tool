from dataclasses import dataclass, fields

from models.database_base_model import DataBaseBase


@dataclass
class AppointmentResourcePlanning:
    questionnaire_name: str = ""
    appointment_time: str = ""
    appointment_language: str = ""
    dial_result: str = ""
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
    DialResult: str
    AppointmentType: int

    @classmethod
    def get_appointments_for_date(cls, config, date):
        query = f"""        
            SELECT
               dbci.InstrumentId,
               TIME_FORMAT(dbci.AppointmentStartTime, "%H:%i") AS AppointmentTime,
               CASE
                  WHEN
                     dbci.GroupName = "TNS" 
                     OR dbci.SelectFields LIKE '%<Field FieldName="QDataBag.IntGroup">TNS</Field>%' 
                     OR dh.AdditionalData LIKE '%<Field Name="QDataBag.IntGroup" Status="Response" Value="\\'TNS\\'"/>%' 
                  THEN
                     "Other" 
                  WHEN
                     dbci.GroupName = "WLS" 
                     OR dbci.SelectFields LIKE '%<Field FieldName="QDataBag.IntGroup">WLS</Field>%' 
                     OR dh.AdditionalData LIKE '%<Field Name="QDataBag.IntGroup" Status="Response" Value="\\'WLS\\'"/>%' 
                  THEN
                     "Welsh" 
                  ELSE
                     "English" 
               END
               AS AppointmentLanguage,
               dh.DialResult,
               COUNT(*) AS Total 
            FROM
               {cls.table_name()} AS dbci 
               LEFT JOIN
                  (
                     SELECT
                        InstrumentId,
                        PrimaryKeyValue,
                        AdditionalData,
                        DialResult,
                        Id
                     FROM
                        DialHistory 
                     GROUP BY
                        InstrumentId,
                        PrimaryKeyValue,
                        AdditionalData,
                        DialResult,
                        Id
                  )
                  AS dh 
                  ON dh.InstrumentId = dbci.InstrumentId 
                  AND dh.PrimaryKeyValue = dbci.PrimaryKeyValue
                  AND dh.DialNumber =
                                (
                                SELECT MAX(Id)
                                FROM cati.DialHistory
                                WHERE InstrumentId = dbci.InstrumentId
                                AND PrimaryKeyValue = dbci.PrimaryKeyValue
                                ) 
            WHERE
               dbci.AppointmentType != "0" 
               AND dbci.AppointmentStartDate like "{date}%" 
            GROUP BY
               dbci.InstrumentId,
               AppointmentTime,
               AppointmentLanguage,
               dh.DialResult 
            ORDER BY
               AppointmentTime ASC,
               AppointmentLanguage ASC
        """
        return cls.query(config, query)

    @classmethod
    def table_name(cls):
        return "cati.DaybatchCaseInfo"
