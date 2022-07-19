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
    DialResult: str
    AppointmentType: int

    @classmethod
    def  get_appointments_for_date(cls, config, date, survey_tla, questionnaires):
        if questionnaires is None or len(questionnaires) == 0:
            questionnaire_filter = f"cf.InstrumentName LIKE '{str(survey_tla or '')}%'"
        else:
            questionnaire_filter = ', '.join("'" + item + "'" for item in questionnaires)
            questionnaire_filter = f"cf.InstrumentName IN({questionnaire_filter})"
        print(f"Questionnaire filter = {questionnaire_filter}")
        
        query = f"""
            with UniqueDialHistoryIdTable as
                (SELECT
                    max(Id) as id,
                    dh.InstrumentId,
                    dh.PrimaryKeyValue
                FROM
                    DialHistory dh
                INNER JOIN
                    configuration.Configuration cf
                ON dh.InstrumentId = cf.InstrumentId
                WHERE {questionnaire_filter}
                GROUP BY
                    dh.PrimaryKeyValue,
                    dh.InstrumentId)
                
            select
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
                COUNT(*) AS Total    
            FROM
                cati.DaybatchCaseInfo AS dbci
            LEFT JOIN DialHistory dh
                ON dh.InstrumentId = dbci.InstrumentId
                AND dh.PrimaryKeyValue = dbci.PrimaryKeyValue
                AND dh.DialResult = "Appointment"    
            INNER JOIN UniqueDialHistoryIdTable uid
                ON dh.id = uid.id
            WHERE
               dbci.AppointmentType != "0"
               AND dbci.AppointmentStartDate LIKE "{date}%"
            GROUP BY
               dbci.InstrumentId,
               AppointmentTime,
               AppointmentLanguage
            ORDER BY
               AppointmentTime ASC,
               AppointmentLanguage ASC       
     
        """
        print(f"Query = {query}")
        return cls.query(config, query)

    @classmethod
    def table_name(cls):
        return "cati.DaybatchCaseInfo"
