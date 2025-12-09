from dataclasses import dataclass, fields

from models.database_base_model import DatabaseBase


@dataclass
class AppointmentResourcePlanning:
    questionnaire_name: str = ""
    appointment_time: str = ""
    appointment_language: str = ""
    case_reference: str = ""
    respondent_name: str = ""
    telephone_number: str = ""

    @classmethod
    def fields(cls):
        return [field.name for field in fields(cls)]


@dataclass
class CatiAppointmentResourcePlanningTable(DatabaseBase):
    InstrumentId: str
    AppointmentStartDate: str
    AppointmentStartTime: str
    GroupName: str
    DialResult: str
    AppointmentType: int

    @classmethod
    def get_appointments_for_date(cls, config, date, survey_tla, questionnaires):
        params = []
        if questionnaires is None or len(questionnaires) == 0:
            questionnaire_filter = "cf.InstrumentName LIKE %s"
            params.append(f"{str(survey_tla or '')}%")
        else:
            q_placeholders = ", ".join(["%s" for _ in questionnaires])
            questionnaire_filter = f"cf.InstrumentName IN ({q_placeholders})"
            params.extend(questionnaires)

        print(f"Questionnaire filter = {questionnaire_filter}")

        query = f"""
            WITH UniqueDialHistoryIdTable AS
            (SELECT 
                MAX(Id) AS id,
                dh.InstrumentId,
                dh.PrimaryKeyValue
            FROM DialHistory dh
                INNER JOIN configuration.Configuration cf
                   ON dh.InstrumentId = cf.InstrumentId
            WHERE {questionnaire_filter}
            GROUP BY 
                dh.PrimaryKeyValue,
                dh.InstrumentId)
            SELECT 
                dbci.InstrumentId,
                dh.PrimaryKeyValue AS CaseReference,
                ExtractValue(dh.AdditionalData, "//Field[@Name='CATIAppointment.WhoFor']/@Value") AS RespondentName,
                ExtractValue(dh.AdditionalData, "//Field[@Name='CATIAppointment.ClctNum']/@Value") AS TelephoneNumber,
                TIME_FORMAT(dbci.AppointmentStartTime, "%H:%i") AS AppointmentTime,
            CASE
                WHEN
                    dbci.GroupName = "TNS"
                    OR ExtractValue(dbci.SelectFields, "//Field[@FieldName='QDataBag.IntGroup']/text()") = "TNS"
                    OR ExtractValue(dh.AdditionalData, "//Field[Name='QDataBag.IntGroup']/@Value") = "TNS"
                THEN
                    "Other"
                WHEN
                    dbci.GroupName = "WLS"
                    OR ExtractValue(dbci.SelectFields, "//Field[@FieldName='QDataBag.IntGroup']/text()") = "WLS"
                    OR ExtractValue(dh.AdditionalData, "//Field[@Name='QDataBag.IntGroup']/@Value") = "WLS"
                THEN
                    "Welsh"
                ELSE
                    "English"
                END
            AS AppointmentLanguage
            FROM cati.DaybatchCaseInfo AS dbci
                LEFT JOIN DialHistory dh
                    ON dh.InstrumentId = dbci.InstrumentId
                    AND dh.PrimaryKeyValue = dbci.PrimaryKeyValue
                    AND dh.DialResult = "Appointment"
                INNER JOIN UniqueDialHistoryIdTable uid
                    ON dh.id = uid.id
            WHERE dbci.AppointmentType != "0"
            AND dbci.AppointmentStartDate LIKE %s
            ORDER BY
                AppointmentTime ASC,
                AppointmentLanguage ASC       
        """
        params.append(f"{date}%")
        print(f"Query = {query} | Params = {params}")
        return cls.query(config, query, params)

    @classmethod
    def table_name(cls):
        return "cati.DaybatchCaseInfo"
