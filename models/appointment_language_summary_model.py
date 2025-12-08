from dataclasses import dataclass, fields

from models.database_base_model import DatabaseBase


@dataclass
class AppointmentLanguageSummary:
    language: str = ""
    total: int = 0

    @classmethod
    def fields(cls):
        return [field.name for field in fields(cls)]


@dataclass
class CatiAppointmentLanguageSummaryTable(DatabaseBase):
    AppointmentStartDate: str
    GroupName: str
    DialResult: str
    AppointmentType: int

    @classmethod
    def get_language_summary_for_date(cls, config, date, survey_tla, questionnaires):
        params = []
        if questionnaires is None or len(questionnaires) == 0:
            questionnaire_filter = "cf.InstrumentName LIKE %s"
            params.append(f"{survey_tla or ''}%")
        else:
            placeholders = ", ".join(["%s"] * len(questionnaires))
            questionnaire_filter = f"cf.InstrumentName IN ({placeholders})"
            params.extend(questionnaires)

        query = f"""
            WITH UniqueDialHistoryIdTable AS
                (SELECT
                    MAX(Id) AS id,
                    dh.PrimaryKeyValue
                FROM
                    DialHistory dh
                INNER JOIN
                    configuration.Configuration cf
                ON dh.InstrumentId = cf.InstrumentId
                WHERE {questionnaire_filter}
                GROUP BY
                    dh.PrimaryKeyValue, dh.InstrumentId)

                SELECT
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
                    ON dh.PrimaryKeyValue = dbci.PrimaryKeyValue
                    AND dh.InstrumentId = dbci.InstrumentId
                    AND dh.DialResult = "Appointment"
                INNER JOIN UniqueDialHistoryIdTable uid
                    ON dh.id = uid.id
                WHERE
                    dbci.AppointmentType != "0"
                    AND dbci.AppointmentStartDate LIKE %s
                GROUP BY
                   AppointmentLanguage;

        """

        # Last parameter is the date LIKE condition
        params.append(f"{date}%")

        return cls.query(config, query, params)

    @classmethod
    def table_name(cls):
        return "cati.DaybatchCaseInfo"
