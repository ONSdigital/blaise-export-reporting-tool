from dataclasses import dataclass, fields

from models.database_base_model import DataBaseBase


@dataclass
class AppointmentLanguageSummary:
    language: str = ""
    total: int = 0

    @classmethod
    def fields(cls):
        return [field.name for field in fields(cls)]


@dataclass
class CatiAppointmentLanguageSummaryTable(DataBaseBase):
    AppointmentStartDate: str
    GroupName: str
    DialResult: str
    AppointmentType: int

    @classmethod
    def get_language_summary_for_date(cls, config, date, survey_tla):
        query = f"""
            with UniqueDialHistoryIdTable as
                (SELECT
                    max(Id) as id,
                    dh.PrimaryKeyValue
                FROM
                    DialHistory dh
                INNER JOIN
                    configuration.Configuration cf
                ON dh.InstrumentId = cf.InstrumentId
                WHERE cf.InstrumentName LIKE '{str(survey_tla or '')}%'
                GROUP BY
                    dh.PrimaryKeyValue)

                select
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
                from
                    cati.DaybatchCaseInfo AS dbci
                left join DialHistory dh
                    ON dh.PrimaryKeyValue = dbci.PrimaryKeyValue
                    AND dh.DialResult = "Appointment"
                inner join UniqueDialHistoryIdTable uid
                    ON dh.id = uid.id
                WHERE
                   dbci.AppointmentType != "0"
                   AND dbci.AppointmentStartDate LIKE "{date}%"
                GROUP BY
                   AppointmentLanguage;

        """
        return cls.query(config, query)

    @classmethod
    def table_name(cls):
        return "cati.DaybatchCaseInfo"
