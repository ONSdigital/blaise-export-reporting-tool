from dataclasses import dataclass, fields

from models.database_base_model import DataBaseBase


@dataclass
class AppointmentResourcePlanningLanguageSummary:
    english: str = ""
    welsh: str = ""
    other: str = ""

    @classmethod
    def fields(cls):
        return [field.name for field in fields(cls)]


@dataclass
class CatiAppointmentResourcePlanningSummaryLanguageTable(DataBaseBase):
    AppointmentStartDate: str
    GroupName: str
    DialResult: str
    AppointmentType: int

    @classmethod
    def get_language_summary_for_date(cls, config, date):
        query = f"""
            with UniqueDialHistoryIdTable as
                (SELECT
                    max(Id) as id,
                    PrimaryKeyValue
                FROM
                    DialHistory
                group by
                    PrimaryKeyValue)

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
                    "{cls.table_name}%" AS dbci
                left join DialHistory dh
                    ON dh.PrimaryKeyValue = dbci.PrimaryKeyValue
                    AND dh.DialResult = "Appointment"
                inner join UniqueDialHistoryIdTable uid
                    on dh.id = uid.id
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
