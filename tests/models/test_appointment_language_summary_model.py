from unittest.mock import patch

from data_sources.cati_data import get_cati_appointment_language_summary_from_database
from models.appointment_language_summary_model import (
    AppointmentLanguageSummary,
    CatiAppointmentLanguageSummaryTable,
)
from models.config_model import Config


def test_appointment_language_summary():
    assert AppointmentLanguageSummary(language="", total=0) is not None


def test_cati_appointment_language_summary_table_fields():
    fields = CatiAppointmentLanguageSummaryTable.fields()
    assert fields == ", ".join(
        ["AppointmentStartDate", "GroupName", "DialResult", "AppointmentType"]
    )


def test_cati_appointment_language_summary_table_table_name():
    assert CatiAppointmentLanguageSummaryTable.table_name() == "cati.DaybatchCaseInfo"


@patch.object(CatiAppointmentLanguageSummaryTable, "query")
def test_mysql_with_questionnaires(mock_query):
    config = Config.from_env()
    date = "1990-06-30"
    survey_tla = "DST"
    questionnaires = ["DST2111Z", "DST2106Z"]

    get_cati_appointment_language_summary_from_database(
        config, date, survey_tla, questionnaires
    )
    mock_query.assert_called_with(
        config,
        "\n"
        + "            with UniqueDialHistoryIdTable as\n"
        + "                (SELECT\n"
        + "                    max(Id) as id,\n"
        + "                    dh.PrimaryKeyValue\n"
        + "                FROM\n"
        + "                    DialHistory dh\n"
        + "                INNER JOIN\n"
        + "                    configuration.Configuration cf\n"
        + "                ON dh.InstrumentId = cf.InstrumentId\n"
        + "                WHERE cf.InstrumentName IN('DST2111Z', 'DST2106Z')\n"
        + "                GROUP BY\n"
        + "                    dh.PrimaryKeyValue, dh.InstrumentId)\n"
        + "\n"
        + "                select\n"
        + "                    CASE\n"
        + "                       WHEN\n"
        + '                          dbci.GroupName = "TNS"\n'
        + "                          OR dbci.SelectFields LIKE '%<Field FieldName=\"QDataBag.IntGroup\">TNS</Field>%'\n"
        + '                          OR dh.AdditionalData LIKE \'%<Field Name="QDataBag.IntGroup" Status="Response" Value="\\\'TNS\\\'"/>%\'\n'
        + "                       THEN\n"
        + '                          "Other"\n'
        + "                       WHEN\n"
        + '                          dbci.GroupName = "WLS"\n'
        + "                          OR dbci.SelectFields LIKE '%<Field FieldName=\"QDataBag.IntGroup\">WLS</Field>%'\n"
        + '                          OR dh.AdditionalData LIKE \'%<Field Name="QDataBag.IntGroup" Status="Response" Value="\\\'WLS\\\'"/>%\'\n'
        + "                       THEN\n"
        + '                          "Welsh"\n'
        + "                       ELSE\n"
        + '                          "English"\n'
        + "                    END\n"
        + "                    AS AppointmentLanguage,\n"
        + "                    COUNT(*) AS Total\n"
        + "                from\n"
        + "                    cati.DaybatchCaseInfo AS dbci\n"
        + "                left join DialHistory dh\n"
        + "                    ON dh.PrimaryKeyValue = dbci.PrimaryKeyValue\n"
        + "                    AND dh.InstrumentId = dbci.InstrumentId\n"
        + '                    AND dh.DialResult = "Appointment"\n'
        + "                inner join UniqueDialHistoryIdTable uid\n"
        + "                    ON dh.id = uid.id\n"
        + "                WHERE\n"
        + '                   dbci.AppointmentType != "0"\n'
        + '                   AND dbci.AppointmentStartDate LIKE "1990-06-30%"\n'
        + "                GROUP BY\n"
        + "                   AppointmentLanguage;\n"
        + "\n"
        + "        ",
    )


@patch.object(CatiAppointmentLanguageSummaryTable, "query")
def test_mysql_without_questionnaires(mock_query):
    config = Config.from_env()
    date = "1990-06-30"
    survey_tla = "DST"
    questionnaires = None

    get_cati_appointment_language_summary_from_database(
        config, date, survey_tla, questionnaires
    )
    mock_query.assert_called_with(
        config,
        "\n"
        + "            with UniqueDialHistoryIdTable as\n"
        + "                (SELECT\n"
        + "                    max(Id) as id,\n"
        + "                    dh.PrimaryKeyValue\n"
        + "                FROM\n"
        + "                    DialHistory dh\n"
        + "                INNER JOIN\n"
        + "                    configuration.Configuration cf\n"
        + "                ON dh.InstrumentId = cf.InstrumentId\n"
        + "                WHERE cf.InstrumentName LIKE 'DST%'\n"
        + "                GROUP BY\n"
        + "                    dh.PrimaryKeyValue, dh.InstrumentId)\n"
        + "\n"
        + "                select\n"
        + "                    CASE\n"
        + "                       WHEN\n"
        + '                          dbci.GroupName = "TNS"\n'
        + "                          OR dbci.SelectFields LIKE '%<Field FieldName=\"QDataBag.IntGroup\">TNS</Field>%'\n"
        + '                          OR dh.AdditionalData LIKE \'%<Field Name="QDataBag.IntGroup" Status="Response" Value="\\\'TNS\\\'"/>%\'\n'
        + "                       THEN\n"
        + '                          "Other"\n'
        + "                       WHEN\n"
        + '                          dbci.GroupName = "WLS"\n'
        + "                          OR dbci.SelectFields LIKE '%<Field FieldName=\"QDataBag.IntGroup\">WLS</Field>%'\n"
        + '                          OR dh.AdditionalData LIKE \'%<Field Name="QDataBag.IntGroup" Status="Response" Value="\\\'WLS\\\'"/>%\'\n'
        + "                       THEN\n"
        + '                          "Welsh"\n'
        + "                       ELSE\n"
        + '                          "English"\n'
        + "                    END\n"
        + "                    AS AppointmentLanguage,\n"
        + "                    COUNT(*) AS Total\n"
        + "                from\n"
        + "                    cati.DaybatchCaseInfo AS dbci\n"
        + "                left join DialHistory dh\n"
        + "                    ON dh.PrimaryKeyValue = dbci.PrimaryKeyValue\n"
        + "                    AND dh.InstrumentId = dbci.InstrumentId\n"
        + '                    AND dh.DialResult = "Appointment"\n'
        + "                inner join UniqueDialHistoryIdTable uid\n"
        + "                    ON dh.id = uid.id\n"
        + "                WHERE\n"
        + '                   dbci.AppointmentType != "0"\n'
        + '                   AND dbci.AppointmentStartDate LIKE "1990-06-30%"\n'
        + "                GROUP BY\n"
        + "                   AppointmentLanguage;\n"
        + "\n"
        + "        ",
    )
