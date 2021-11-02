from models.appointment_language_summary_model import AppointmentLanguageSummary, CatiAppointmentLanguageSummaryTable


def test_appointment_language_summary():
    assert AppointmentLanguageSummary(
        language="",
        total=0
    ) is not None


def test_cati_appointment_language_summary_table_fields():
    fields = CatiAppointmentLanguageSummaryTable.fields()
    assert fields == ", ".join(
        [
            "AppointmentStartDate",
            "GroupName",
            "DialResult",
            "AppointmentType"
        ]
    )


def test_cati_appointment_language_summary_table_table_name():
    assert CatiAppointmentLanguageSummaryTable.table_name() == "cati.DaybatchCaseInfo"
