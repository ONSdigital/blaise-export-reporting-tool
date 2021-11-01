from models.appointment_resource_planning_language_summary_model import AppointmentResourcePlanningLanguageSummary, CatiAppointmentResourcePlanningSummaryLanguageTable


def test_appointment_resource_planning_language_summary():
    assert AppointmentResourcePlanningLanguageSummary(
        language="",
        total=0
    ) is not None


def test_cati_appointment_resource_planning_table_fields():
    fields = CatiAppointmentResourcePlanningSummaryLanguageTable.fields()
    assert fields == ", ".join(
        [
            "AppointmentStartDate",
            "GroupName",
            "DialResult",
            "AppointmentType"
        ]
    )


def test_cati_appointment_resource_planning_table_table_name():
    assert CatiAppointmentResourcePlanningSummaryLanguageTable.table_name() == "cati.DaybatchCaseInfo"
