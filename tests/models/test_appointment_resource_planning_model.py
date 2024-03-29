from models.appointment_resource_planning_model import (
    AppointmentResourcePlanning,
    CatiAppointmentResourcePlanningTable,
)


def test_appointment_resource_planning():
    appointment_resource_planning = AppointmentResourcePlanning(
        questionnaire_name="",
        appointment_time="",
        appointment_language="",
        case_reference="",
        respondent_name="",
        telephone_number="",
    )
    assert appointment_resource_planning is not None


def test_cati_appointment_resource_planning_table_fields():
    fields = CatiAppointmentResourcePlanningTable.fields()
    assert fields == ", ".join(
        [
            "InstrumentId",
            "AppointmentStartDate",
            "AppointmentStartTime",
            "GroupName",
            "DialResult",
            "AppointmentType",
        ]
    )


def test_cati_appointment_resource_planning_table_table_name():
    assert CatiAppointmentResourcePlanningTable.table_name() == "cati.DaybatchCaseInfo"
