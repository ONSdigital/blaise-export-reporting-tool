from models.appointment_resource_planning_model import (
    CatiAppointmentResourcePlanningTable,
)
from models.appointment_language_summary_model import (
    CatiAppointmentLanguageSummaryTable,
)
from models.call_history_model import CatiCallHistoryTable
from models.mi_hub_call_history_model import CatiMiHubCallHistoryTable


def get_cati_call_history_from_database(config):
    return CatiCallHistoryTable.select_from(config)


def get_cati_mi_hub_call_history_from_database(config):
    return CatiMiHubCallHistoryTable.select_from(config)


def get_cati_appointment_resource_planning_from_database(
    config, date, survey_tla, questionnaires
):
    return CatiAppointmentResourcePlanningTable.get_appointments_for_date(
        config, date, survey_tla, questionnaires
    )


def get_cati_appointment_language_summary_from_database(
    config, date, survey_tla, questionnaires
):
    return CatiAppointmentLanguageSummaryTable.get_language_summary_for_date(
        config, date, survey_tla, questionnaires
    )
