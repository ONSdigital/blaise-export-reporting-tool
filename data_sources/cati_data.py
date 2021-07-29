from models.appointment_resource_planning_model import CatiAppointmentResourcePlanningTable
from models.call_history_model import CatiCallHistoryTable
from models.mi_hub_call_history_model import CatiMiHubCallHistoryTable


def get_cati_call_history_from_database(config):
    return CatiCallHistoryTable.select_from(config)


def get_cati_mi_hub_call_history_from_database(config):
    return CatiMiHubCallHistoryTable.select_from(config)


def get_cati_appointment_resource_planning_from_database(config):
    return CatiAppointmentResourcePlanningTable.select_from(config)
