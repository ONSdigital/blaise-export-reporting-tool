from models.mi_hub_call_history import CatiMiHubCallHistoryTable
from models.call_history import CatiCallHistoryTable
from models.events import CatiEventsTable


def get_call_history(config):
    return CatiCallHistoryTable.select_from(config)


def get_mi_hub_call_history(config):
    return CatiMiHubCallHistoryTable.select_from(config)


def get_events(config):
    return CatiEventsTable.select_from(config)
