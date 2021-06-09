from models.mi_call_history import CatiMiCallHistoryTable
from models.call_history import CatiCallHistoryTable
from models.events import CatiEventsTable


def get_call_history(config):
    return CatiCallHistoryTable.select_from(config)


def get_mi_call_history(config):
    return CatiMiCallHistoryTable.select_from(config)


def get_events(config):
    return CatiEventsTable.select_from(config)
