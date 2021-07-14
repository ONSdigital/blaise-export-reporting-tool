from models.call_history import CatiCallHistoryTable
from models.mi_hub_call_history import CatiMiHubCallHistoryTable


def get_cati_call_history_from_database(config):
    return CatiCallHistoryTable.select_from(config)


def get_cati_mi_hub_call_history_from_database(config):
    return CatiMiHubCallHistoryTable.select_from(config)
