from unittest import mock

from data_sources.cati_data import get_cati_mi_hub_call_history_from_database
from data_sources.call_history_data import CallHistoryClient
from data_sources.questionnaire_data import (
    get_list_of_installed_questionnaires,
    get_questionnaire_data,
    get_questionnaire_name_from_id,
)
from models.mi_hub_call_history_model import MiHubCallHistory


def get_mi_hub_call_history(config):
    print("Getting data for the MI hub call history report")
    installed_questionnaire_list = get_list_of_installed_questionnaires(config)
    mi_hub_cati_call_history = get_cati_mi_hub_call_history(
        config, installed_questionnaire_list
    )
    return group_by_questionnaire(mi_hub_cati_call_history)


def get_cati_mi_hub_call_history(config, questionnaire_list):
    results = get_cati_mi_hub_call_history_from_database(config)
    cati_mi_hub_call_history_list = []
    for item in results:
        if not check_if_questionnaire_id_is_in_questionnaire_list(
            item.get("InstrumentId"), questionnaire_list
        ):
            continue
        cati_mi_hub_call_history = MiHubCallHistory(
            questionnaire_id=item.get("InstrumentId"),
            serial_number=item.get("PrimaryKeyValue"),
            call_number=item.get("CallNumber"),
            dial_number=item.get("DialNumber"),
            interviewer=item.get("Interviewer"),
            dial_result=item.get("DialResult"),
            dial_line_number=item.get("DialedNumber"),
            seconds_interview=item.get("dial_secs"),
            outcome_code=item.get("OutcomeCode"),
        )
        cati_mi_hub_call_history.generate_dial_date_and_time_fields(
            item.get("StartTime"), item.get("EndTime")
        )
        questionnaire_name = get_questionnaire_name_from_id(
            cati_mi_hub_call_history.questionnaire_id, questionnaire_list
        )
        if questionnaire_name != "":
            cati_mi_hub_call_history.questionnaire_name = questionnaire_name
        cati_mi_hub_call_history_list.append(cati_mi_hub_call_history)
    print(f"Found {len(results)} mi hub call history records in the CATI database")
    return cati_mi_hub_call_history_list


def check_if_questionnaire_id_is_in_questionnaire_list(
    instrument_id, questionnaire_list
):
    for questionnaire in questionnaire_list:
        if instrument_id == questionnaire.get("id"):
            return True
    return False


def group_by_questionnaire(data):
    result = {}
    for record in data:
        result[record.questionnaire_name] = result.get(record.questionnaire_name, [])
        result[record.questionnaire_name].append(record)
    return result
