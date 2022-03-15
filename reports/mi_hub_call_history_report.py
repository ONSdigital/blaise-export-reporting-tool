from unittest import mock

from data_sources.cati_data import get_cati_mi_hub_call_history_from_database
from data_sources.call_history_data import CallHistoryClient
from data_sources.questionnaire_data import (
    get_questionnaire_data,
)
from models.mi_hub_call_history_model import MiHubCallHistory


def get_mi_hub_call_history(config, questionnaire_name, questionnaire_id):
    print("Getting data for the MI hub call history report")
    mi_hub_cati_call_history = get_cati_mi_hub_call_history(config, questionnaire_name, questionnaire_id)

    questionnaire_fields_to_get = [
        "QID.Serial_Number",
        "QHAdmin.HOut",
    ]

    questionnaire_data = []

    questionnaire_data.extend(get_questionnaire_data(
        questionnaire_name, config, questionnaire_fields_to_get)
    )
 
    print(f"Found {len(questionnaire_data)} questionnaire records")
    call_history_client = CallHistoryClient(mock.MagicMock, config)
    mi_hub_cati_call_history_and_questionnaire_data_merged = (
        call_history_client.merge_cati_call_history_and_questionnaire_data(
            mi_hub_cati_call_history, questionnaire_data
        )
    )
    return group_by_questionnaire(
        mi_hub_cati_call_history_and_questionnaire_data_merged
    )


def get_cati_mi_hub_call_history(config, questionnaire_name, questionnaire_id):
    results = get_cati_mi_hub_call_history_from_database(config)
    print(results)
    cati_mi_hub_call_history_list = []
    for item in results:
        if not item.get("InstrumentId") == questionnaire_id:
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
        )
        cati_mi_hub_call_history.generate_dial_date_and_time_fields(
            item.get("StartTime"), item.get("EndTime")
        )
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
