from data_sources.cati_data import get_cati_mi_hub_call_history_from_database
from data_sources.datastore_data import merge_cati_call_history_and_questionnaire_data
from data_sources.questionnaire_data import get_list_of_installed_questionnaires, get_questionnaire_data, \
    get_questionnaire_name_from_id
from functions.csv_functions import write_list_of_dicts_to_csv
from functions.folder_functions import create_questionnaire_name_folder_in_tmp_directory, get_tmp_directory_path
from models.mi_hub_call_history_model import MiHubCallHistory


def get_mi_hub_call_history(config):
    print("Getting data for the MI hub call history report")
    installed_questionnaire_list = get_list_of_installed_questionnaires(config)
    mi_hub_cati_call_history = get_cati_mi_hub_call_history(config, installed_questionnaire_list)
    questionnaire_fields_to_get = [
        "QID.Serial_Number",
        "QHAdmin.HOut",
    ]
    questionnaire_data = []
    for questionnaire in installed_questionnaire_list:
        questionnaire_data.extend(
            get_questionnaire_data(questionnaire.get("name"), config, questionnaire_fields_to_get))
    print(f"Found {len(questionnaire_data)} questionnaire records")
    mi_hub_cati_call_history_and_questionnaire_data_merged = merge_cati_call_history_and_questionnaire_data(
        mi_hub_cati_call_history, questionnaire_data)
    mi_hub_call_history_grouped = group_by_questionnaire(mi_hub_cati_call_history_and_questionnaire_data_merged)
    tmp_folder = get_tmp_directory_path()
    for questionnaire_name in mi_hub_call_history_grouped.keys():
        create_questionnaire_name_folder_in_tmp_directory(questionnaire_name)
        csv_file = f"{tmp_folder}/{questionnaire_name}/call_history.csv"
        write_list_of_dicts_to_csv(csv_file, mi_hub_call_history_grouped[questionnaire_name], MiHubCallHistory.fields())


def get_cati_mi_hub_call_history(config, questionnaire_list):
    results = get_cati_mi_hub_call_history_from_database(config)
    cati_mi_hub_call_history_list = []
    for item in results:
        cati_mi_hub_call_history = MiHubCallHistory(
            questionnaire_id=item.get("InstrumentId"),
            serial_number=item.get("PrimaryKeyValue"),
            call_number=item.get("CallNumber"),
            dial_number=item.get("DialNumber"),
            interviewer=item.get("Interviewer"),
            dial_result=item.get("DialResult"),
            dial_line_number=item.get("DialedNumber"),
            seconds_interview=item.get("dial_secs")
        )
        cati_mi_hub_call_history.generate_dial_date_and_time_fields(item.get("StartTime"), item.get("EndTime"))
        questionnaire_name = get_questionnaire_name_from_id(cati_mi_hub_call_history.questionnaire_id,
                                                            questionnaire_list)
        if questionnaire_name != "":
            cati_mi_hub_call_history.questionnaire_name = questionnaire_name
        cati_mi_hub_call_history_list.append(cati_mi_hub_call_history)
    print(f"Found {len(results)} mi hub call history records in the CATI database")
    return cati_mi_hub_call_history_list


def group_by_questionnaire(data):
    result = {}
    for record in data:
        result[record.questionnaire_name] = result.get(record.questionnaire_name, [])
        result[record.questionnaire_name].append(record)
    return result
