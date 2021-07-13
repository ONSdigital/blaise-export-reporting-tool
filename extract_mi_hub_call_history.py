from data_sources.blaise_api import get_list_of_installed_instruments, get_instrument_data
from extract_call_history import load_mi_hub_cati_dial_history
from import_call_history import merge_cati_call_history_and_instrument_data
from models.mi_hub_call_history import MiHubCallHistory
from storage_and_files.folder_management import create_instrument_name_folder_in_tmp_directory, get_tmp_directory_path
from storage_and_files.write_csv import write_list_of_dict_to_csv


def extract_mi_hub_call_history(config):
    questionnaire_list = get_list_of_installed_instruments(config)

    case_history_data = load_mi_hub_cati_dial_history(config, questionnaire_list)
    print(f"Read {len(case_history_data)} case history data")

    blaise_fields_to_get = [
        "QID.Serial_Number",
        "QHAdmin.HOut",
    ]

    cases = []
    for questionnaire in questionnaire_list:
        cases.extend(
            get_instrument_data(questionnaire.get("name"), config, blaise_fields_to_get)
        )
    print(f"Read {len(cases)} cases")

    merged_call_history = merge_cati_call_history_and_instrument_data(case_history_data, cases)

    grouped_call_history = group_by_questionnaire(merged_call_history)

    tmp_folder = get_tmp_directory_path()

    for questionnaire_name in grouped_call_history.keys():
        create_instrument_name_folder_in_tmp_directory(questionnaire_name)

        csv_file = f"{tmp_folder}/{questionnaire_name}/call_history.csv"
        write_list_of_dict_to_csv(csv_file, grouped_call_history[questionnaire_name], MiHubCallHistory.fields())

    return grouped_call_history


def group_by_questionnaire(case_history_data):
    res = {}
    for dial in case_history_data:
        res[dial.questionnaire_name] = res.get(dial.questionnaire_name, [])
        res[dial.questionnaire_name].append(dial)
    return res
