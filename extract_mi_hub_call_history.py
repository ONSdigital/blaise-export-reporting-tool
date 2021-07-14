from data_sources.blaise_api import get_list_of_installed_instruments, get_instrument_data
from extract_call_history import get_cati_mi_hub_call_history
from import_call_history import merge_cati_call_history_and_instrument_data
from models.mi_hub_call_history import MiHubCallHistory
from storage_and_files.folder_management import create_instrument_name_folder_in_tmp_directory, get_tmp_directory_path
from storage_and_files.write_csv import write_list_of_dicts_to_csv


def get_mi_hub_call_history(config):
    installed_instrument_list = get_list_of_installed_instruments(config)
    print("Getting data for the MI hub call history report")
    mi_hub_cati_call_history = get_cati_mi_hub_call_history(config, installed_instrument_list)
    instrument_fields_to_get = [
        "QID.Serial_Number",
        "QHAdmin.HOut",
    ]
    instrument_data = []
    for instrument in installed_instrument_list:
        instrument_data.extend(get_instrument_data(instrument.get("name"), config, instrument_fields_to_get))
    print(f"Found {len(instrument_data)} instrument records")
    mi_hub_cati_call_history_and_instrument_data_merged = merge_cati_call_history_and_instrument_data(
        mi_hub_cati_call_history, instrument_data)
    mi_hub_call_history_grouped = group_by_instrument(mi_hub_cati_call_history_and_instrument_data_merged)
    tmp_folder = get_tmp_directory_path()
    for instrument_name in mi_hub_call_history_grouped.keys():
        create_instrument_name_folder_in_tmp_directory(instrument_name)
        csv_file = f"{tmp_folder}/{instrument_name}/call_history.csv"
        write_list_of_dicts_to_csv(csv_file, mi_hub_call_history_grouped[instrument_name], MiHubCallHistory.fields())


def group_by_instrument(data):
    result = {}
    for record in data:
        result[record.questionnaire_name] = result.get(record.questionnaire_name, [])
        result[record.questionnaire_name].append(record)
    return result
