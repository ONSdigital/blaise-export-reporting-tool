from data_sources.blaise_api import get_list_of_installed_instruments, get_instrument_data
from data_sources.cati_database import get_cati_mi_hub_call_history_from_database
from functions.call_history_functions import merge_cati_call_history_and_instrument_data, get_instrument_name_from_id
from functions.csv_functions import write_list_of_dicts_to_csv
from functions.folder_functions import create_instrument_name_folder_in_tmp_directory, get_tmp_directory_path
from models.mi_hub_call_history import MiHubCallHistory


def get_mi_hub_call_history(config):
    print("Getting data for the MI hub call history report")
    installed_instrument_list = get_list_of_installed_instruments(config)
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


def get_cati_mi_hub_call_history(config, instrument_list):
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
        instrument_name = get_instrument_name_from_id(cati_mi_hub_call_history.questionnaire_id, instrument_list)
        if instrument_name != "":
            cati_mi_hub_call_history.questionnaire_name = instrument_name
        cati_mi_hub_call_history_list.append(cati_mi_hub_call_history)
    print(f"Found {len(results)} mi hub call history records in the CATI database")
    return cati_mi_hub_call_history_list


def group_by_instrument(data):
    result = {}
    for record in data:
        result[record.questionnaire_name] = result.get(record.questionnaire_name, [])
        result[record.questionnaire_name].append(record)
    return result
