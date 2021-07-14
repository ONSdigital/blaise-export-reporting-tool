from data_sources.blaise_api import get_list_of_installed_instruments, get_instrument_data
from extract_call_history import get_cati_call_history


def get_call_history(config):
    installed_instrument_list = get_list_of_installed_instruments(config)
    cati_call_history = get_cati_call_history(config, installed_instrument_list)
    instrument_fields_to_get = [
        "QID.Serial_Number",
        "QHAdmin.HOut",
        "QHousehold.QHHold.HHSize",
    ]
    instrument_data = []
    for instrument in installed_instrument_list:
        instrument_data.extend(get_instrument_data(instrument.get("name"), config, instrument_fields_to_get))
    print(f"Found {len(instrument_data)} instrument records")
    cati_call_history_and_instrument_data_merged = merge_cati_call_history_and_instrument_data(
        cati_call_history, instrument_data)
    print(f"Merged cati call history and instrument data")
    return cati_call_history_and_instrument_data_merged


def match_cati_call_history_and_instrument_data(serial_number, instrument_name, instrument_data):
    matched_record = [
        record
        for record in instrument_data
        if record.get("qiD.Serial_Number") == serial_number and record["questionnaire_name"] == instrument_name
    ]
    if len(matched_record) > 0:
        return matched_record[0]
    else:
        return None


def merge_cati_call_history_and_instrument_data(cati_call_history, instrument_data):
    for record in cati_call_history:
        matched_record = match_cati_call_history_and_instrument_data(
            record.serial_number, record.questionnaire_name, instrument_data)
        if matched_record is not None:
            record.number_of_interviews = matched_record.get("qHousehold.QHHold.HHSize", "")
            record.outcome_code = matched_record.get("qhAdmin.HOut", "")
    return cati_call_history
