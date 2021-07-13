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
    cati_call_history_and_instrument_data_merged = merge_cati_call_history_and_instrument_data(cati_call_history,
                                                                                               instrument_data)
    print(f"Merged cati call history and instrument data")
    return cati_call_history_and_instrument_data_merged


def get_matching_case(serial_number, questionnaire_name, case_list_to_query):
    case_data = [
        case
        for case in case_list_to_query
        if case.get("qiD.Serial_Number") == serial_number
           and case["questionnaire_name"] == questionnaire_name
    ]
    if len(case_data) != 1:
        return None
    return case_data[0]


def merge_cati_call_history_and_instrument_data(case_history_data, cases):
    for case in case_history_data:
        case_data = get_matching_case(
            case.serial_number, case.questionnaire_name, cases
        )
        if case_data is not None:
            # Get additional field data from the case and append to dial
            case.number_of_interviews = case_data.get("qHousehold.QHHold.HHSize", "")
            case.outcome_code = case_data.get("qhAdmin.HOut", "")
    return case_history_data
