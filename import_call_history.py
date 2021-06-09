from data_sources.blaise_api import get_questionnaire_list, load_case_data
from extract_call_history import load_cati_dial_history


def append_case_data_to_dials(case_history_data, cases):
    for case in case_history_data:
        case_data = get_matching_case(
            case.serial_number, case.questionnaire_name, cases
        )
        if case_data is not None:
            # Get additional field data from the case and append to dial
            case.number_of_interviews = case_data.get("qHousehold.QHHold.HHSize", "")
            case.outcome_code = case_data.get("qhAdmin.HOut", "")
    return case_history_data


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


def import_call_history_data(config):
    questionnaire_list = get_questionnaire_list(config)

    case_history_data = load_cati_dial_history(config, questionnaire_list)
    print(f"Read {len(case_history_data)} case history data")

    blaise_fields_to_get = [
        "QID.Serial_Number",
        "QHAdmin.HOut",
        "QHousehold.QHHold.HHSize",
    ]

    cases = []
    for questionnaire in questionnaire_list:
        cases.extend(
            load_case_data(questionnaire.get("name"), config, blaise_fields_to_get)
        )
    print(f"Read {len(cases)} cases")

    merged_call_history = append_case_data_to_dials(case_history_data, cases)
    print(f"Merged case history data with case data")

    return merged_call_history
