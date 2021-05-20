import timeit

import requests

from cati_reader import get_call_history
from models.call_history import CallHistory
from models.config import Config

global questionnaire_list


def get_questionnaire_list(config):
    print("Get Questionnaire list")
    response = requests.get(
        f"{config.blaise_api_url}/api/v1/serverparks/gusty/instruments"
    )
    global questionnaire_list
    questionnaire_list = response.json()
    print(f"{len(questionnaire_list)} questionnaires installed")


def get_questionnaire_name_from_id(questionnaire_id):
    return next(
        (item for item in questionnaire_list if item.get("id") == questionnaire_id),
        None,
    ).get("name")


def load_case_data(questionnaire_name, config):
    fields_to_get = [
        ("fieldIds", "QID.Serial_Number"),
        ("fieldIds", "QHAdmin.HOut"),
        ("fieldIds", "QHousehold.QHHold.HHSize"),
    ]

    print(f"Get reporting data for questionnaire {questionnaire_name}")

    try:
        response = requests.get(
            f"{config.blaise_api_url}/api/v1/serverparks/gusty/instruments/{questionnaire_name}/report",
            params=fields_to_get,
        )
        data = response.json()
        reporting_data = data.get("reportingData")
        if len(reporting_data) == 0:
            return []

        for case in reporting_data:
            case["questionnaire_name"] = questionnaire_name

        return reporting_data
    except ConnectionResetError:
        print("connection error :( ")
        return []


def load_cati_dial_history(config):
    results = get_call_history(config)

    call_history_list = []

    for item in results:
        call_history = CallHistory(*item)
        questionnaire_name = get_questionnaire_name_from_id(
            call_history.questionnaire_id
        )
        call_history.generate_questionnaire_details(questionnaire_name)

        call_history_list.append(call_history)

    print(
        f"{len(results)} calls in CATI DB - {len(call_history_list)} calls appended to list"
    )
    return call_history_list


def append_case_data_to_dials(case_history_data, cases):
    for case in case_history_data:
        case_data = get_matching_case(
            case.serial_number, case.questionnaire_name, cases
        )
        if case_data is not None:
            # Get additional field data from the case and append to dial
            case.number_of_interviews = case_data.get("qHousehold.QHHold.HHSize")
            case.outcome_code = case_data.get("qhAdmin.HOut")
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
    get_questionnaire_list(config)

    case_history_data = load_cati_dial_history(config)
    print(f"Read {len(case_history_data)} case history data")

    cases = []
    for questionnaire in questionnaire_list:
        cases.extend(load_case_data(questionnaire.get("name"), config))
    print(f"Read {len(cases)} cases")

    merged_call_history = append_case_data_to_dials(case_history_data, cases)
    print(f"Merged case history data with case data")

    return merged_call_history

# For testing the data extraction locally
# if __name__ == "__main__":
#     config = Config.from_env()
#     config.log()
#     import_call_history_data(config)
