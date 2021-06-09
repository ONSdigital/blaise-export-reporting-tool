import requests


def get_questionnaire_list(config):
    print("Get Questionnaire list")
    response = requests.get(
        f"http://{config.blaise_api_url}/api/v1/serverparks/gusty/instruments"
    )
    questionnaire_list = response.json()
    print(f"{len(questionnaire_list)} questionnaires installed")
    return questionnaire_list


def load_case_data(questionnaire_name, config, fields):
    fields_to_get = []
    for field in fields:
        fields_to_get.append(("fieldIds", field))

    print(f"Get reporting data for questionnaire {questionnaire_name}")

    try:
        response = requests.get(
            f"http://{config.blaise_api_url}/api/v1/serverparks/gusty/instruments/{questionnaire_name}/report",
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
