import requests

from models.error_capture import RowNotFound
from models.questionnaire_configuration_model import QuestionnaireConfigurationTable


def get_list_of_installed_questionnaires(config):
    print("Getting list of installed questionnaires")
    response = requests.get(
        f"http://{config.blaise_api_url}/api/v2/serverparks/gusty/questionnaires"
    )
    questionnaire_list = response.json()
    print(f"Found {len(questionnaire_list)} questionnaires installed")
    return questionnaire_list


def get_questionnaire_name(config, questionnaire_id):
    try:
        return QuestionnaireConfigurationTable.get_questionnaire_name_from_id(config, questionnaire_id)
    except RowNotFound:
        return ""


def get_questionnaire_data(questionnaire_name, config, fields):
    fields_to_get = []
    for field in fields:
        fields_to_get.append(("fieldIds", field))
    print(f"Getting questionnaire data for questionnaire {questionnaire_name}")
    try:
        response = requests.get(
            f"http://{config.blaise_api_url}/api/v2/serverparks/gusty/questionnaires/{questionnaire_name}/report",
            params=fields_to_get,
        )
        if response.status_code != 200:
            return []
        data = response.json()
        reporting_data = data.get("reportingData")
        if len(reporting_data) == 0:
            return []
        for record in reporting_data:
            record["questionnaire_name"] = questionnaire_name
        return reporting_data
    except ConnectionResetError:
        print("Connection error")
        return []
