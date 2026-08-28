from json import JSONDecodeError

import requests
from requests import Response
from requests.exceptions import RequestException

from models.error_capture import RowNotFound
from models.questionnaire_configuration_model import QuestionnaireConfigurationTable


class BlaiseAPIException(Exception):
    pass


def _raise_for_invalid_response(response: Response, questionnaire_name: str):
    raise BlaiseAPIException(
        "Blaise questionnaire report request failed for "
        f"{questionnaire_name} with status {response.status_code}. "
        f"Response: '{response.text}'"
    )


def get_list_of_installed_questionnaires(config):
    print("Getting list of installed questionnaires")
    response = requests.get(
        f"http://{config.blaise_api_url}/api/v2/serverparks/gusty/questionnaires"
    )
    try:
        questionnaire_list = response.json()
    except JSONDecodeError:
        raise BlaiseAPIException(
            f"Status = {response.status_code}. Expected JSON, received '{response.text}'"
        )
    print(f"Found {len(questionnaire_list)} questionnaires installed")
    return questionnaire_list


def get_questionnaire_name(config, questionnaire_id):
    try:
        return QuestionnaireConfigurationTable.get_questionnaire_name_from_id(
            config, questionnaire_id
        )
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
            _raise_for_invalid_response(response, questionnaire_name)
        data = response.json()
        reporting_data = data.get("reportingData")
        if len(reporting_data) == 0:
            return []
        for record in reporting_data:
            record["questionnaire_name"] = questionnaire_name
        return reporting_data
    except JSONDecodeError:
        raise BlaiseAPIException(
            "Blaise questionnaire report request returned non-JSON payload for "
            f"{questionnaire_name} with status {response.status_code}. "
            f"Response: '{response.text}'"
        )
    except (ConnectionResetError, RequestException) as err:
        raise BlaiseAPIException(
            "Blaise questionnaire report request failed for "
            f"{questionnaire_name}: {err}"
        )
