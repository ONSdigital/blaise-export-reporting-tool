from unittest.mock import patch

from data_sources.questionnaire_data import (
    get_list_of_installed_questionnaires,
    get_questionnaire_data,
    get_questionnaire_name,
)
from models.error_capture import RowNotFound


def test_get_list_of_installed_questionnaires(
    config, requests_mock, api_installed_questionnaires_response
):
    requests_mock.get(
        f"http://{config.blaise_api_url}/api/V2/serverparks/gusty/questionnaires",
        json=api_installed_questionnaires_response,
    )
    questionnaire_list = get_list_of_installed_questionnaires(config)
    assert questionnaire_list == api_installed_questionnaires_response
    assert len(questionnaire_list) == 3


def test_get_questionnaire_data(
    questionnaire_name,
    config,
    questionnaire_fields_to_get,
    requests_mock,
    api_reporting_data_response,
):
    requests_mock.get(
        f"http://{config.blaise_api_url}/api/v2/serverparks/gusty/questionnaires/{questionnaire_name}/report",
        json=api_reporting_data_response,
    )
    reporting_data = get_questionnaire_data(
        questionnaire_name, config, questionnaire_fields_to_get
    )
    assert reporting_data == [
        {
            "qhAdmin.HOut": "110",
            "qiD.Serial_Number": "10010",
            "questionnaire_name": "DST2106Z",
        },
        {
            "qhAdmin.HOut": "110",
            "qiD.Serial_Number": "10020",
            "questionnaire_name": "DST2106Z",
        },
        {
            "qhAdmin.HOut": "110",
            "qiD.Serial_Number": "10030",
            "questionnaire_name": "DST2106Z",
        },
    ]


@patch(
    "models.questionnaire_configuration_model.QuestionnaireConfigurationTable.get_questionnaire_name_from_id"
)
def test_get_questionnaire_name_from_id_is_called_with_the_correct_parameters(
    mock_get_questionnaire_name_from_id, config
):
    # arrange & act
    get_questionnaire_name(config, "12345-12345-12345-12345-ZZZZZ")

    # assert
    mock_get_questionnaire_name_from_id.assert_called_with(
        config, "12345-12345-12345-12345-ZZZZZ"
    )


@patch(
    "models.questionnaire_configuration_model.QuestionnaireConfigurationTable.get_questionnaire_name_from_id"
)
def test_get_questionnaire_name_from_id_returns_questionnaire_name(
    mock_get_questionnaire_name_from_id, config
):
    # arrange
    mock_get_questionnaire_name_from_id.return_value = "DST2106Z"

    # act & assert
    assert get_questionnaire_name(config, "12345-12345-12345-12345-ZZZZZ") == "DST2106Z"


@patch(
    "models.questionnaire_configuration_model.QuestionnaireConfigurationTable.get_questionnaire_name_from_id"
)
def test_get_questionnaire_name_from_id_returns_empty_string_when_questionnaire_id_not_found(
    mock_get_questionnaire_name_from_id, config
):
    # arrange
    mock_get_questionnaire_name_from_id.side_effect = RowNotFound

    # act & assert
    assert get_questionnaire_name(config, "12345-12345-12345-12345-ZZZZZ") == ""
