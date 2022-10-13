from unittest.mock import patch

import pytest

from models.error_capture import RowNotFound
from models.questionnaire_configuration_model import QuestionnaireConfigurationTable


@patch("models.database_base_model.DataBaseBase.query")
def test_get_questionnaire_name_from_id_executes_as_expected(query, config):
    # arrange
    sql_query = ("SELECT InstrumentName " 
                 "FROM configuration.Configuration " 
                 "WHERE InstrumentId = 'guid_id_example_1'")

    # act
    QuestionnaireConfigurationTable.get_questionnaire_name_from_id(config, "guid_id_example_1")

    # assert
    query.assert_called_with(config, sql_query)


@patch("models.database_base_model.DataBaseBase.query")
def test_get_questionnaire_name_from_id_returns_questionnaire_name_when_found(query, config):
    # arrange
    query.return_value = [("LMS2202A", )]

    # act
    result = QuestionnaireConfigurationTable.get_questionnaire_name_from_id(config, "guid_id_example_1")

    # assert
    assert result == "LMS2202A"


@patch("models.database_base_model.DataBaseBase.query")
def test_get_questionnaire_name_from_id_throws_row_not_found_exception(query, config):
    # arrange
    query.return_value = []

    # act & assert
    with pytest.raises(RowNotFound, match="Could not find configuration with InstrumentId: guid_id_example_1"):
        QuestionnaireConfigurationTable.get_questionnaire_name_from_id(config, "guid_id_example_1")
