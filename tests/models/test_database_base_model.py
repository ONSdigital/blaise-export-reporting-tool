from dataclasses import dataclass
from unittest.mock import patch

from models.database_base_model import DataBaseBase


@dataclass
class ExampleTableModel(DataBaseBase):
    field_name_1: str
    field_name_2: int
    field_name_3: bool

    @classmethod
    def table_name(cls):
        return "foo.bar"


@patch("models.database_base_model.DataBaseBase.query")
def test_database_base_model_select_from_calls_query_with_the_correct_parameters(
    query, config
):
    # arrange & act
    ExampleTableModel.select_from(config)

    # assert
    query.assert_called_with(
        config, "SELECT field_name_1, field_name_2, field_name_3 FROM foo.bar"
    )


@patch("models.database_base_model.DataBaseBase.query")
def test_database_base_model_select_from_returns_the_expected_query_result(
    query, config
):
    # arrange
    query.return_value = [
        ("row_1_value_for_field_name_1", 1, False),
        ("row_2_value_for_field_name_1", 2, True),
    ]

    # act
    result = ExampleTableModel.select_from(config)

    # assert
    assert result == [
        ("row_1_value_for_field_name_1", 1, False),
        ("row_2_value_for_field_name_1", 2, True),
    ]
