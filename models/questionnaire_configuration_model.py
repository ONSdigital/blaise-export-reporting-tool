from dataclasses import dataclass

from models.database_base_model import DataBaseBase
from models.error_capture import RowNotFound


@dataclass
class QuestionnaireConfigurationTable(DataBaseBase):
    @classmethod
    def table_name(cls) -> str:
        return "configuration.Configuration"

    @classmethod
    def get_questionnaire_name_from_id(cls, config, questionnaire_id: str) -> str:
        result = cls.query(config, "SELECT InstrumentName "
                                 f"FROM {cls.table_name()} "
                                 f"WHERE InstrumentId = '{questionnaire_id}'")

        if not result:
            raise RowNotFound(f"Could not find configuration with InstrumentId: {questionnaire_id}")

        print(f"El's DEBUG: This function used the database instead of the API and returned: {result} of type {type(result)}")
        return result[0][0]
