from dataclasses import dataclass

from models.database_base_model import DatabaseBase
from models.error_capture import RowNotFound


@dataclass
class QuestionnaireConfigurationTable(DatabaseBase):
    @classmethod
    def table_name(cls) -> str:
        return "configuration.Configuration"

    @classmethod
    def get_questionnaire_name_from_id(cls, config, questionnaire_id: str) -> str:
        result = cls.query(
            config,
            "SELECT InstrumentName "
            f"FROM {cls.table_name()} "
            f"WHERE InstrumentId = '{questionnaire_id}'",
        )

        if not result:
            raise RowNotFound(
                f"Could not find configuration with InstrumentId: {questionnaire_id}"
            )

        return result[0]["InstrumentName"]
