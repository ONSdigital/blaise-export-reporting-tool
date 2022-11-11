from unittest.mock import create_autospec

import pytest

from functions.google_storage_functions import GoogleStorage
from models.mi_hub_call_history_model import MiHubCallHistory
from models.mi_hub_respondent_data_model import MiHubRespondentData
from services.deliver_mi_hub_reports_service import DeliverMiHubReportsService

QUESTIONNAIRE_NAME = "LMS2222Z"


def test_deliver_mi_hub_reports_returns_expected_string(
    mock_mi_hub_call_history, mock_mi_hub_respondent_data
):
    # arrange
    mock_google_storage = create_autospec(GoogleStorage)

    # act
    result = DeliverMiHubReportsService.upload_mi_hub_reports_to_gcp(
        questionnaire_name=QUESTIONNAIRE_NAME,
        mi_hub_call_history=mock_mi_hub_call_history,
        mi_hub_respondent_data=mock_mi_hub_respondent_data,
        google_storage=mock_google_storage,
    )

    # assert
    assert result == f"Done - {QUESTIONNAIRE_NAME}"
