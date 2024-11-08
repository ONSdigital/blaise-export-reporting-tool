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



@pytest.fixture
def mock_mi_hub_respondent_data_no_missing_data():
    return [
        MiHubRespondentData(
            serial_number="900001",
            outcome_code="310",
            date_completed="2-11-2022_9:20",
            interviewer="testuser",
            mode="default",
            postcode="PO57 2OD",
            gender="Male",
            date_of_birth="21-10-2022",
            age="18",
        ),
    ]


def test_deliver_mi_hub_reports_with_no_missing_data_returns_expected_string(
    mock_mi_hub_call_history, mock_mi_hub_respondent_data_no_missing_data
):
    # arrange
    mock_google_storage = create_autospec(GoogleStorage)

    # act
    result = DeliverMiHubReportsService.upload_mi_hub_reports_to_gcp(
        questionnaire_name=QUESTIONNAIRE_NAME,
        mi_hub_call_history=mock_mi_hub_call_history,
        mi_hub_respondent_data=mock_mi_hub_respondent_data_no_missing_data,
        google_storage=mock_google_storage,
    )

    # assert
    assert result == f"Done - {QUESTIONNAIRE_NAME}"


@pytest.fixture
def mock_mi_hub_respondent_data_missing_serial_number_data():
    return [
        MiHubRespondentData(
            serial_number="",
            outcome_code="310",
            date_completed="2-11-2022_9:20",
            interviewer="testuser",
            mode="default",
            postcode="PO57 2OD",
            gender="Male",
            date_of_birth="21-10-2022",
            age="18",
        ),
    ]


def test_deliver_mi_hub_reports_with_missing_serial_number_returns_expected_string(
    mock_mi_hub_call_history, mock_mi_hub_respondent_data_missing_serial_number_data
):
    # arrange
    mock_google_storage = create_autospec(GoogleStorage)

    # act
    result = DeliverMiHubReportsService.upload_mi_hub_reports_to_gcp(
        questionnaire_name=QUESTIONNAIRE_NAME,
        mi_hub_call_history=mock_mi_hub_call_history,
        mi_hub_respondent_data=mock_mi_hub_respondent_data_missing_serial_number_data,
        google_storage=mock_google_storage,
    )

    # assert
    assert result == f"Done - {QUESTIONNAIRE_NAME}"


@pytest.fixture
def mock_mi_hub_respondent_data_missing_all_data():
    return [
        MiHubRespondentData(
            serial_number="",
            outcome_code="",
            date_completed="",
            interviewer="",
            mode="",
            postcode="",
            gender="",
            date_of_birth="",
            age="",
        ),
    ]


def test_deliver_mi_hub_reports_with_missing_all_data_returns_expected_string(
    mock_mi_hub_call_history, mock_mi_hub_respondent_data_missing_all_data
):
    # arrange
    mock_google_storage = create_autospec(GoogleStorage)

    # act
    result = DeliverMiHubReportsService.upload_mi_hub_reports_to_gcp(
        questionnaire_name=QUESTIONNAIRE_NAME,
        mi_hub_call_history=mock_mi_hub_call_history,
        mi_hub_respondent_data=mock_mi_hub_respondent_data_missing_all_data,
        google_storage=mock_google_storage,
    )

    # assert
    assert result == f"Done - {QUESTIONNAIRE_NAME}"


def test_deliver_mi_hub_reports_with_no_data_returns_expected_string(
    mock_mi_hub_call_history, mock_mi_hub_respondent_data_missing_all_data
):
    # arrange
    mock_google_storage = create_autospec(GoogleStorage)

    # act
    result = DeliverMiHubReportsService.upload_mi_hub_reports_to_gcp(
        questionnaire_name=QUESTIONNAIRE_NAME,
        mi_hub_call_history=mock_mi_hub_call_history,
        mi_hub_respondent_data=None,
        google_storage=mock_google_storage,
    )

    # assert
    assert result == f"Done - {QUESTIONNAIRE_NAME}"
