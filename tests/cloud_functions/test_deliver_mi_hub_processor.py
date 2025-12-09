from dataclasses import dataclass
from typing import Any, Dict
from unittest import mock

import flask
import pytest

from cloud_functions.deliver_mi_hub_reports import (
    deliver_mi_hub_reports_cloud_function_processor,
)

QUESTIONNAIRE_NAME = "LMS2222Z"
QUESTIONNAIRE_NAME_DIA_A = "DIA2506A"
QUESTIONNAIRE_NAME_DIA_B = "DIA2506B"
QUESTIONNAIRE_NAME_CONTACT_INFO = "DIA2506A_ContactInfo"
QUESTIONNAIRE_ID = "s0me-r7nd0m-gu1d"


@dataclass
class FakeGoogleStorage:
    bucket: Any = None
    nifi_staging_bucket: str = ""
    log: str = ""
    storage_client: str = ""


@pytest.fixture
def fake_google_storage():
    return FakeGoogleStorage()


@pytest.fixture
def mock_request_values() -> Dict:
    return {"name": QUESTIONNAIRE_NAME, "id": QUESTIONNAIRE_ID}


@pytest.fixture
def mock_request_values_DIA_A() -> Dict:
    return {"name": QUESTIONNAIRE_NAME_DIA_A, "id": QUESTIONNAIRE_ID}


@pytest.fixture
def mock_request_values_DIA_B() -> Dict:
    return {"name": QUESTIONNAIRE_NAME_DIA_B, "id": QUESTIONNAIRE_ID}


@pytest.fixture
def mock_request_values_CONTACT_INFO() -> Dict:
    return {"name": QUESTIONNAIRE_NAME_CONTACT_INFO, "id": QUESTIONNAIRE_ID}


def test_deliver_mi_hub_reports_cloud_function_processor_raises_exception_when_triggered_with_an_invalid_request(
    config,
):
    # arrange
    mock_request = flask.Request.from_values()

    # act & assert
    with pytest.raises(Exception) as err:
        deliver_mi_hub_reports_cloud_function_processor(mock_request, config)

    assert (
        str(err.value)
        == "415 Unsupported Media Type: Did not attempt to load JSON data because the request Content-Type was not 'application/json'."
    )


@mock.patch("cloud_functions.deliver_mi_hub_reports.init_google_storage")
def test_deliver_mi_hub_reports_cloud_function_processor_raises_exception_when_google_storage_bucket_fails_to_connect(
    _mock_init_google_storage, mock_request_values, fake_google_storage, config
):
    # arrange
    mock_request = flask.Request.from_values(json=mock_request_values)
    _mock_init_google_storage.return_value = fake_google_storage

    # act & assert
    with pytest.raises(Exception) as err:
        deliver_mi_hub_reports_cloud_function_processor(mock_request, config)

    assert (
        str(err.value)
        == f"('Connection to storage bucket {config.nifi_staging_bucket} failed', 500)"
    )


@mock.patch("cloud_functions.deliver_mi_hub_reports.init_google_storage")
@mock.patch("cloud_functions.deliver_mi_hub_reports.get_mi_hub_call_history")
@mock.patch("cloud_functions.deliver_mi_hub_reports.get_mi_hub_respondent_data")
@mock.patch(
    "cloud_functions.deliver_mi_hub_reports.DeliverMiHubReportsService.upload_mi_hub_reports_to_gcp"
)
def test_deliver_mi_hub_reports_cloud_function_processor_calls_get_mi_hub_call_history_with_the_correct_parameters(
    _mock_upload_mi_hub_reports_to_gcp,
    _mock_get_mi_hub_respondent_data,
    _mock_get_mi_hub_call_history,
    _mock_init_google_storage,
    mock_request_values,
    config,
    fake_google_storage,
):
    # arrange
    mock_request = flask.Request.from_values(json=mock_request_values)
    fake_google_storage.bucket = "not-none"
    _mock_init_google_storage.return_value = fake_google_storage

    # act
    deliver_mi_hub_reports_cloud_function_processor(mock_request, config)

    # assert
    _mock_get_mi_hub_call_history.assert_called_with(
        config, QUESTIONNAIRE_NAME, QUESTIONNAIRE_ID
    )


@mock.patch("cloud_functions.deliver_mi_hub_reports.init_google_storage")
@mock.patch("cloud_functions.deliver_mi_hub_reports.get_mi_hub_call_history")
@mock.patch("cloud_functions.deliver_mi_hub_reports.get_mi_hub_respondent_data")
@mock.patch(
    "cloud_functions.deliver_mi_hub_reports.DeliverMiHubReportsService.upload_mi_hub_reports_to_gcp"
)
def test_deliver_mi_hub_reports_cloud_function_processor_calls_get_mi_hub_respondent_data_with_the_correct_parameters(
    _mock_upload_mi_hub_reports_to_gcp,
    _mock_get_mi_hub_respondent_data,
    _mock_get_mi_hub_call_history,
    _mock_init_google_storage,
    mock_request_values,
    config,
    fake_google_storage,
):
    # arrange
    mock_request = flask.Request.from_values(json=mock_request_values)
    fake_google_storage.bucket = "not-none"
    _mock_init_google_storage.return_value = fake_google_storage

    # act
    deliver_mi_hub_reports_cloud_function_processor(mock_request, config)

    # assert
    _mock_get_mi_hub_respondent_data.assert_called_with(config, QUESTIONNAIRE_NAME)


@mock.patch("cloud_functions.deliver_mi_hub_reports.init_google_storage")
@mock.patch("cloud_functions.deliver_mi_hub_reports.get_mi_hub_call_history")
@mock.patch("cloud_functions.deliver_mi_hub_reports.get_mi_hub_respondent_data")
@mock.patch(
    "cloud_functions.deliver_mi_hub_reports.DeliverMiHubReportsService.upload_mi_hub_reports_to_gcp"
)
def test_deliver_mi_hub_reports_cloud_function_processor_calls_upload_mi_hub_reports_to_gcp_with_the_correct_parameters(
    _mock_upload_mi_hub_reports_to_gcp,
    _mock_get_mi_hub_respondent_data,
    _mock_get_mi_hub_call_history,
    _mock_init_google_storage,
    mock_request_values,
    config,
    fake_google_storage,
    mock_mi_hub_call_history,
    mock_mi_hub_respondent_data,
):
    # arrange
    mock_request = flask.Request.from_values(json=mock_request_values)
    fake_google_storage.bucket = "not-none"
    _mock_init_google_storage.return_value = fake_google_storage
    _mock_get_mi_hub_call_history.return_value = mock_mi_hub_call_history
    _mock_get_mi_hub_respondent_data.return_value = mock_mi_hub_respondent_data

    # act
    deliver_mi_hub_reports_cloud_function_processor(mock_request, config)

    # assert
    _mock_upload_mi_hub_reports_to_gcp.assert_called_with(
        QUESTIONNAIRE_NAME,
        mock_mi_hub_call_history,
        mock_mi_hub_respondent_data,
        fake_google_storage,
    )


@mock.patch("cloud_functions.deliver_mi_hub_reports.init_google_storage")
@mock.patch("cloud_functions.deliver_mi_hub_reports.get_mi_hub_call_history")
@mock.patch("cloud_functions.deliver_mi_hub_reports.get_mi_hub_respondent_data")
@mock.patch(
    "cloud_functions.deliver_mi_hub_reports.DeliverMiHubReportsService.upload_mi_hub_reports_to_gcp"
)
def test_deliver_mi_hub_reports_cloud_function_processor_calling_get_mi_hub_call_history_as_DIA_A(
    _mock_upload_mi_hub_reports_to_gcp,
    _mock_get_mi_hub_respondent_data,
    _mock_get_mi_hub_call_history,
    _mock_init_google_storage,
    mock_request_values_DIA_A,
    config,
    fake_google_storage,
):
    # arrange
    mock_request = flask.Request.from_values(json=mock_request_values_DIA_A)
    fake_google_storage.bucket = "not-none"
    _mock_init_google_storage.return_value = fake_google_storage

    # act
    deliver_mi_hub_reports_cloud_function_processor(mock_request, config)

    # assert
    _mock_get_mi_hub_call_history.assert_called_with(
        config, QUESTIONNAIRE_NAME_DIA_A, QUESTIONNAIRE_ID
    )


@mock.patch("cloud_functions.deliver_mi_hub_reports.init_google_storage")
@mock.patch("cloud_functions.deliver_mi_hub_reports.get_mi_hub_call_history")
@mock.patch("cloud_functions.deliver_mi_hub_reports.get_mi_hub_respondent_data")
@mock.patch(
    "cloud_functions.deliver_mi_hub_reports.DeliverMiHubReportsService.upload_mi_hub_reports_to_gcp"
)
def test_deliver_mi_hub_reports_cloud_function_processor_skips_calling_get_mi_hub_call_history_as_DIA_B(
    _mock_upload_mi_hub_reports_to_gcp,
    _mock_get_mi_hub_respondent_data,
    _mock_get_mi_hub_call_history,
    _mock_init_google_storage,
    mock_request_values_DIA_B,
    config,
    fake_google_storage,
):
    # arrange
    mock_request = flask.Request.from_values(json=mock_request_values_DIA_B)
    fake_google_storage.bucket = "not-none"
    _mock_init_google_storage.return_value = fake_google_storage

    # act
    return_value = deliver_mi_hub_reports_cloud_function_processor(mock_request, config)

    # assert
    _mock_get_mi_hub_call_history.assert_not_called()
    assert (
        return_value
        == f"Skipping '{QUESTIONNAIRE_NAME_DIA_B}' as do not process version B or ContactInfo questionnaires"
    )


@mock.patch("cloud_functions.deliver_mi_hub_reports.init_google_storage")
@mock.patch("cloud_functions.deliver_mi_hub_reports.get_mi_hub_call_history")
@mock.patch("cloud_functions.deliver_mi_hub_reports.get_mi_hub_respondent_data")
@mock.patch(
    "cloud_functions.deliver_mi_hub_reports.DeliverMiHubReportsService.upload_mi_hub_reports_to_gcp"
)
def test_deliver_mi_hub_reports_cloud_function_processor_skips_calling_get_mi_hub_call_history_as_CONFIG(
    _mock_upload_mi_hub_reports_to_gcp,
    _mock_get_mi_hub_respondent_data,
    _mock_get_mi_hub_call_history,
    _mock_init_google_storage,
    mock_request_values_CONTACT_INFO,
    config,
    fake_google_storage,
):
    # arrange
    mock_request = flask.Request.from_values(json=mock_request_values_CONTACT_INFO)
    fake_google_storage.bucket = "not-none"
    _mock_init_google_storage.return_value = fake_google_storage

    # act
    return_value = deliver_mi_hub_reports_cloud_function_processor(mock_request, config)

    # assert
    _mock_get_mi_hub_call_history.assert_not_called()
    assert (
        return_value
        == f"Skipping '{QUESTIONNAIRE_NAME_CONTACT_INFO}' as do not process version B or ContactInfo questionnaires"
    )
