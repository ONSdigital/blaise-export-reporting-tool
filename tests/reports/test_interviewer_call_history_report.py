import pytest

from tests.helpers.interviewer_call_history_helpers import entity_builder

from unittest.mock import patch

from models.error_capture import BertException
from reports.interviewer_call_history_report import get_call_history_records


def test_get_call_history_records_with_invalid_dates(interviewer_name, invalid_date):
    with pytest.raises(BertException) as err:
        get_call_history_records(interviewer_name, invalid_date, invalid_date)

    assert err.value.message == "Invalid date range parameters provided"
    assert err.value.code == 400


@patch("reports.interviewer_call_history_report.get_datastore_records")
def test_get_call_history_records_with_one_datastore_entity(mock_get_datastore_records, interviewer_name,
                                                            start_date_as_string, end_date_as_string,
                                                            arbitrary_outcome_code):
    mock_datastore_entity = [
        entity_builder(
            1, interviewer_name, start_date_as_string, end_date_as_string, arbitrary_outcome_code, "Completed"
        )
    ]

    mock_get_datastore_records.return_value = mock_datastore_entity
    results = get_call_history_records(interviewer_name, start_date_as_string, end_date_as_string)

    assert len(results) == 1
    assert results == mock_datastore_entity


@patch("reports.interviewer_call_history_report.get_datastore_records")
def test_get_call_history_records_with_one_datastore_entity_returns_webnudge(mock_get_datastore_records,
                                                                             interviewer_name, start_date_as_string,
                                                                             end_date_as_string, webnudge_outcome_code):
    original_status = "Completed"
    new_status = "WebNudge"

    mock_datastore_entity = [
        entity_builder(
            1, interviewer_name, start_date_as_string, end_date_as_string, webnudge_outcome_code, original_status
        )
    ]

    mock_get_datastore_records.return_value = mock_datastore_entity
    assert len(mock_datastore_entity) == 1
    assert mock_datastore_entity[0]["status"] == original_status

    results = get_call_history_records(interviewer_name, start_date_as_string, end_date_as_string)

    assert len(results) == 1
    assert results[0]["status"] == new_status


@patch("reports.interviewer_call_history_report.get_datastore_records")
def test_get_call_history_records_with_multiple_entities_returns_one_webnudge(
        mock_get_datastore_records, interviewer_name, start_date_as_string,
        end_date_as_string, webnudge_outcome_code, arbitrary_outcome_code):
    original_status = "Completed"
    new_status = "WebNudge"

    mock_datastore_entities = [
        entity_builder(
            2, interviewer_name, start_date_as_string, end_date_as_string, webnudge_outcome_code, original_status
        ),
        entity_builder(
            3, interviewer_name, start_date_as_string, end_date_as_string, arbitrary_outcome_code, original_status
        ),
        entity_builder(
            4, interviewer_name, start_date_as_string, end_date_as_string, arbitrary_outcome_code, original_status
        )
    ]

    mock_get_datastore_records.return_value = mock_datastore_entities
    assert len(mock_datastore_entities) == 3
    assert mock_datastore_entities[0]["status"] == original_status
    assert mock_datastore_entities[1]["status"] == original_status
    assert mock_datastore_entities[2]["status"] == original_status

    results = get_call_history_records(interviewer_name, start_date_as_string, end_date_as_string)

    assert len(results) == 3
    assert results[0]["status"] == new_status
    assert results[1]["status"] == original_status
    assert results[2]["status"] == original_status


@patch("reports.interviewer_call_history_report.get_datastore_records")
def test_get_call_history_records_with_multiple_entities_returns_all_webnudges(
        mock_get_datastore_records, interviewer_name, start_date_as_string, end_date_as_string, webnudge_outcome_code):
    original_status = "Questionnaire"
    new_status = "WebNudge"

    mock_datastore_entities = [
        entity_builder(
            1, interviewer_name, start_date_as_string, end_date_as_string, webnudge_outcome_code, original_status
        ),
        entity_builder(
            2, interviewer_name, start_date_as_string, end_date_as_string, webnudge_outcome_code, original_status
        )
    ]

    mock_get_datastore_records.return_value = mock_datastore_entities
    assert len(mock_datastore_entities) == 2
    assert mock_datastore_entities[0]["status"] == original_status
    assert mock_datastore_entities[1]["status"] == original_status

    results = get_call_history_records(interviewer_name, start_date_as_string, end_date_as_string)

    assert len(results) == 2
    assert results[0]["status"] == new_status
    assert results[1]["status"] == new_status
