import pytest

from tests.helpers.interviewer_call_history_helpers import entity_builder

from unittest.mock import patch

from models.error_capture import BertException
from reports.interviewer_call_history_report import get_call_history_records

INTERVIEWER = "Bob"
START_DATE = "2022-1-24"
END_DATE = "2022-1-25"
ORIGINAL_CALL_RESULT = "Questionnaire"
NEW_CALL_RESULT = "WebNudge"


def test_get_call_history_records_with_invalid_dates(interviewer_name, invalid_date):
    with pytest.raises(BertException) as err:
        get_call_history_records(interviewer_name, invalid_date, invalid_date)

    assert err.value.message == "Invalid date range parameters provided"
    assert err.value.code == 400


@patch("reports.interviewer_call_history_report.get_datastore_records")
def test_get_call_history_records_with_one_datastore_entity(mock_get_datastore_records):
    mock_datastore_entity = [
        entity_builder(
            1, INTERVIEWER, START_DATE, END_DATE, "", ORIGINAL_CALL_RESULT
        )
    ]

    mock_get_datastore_records.return_value = mock_datastore_entity
    results = get_call_history_records(INTERVIEWER, START_DATE, END_DATE)

    assert len(results) == 1
    assert results == mock_datastore_entity


@patch("reports.interviewer_call_history_report.get_datastore_records")
def test_get_call_history_records_with_one_datastore_entity_returns_webnudge(mock_get_datastore_records):
    mock_datastore_entity = [
        entity_builder(
            1, INTERVIEWER, START_DATE, END_DATE, "120", ORIGINAL_CALL_RESULT
        )
    ]

    mock_get_datastore_records.return_value = mock_datastore_entity
    assert len(mock_datastore_entity) == 1
    assert mock_datastore_entity[0]["call_result"] == ORIGINAL_CALL_RESULT

    results = get_call_history_records(INTERVIEWER, START_DATE, END_DATE)

    assert len(results) == 1
    assert results[0]["call_result"] == NEW_CALL_RESULT


@patch("reports.interviewer_call_history_report.get_datastore_records")
def test_get_call_history_records_with_multiple_entities_returns_one_webnudge(mock_get_datastore_records):
    mock_datastore_entities = [
        entity_builder(
            2, INTERVIEWER, START_DATE, END_DATE, "120", ORIGINAL_CALL_RESULT
        ),
        entity_builder(
            3, INTERVIEWER, START_DATE, END_DATE, "", ORIGINAL_CALL_RESULT
        ),
        entity_builder(
            4, INTERVIEWER, START_DATE, END_DATE, "", ORIGINAL_CALL_RESULT
        )
    ]

    mock_get_datastore_records.return_value = mock_datastore_entities
    assert len(mock_datastore_entities) == 3
    assert mock_datastore_entities[0]["call_result"] == ORIGINAL_CALL_RESULT
    assert mock_datastore_entities[1]["call_result"] == ORIGINAL_CALL_RESULT
    assert mock_datastore_entities[2]["call_result"] == ORIGINAL_CALL_RESULT

    results = get_call_history_records(INTERVIEWER, START_DATE, END_DATE)

    assert len(results) == 3
    assert results[0]["call_result"] == NEW_CALL_RESULT
    assert results[1]["call_result"] == ORIGINAL_CALL_RESULT
    assert results[2]["call_result"] == ORIGINAL_CALL_RESULT


@patch("reports.interviewer_call_history_report.get_datastore_records")
def test_get_call_history_records_with_multiple_entities_returns_all_webnudges(mock_get_datastore_records):
    mock_datastore_entities = [
        entity_builder(
            1, INTERVIEWER, START_DATE, END_DATE, "120", ORIGINAL_CALL_RESULT
        ),
        entity_builder(
            2, INTERVIEWER, START_DATE, END_DATE, "120", ORIGINAL_CALL_RESULT
        )
    ]

    mock_get_datastore_records.return_value = mock_datastore_entities
    assert len(mock_datastore_entities) == 2
    assert mock_datastore_entities[0]["call_result"] == ORIGINAL_CALL_RESULT
    assert mock_datastore_entities[1]["call_result"] == ORIGINAL_CALL_RESULT

    results = get_call_history_records(INTERVIEWER, START_DATE, END_DATE)

    assert len(results) == 2
    assert results[0]["call_result"] == NEW_CALL_RESULT
    assert results[1]["call_result"] == NEW_CALL_RESULT
