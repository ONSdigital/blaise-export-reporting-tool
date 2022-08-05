import datetime
from unittest.mock import patch

import pytest
from google.api_core.datetime_helpers import DatetimeWithNanoseconds

from models.error_capture import BertException
from functions.datastore_functions import (
    get_call_history_records,
    get_datastore_records,
    get_questionnaires,
)
from tests.helpers.interviewer_call_history_helpers import entity_builder


def test_get_call_history_records_with_invalid_dates(interviewer_name, invalid_date):
    with pytest.raises(BertException) as err:
        get_call_history_records(interviewer_name, invalid_date, invalid_date)

    assert err.value.message == "Invalid date range parameters provided"
    assert err.value.code == 400


@patch("functions.datastore_functions.get_datastore_records")
def test_get_call_history_records_with_one_datastore_entity(
    mock_get_datastore_records,
    interviewer_name,
    start_date_as_string,
    end_date_as_string,
    arbitrary_outcome_code,
):
    mock_datastore_entity = [
        entity_builder(
            1,
            interviewer_name,
            start_date_as_string,
            end_date_as_string,
            arbitrary_outcome_code,
            "Completed",
        )
    ]

    mock_get_datastore_records.return_value = mock_datastore_entity
    results = get_call_history_records(
        interviewer_name, start_date_as_string, end_date_as_string
    )

    assert len(results) == 1
    assert results == mock_datastore_entity
    mock_get_datastore_records.assert_called_with(
        interviewer_name,
        datetime.datetime(2021, 9, 22, 0, 0),
        datetime.datetime(2021, 9, 22, 23, 59, 59),
        None,
        None,
    )


@patch("functions.datastore_functions.get_datastore_records")
def test_get_call_history_records_with_one_datastore_entity_returns_webnudge(
    mock_get_datastore_records,
    interviewer_name,
    start_date_as_string,
    end_date_as_string,
    webnudge_outcome_code,
):
    original_status = "Completed"
    new_status = "WebNudge"

    mock_datastore_entity = [
        entity_builder(
            1,
            interviewer_name,
            start_date_as_string,
            end_date_as_string,
            webnudge_outcome_code,
            original_status,
        )
    ]

    mock_get_datastore_records.return_value = mock_datastore_entity
    assert len(mock_datastore_entity) == 1
    assert mock_datastore_entity[0]["status"] == original_status

    results = get_call_history_records(
        interviewer_name, start_date_as_string, end_date_as_string
    )

    assert len(results) == 1
    assert results[0]["status"] == new_status


@patch("functions.datastore_functions.get_datastore_records")
def test_get_call_history_records_with_multiple_entities_returns_one_webnudge(
    mock_get_datastore_records,
    interviewer_name,
    start_date_as_string,
    end_date_as_string,
    webnudge_outcome_code,
    arbitrary_outcome_code,
):
    original_status = "Completed"
    new_status = "WebNudge"

    mock_datastore_entities = [
        entity_builder(
            2,
            interviewer_name,
            start_date_as_string,
            end_date_as_string,
            webnudge_outcome_code,
            original_status,
        ),
        entity_builder(
            3,
            interviewer_name,
            start_date_as_string,
            end_date_as_string,
            arbitrary_outcome_code,
            original_status,
        ),
        entity_builder(
            4,
            interviewer_name,
            start_date_as_string,
            end_date_as_string,
            arbitrary_outcome_code,
            original_status,
        ),
    ]

    mock_get_datastore_records.return_value = mock_datastore_entities
    assert len(mock_datastore_entities) == 3
    assert mock_datastore_entities[0]["status"] == original_status
    assert mock_datastore_entities[1]["status"] == original_status
    assert mock_datastore_entities[2]["status"] == original_status

    results = get_call_history_records(
        interviewer_name, start_date_as_string, end_date_as_string
    )

    assert len(results) == 3
    assert results[0]["status"] == new_status
    assert results[1]["status"] == original_status
    assert results[2]["status"] == original_status


@patch("functions.datastore_functions.get_datastore_records")
def test_get_call_history_records_with_multiple_entities_returns_all_webnudges(
    mock_get_datastore_records,
    interviewer_name,
    start_date_as_string,
    end_date_as_string,
    webnudge_outcome_code,
):
    original_status = "Questionnaire"
    new_status = "WebNudge"

    mock_datastore_entities = [
        entity_builder(
            1,
            interviewer_name,
            start_date_as_string,
            end_date_as_string,
            webnudge_outcome_code,
            original_status,
        ),
        entity_builder(
            2,
            interviewer_name,
            start_date_as_string,
            end_date_as_string,
            webnudge_outcome_code,
            original_status,
        ),
    ]

    mock_get_datastore_records.return_value = mock_datastore_entities
    assert len(mock_datastore_entities) == 2
    assert mock_datastore_entities[0]["status"] == original_status
    assert mock_datastore_entities[1]["status"] == original_status

    results = get_call_history_records(
        interviewer_name, start_date_as_string, end_date_as_string
    )

    assert len(results) == 2
    assert results[0]["status"] == new_status
    assert results[1]["status"] == new_status


def to_datetime_with_nanoseconds(dt):
    return DatetimeWithNanoseconds(
        dt.year, dt.month, dt.day, dt.hour, dt.minute, tzinfo=datetime.timezone.utc
    )


def datastore_formatted_record(record):
    result = record.copy()

    del result["name"]
    result["call_start_time"] = to_datetime_with_nanoseconds(result["call_start_time"])
    result["call_end_time"] = to_datetime_with_nanoseconds(result["call_end_time"])

    return result


def datastore_formatted_records(records):
    return [datastore_formatted_record(record) for record in records]


@pytest.mark.integration_test
def test_get_datastore_records_returns_records_for_all_tlas_when_no_tla_specified(
    records_in_datastore,
):
    lms_call_record = {
        "name": "name=100001-2022-01-25 12:45:03",
        "interviewer": "James",
        "call_start_time": datetime.datetime(2022, 1, 25, 12, 45),
        "call_end_time": datetime.datetime(2022, 1, 25, 12, 47),
        "survey": "LMS",
    }
    opn_call_record = {
        "name": "name=100002-2022-01-25 12:45:03",
        "interviewer": "James",
        "call_start_time": datetime.datetime(2022, 1, 25, 12, 45),
        "call_end_time": datetime.datetime(2022, 1, 25, 12, 47),
        "survey": "OPN",
    }

    records = [lms_call_record, opn_call_record]

    with records_in_datastore(records):
        result = get_datastore_records(
            "James",
            datetime.datetime(
                2021,
                9,
                22,
                23,
            ),
            datetime.datetime(
                2022,
                1,
                26,
                23,
            ),
            None,
            None,
        )

        expected = datastore_formatted_records([lms_call_record, opn_call_record])

        result = [dict(r) for r in result]

        assert result == expected


@pytest.mark.integration_test
def test_get_datastore_records_returns_expected_result_when_called_with_a_given_tla(
    records_in_datastore,
):
    lms_call_record = {
        "name": "name=100001-2022-01-25 12:45:03",
        "interviewer": "James",
        "call_start_time": datetime.datetime(2022, 1, 25, 12, 45),
        "call_end_time": datetime.datetime(2022, 1, 25, 12, 47),
        "survey": "LMS",
    }
    opn_call_record = {
        "name": "name=100002-2022-01-25 12:45:03",
        "interviewer": "James",
        "call_start_time": datetime.datetime(2022, 1, 25, 12, 45),
        "call_end_time": datetime.datetime(2022, 1, 25, 12, 47),
        "survey": "OPN",
    }
    records = [
        lms_call_record,
        opn_call_record,
    ]
    with records_in_datastore(records):
        result = get_datastore_records(
            "James",
            datetime.datetime(
                2021,
                9,
                22,
                23,
            ),
            datetime.datetime(
                2022,
                1,
                26,
                23,
            ),
            "LMS",
            None,
        )

        expected = datastore_formatted_records([lms_call_record])

        result = [dict(r) for r in result]

        assert result == expected


@pytest.mark.integration_test
def test_get_datastore_records_returns_expected_result_when_called_with_given_questionnaires(
    records_in_datastore,
):
    questionnaire1_name = "LMS2101_AA1"
    questionnaire_call_record1 = {
        "name": "name=100001-2022-01-25 12:45:03",
        "interviewer": "James",
        "call_start_time": datetime.datetime(2022, 1, 25, 12, 45),
        "call_end_time": datetime.datetime(2022, 1, 25, 12, 47),
        "survey": "LMS",
        "questionnaire_name": questionnaire1_name,
    }

    questionnaire2_name = "LMS2101_BB1"
    questionnaire_call_record2 = {
        "name": "name=100002-2022-01-25 12:45:03",
        "interviewer": "James",
        "call_start_time": datetime.datetime(2022, 1, 25, 13, 45),
        "call_end_time": datetime.datetime(2022, 1, 25, 13, 47),
        "survey": "LMS",
        "questionnaire_name": questionnaire2_name,
    }

    questionnaire_call_record3 = {
        "name": "name=100003-2022-01-25 12:45:03",
        "interviewer": "James",
        "call_start_time": datetime.datetime(2022, 1, 25, 14, 45),
        "call_end_time": datetime.datetime(2022, 1, 25, 14, 47),
        "survey": "OPN",
        "questionnaire_name": "OPN2101_CC1",
    }

    records = [
        questionnaire_call_record1,
        questionnaire_call_record2,
        questionnaire_call_record3,
    ]
    with records_in_datastore(records):
        result = get_datastore_records(
            "James",
            datetime.datetime(
                2021,
                9,
                22,
                23,
            ),
            datetime.datetime(
                2022,
                1,
                26,
                23,
            ),
            None,
            [questionnaire1_name, questionnaire2_name],
        )

        expected = datastore_formatted_records(
            [questionnaire_call_record1, questionnaire_call_record2]
        )

        result = [dict(r) for r in result]

        assert result == expected


@pytest.mark.integration_test
def test_get_datastore_records_returns_expected_result_when_called_with_given_questionnaires_sorted_by_time(
    records_in_datastore,
):
    first_call_record = {
        "name": "name=100003-2022-01-25 12:45:03",
        "interviewer": "James",
        "call_start_time": datetime.datetime(2022, 1, 25, 12, 45),
        "call_end_time": datetime.datetime(2022, 1, 25, 12, 47),
        "survey": "OPN",
        "questionnaire_name": "OPN2101_CC1",
    }
    second_call_record = {
        "name": "name=100002-2022-01-25 12:45:03",
        "interviewer": "James",
        "call_start_time": datetime.datetime(2022, 1, 25, 13, 45),
        "call_end_time": datetime.datetime(2022, 1, 25, 13, 47),
        "survey": "LMS",
        "questionnaire_name": "LMS2101_BB1",
    }
    third_call_record = {
        "name": "name=100001-2022-01-25 12:45:03",
        "interviewer": "James",
        "call_start_time": datetime.datetime(2022, 1, 25, 14, 45),
        "call_end_time": datetime.datetime(2022, 1, 25, 14, 47),
        "survey": "LMS",
        "questionnaire_name": "LMS2101_AA1",
    }
    records = [
        third_call_record,
        second_call_record,
        first_call_record,
    ]
    with records_in_datastore(records):
        result = get_datastore_records(
            "James",
            datetime.datetime(
                2021,
                9,
                22,
                23,
            ),
            datetime.datetime(
                2022,
                1,
                26,
                23,
            ),
            None,
            ["LMS2101_AA1", "LMS2101_BB1"],
        )

        expected = datastore_formatted_records([second_call_record, third_call_record])

        result = [dict(r) for r in result]

        assert result == expected


@pytest.mark.integration_test
def test_get_datastore_records_returns_expected_result_when_called_with_multiple_interviewers(
    records_in_datastore,
):
    james_call_record = {
        "name": "name=100001-2022-01-25 12:45:03",
        "interviewer": "James",
        "call_start_time": datetime.datetime(2022, 1, 25, 12, 45),
        "call_end_time": datetime.datetime(2022, 1, 25, 12, 47),
        "survey": "LMS",
    }
    el_call_record = {
        "name": "name=100002-2022-01-25 12:45:03",
        "interviewer": "El",
        "call_start_time": datetime.datetime(2022, 1, 25, 12, 45),
        "call_end_time": datetime.datetime(2022, 1, 25, 12, 47),
        "survey": "LMS",
    }
    records = [
        james_call_record,
        el_call_record,
    ]
    with records_in_datastore(records):
        result = get_datastore_records(
            "James",
            datetime.datetime(
                2021,
                9,
                22,
                23,
            ),
            datetime.datetime(
                2022,
                1,
                26,
                23,
            ),
            None,
            None,
        )

        expected = datastore_formatted_records([james_call_record])

        result = [dict(r) for r in result]

        assert result == expected


@pytest.mark.integration_test
def test_get_datastore_records_returns_expected_result_when_called_with_calls_outside_of_date_range(
    records_in_datastore,
):
    search_start_date = datetime.datetime(2021, 9, 22, 23)
    search_end_date = datetime.datetime(2021, 9, 26, 23)
    call_record_before_start_date = {
        "name": "name=100002-2022-01-25 12:45:03",
        "interviewer": "El",
        "call_start_time": datetime.datetime(2021, 1, 25, 12, 45),
        "call_end_time": datetime.datetime(2021, 1, 25, 12, 47),
        "survey": "LMS",
    }
    call_record_after_end_date = {
        "name": "name=100001-2022-01-25 12:45:03",
        "interviewer": "James",
        "call_start_time": datetime.datetime(2022, 1, 25, 12, 45),
        "call_end_time": datetime.datetime(2022, 1, 25, 12, 47),
        "survey": "LMS",
    }
    records = [
        call_record_after_end_date,
        call_record_before_start_date,
    ]
    with records_in_datastore(records):
        result = get_datastore_records(
            "James", search_start_date, search_end_date, None, None
        )

        expected = []

        assert result == expected


@patch("functions.datastore_functions.get_datastore_records")
def test_get_call_history_instruments_returns_a_list_of_unique_questionnaires(
    mock_get_datastore_records,
    interviewer_name,
    start_date_as_string,
    end_date_as_string,
    arbitrary_outcome_code,
):
    mock_datastore_entity = [
        entity_builder(
            1,
            interviewer_name,
            start_date_as_string,
            end_date_as_string,
            arbitrary_outcome_code,
            "Completed",
            "LMS2202_TST",
        ),
        entity_builder(
            2,
            interviewer_name,
            start_date_as_string,
            end_date_as_string,
            arbitrary_outcome_code,
            "Completed",
            "LMS2202_TST",
        ),
        entity_builder(
            1,
            interviewer_name,
            start_date_as_string,
            end_date_as_string,
            arbitrary_outcome_code,
            "Completed",
            "LMS2101_AA1",
        ),
    ]

    mock_get_datastore_records.return_value = mock_datastore_entity
    results = get_questionnaires(
        interviewer_name, start_date_as_string, end_date_as_string, "LMS"
    )

    assert set(results) == {"LMS2101_AA1", "LMS2202_TST"}
    mock_get_datastore_records.assert_called_with(
        interviewer_name,
        datetime.datetime(2021, 9, 22, 0, 0),
        datetime.datetime(2021, 9, 22, 23, 59, 59),
        "LMS",
        None,
    )


@patch("functions.datastore_functions.get_datastore_records")
def test_get_call_history_records_with_a_list_of_questionnaires(
    mock_get_datastore_records,
    interviewer_name,
    start_date_as_string,
    end_date_as_string,
    arbitrary_outcome_code,
):
    mock_datastore_entity = [
        entity_builder(
            1,
            interviewer_name,
            start_date_as_string,
            end_date_as_string,
            arbitrary_outcome_code,
            "Completed",
        )
    ]

    mock_get_datastore_records.return_value = mock_datastore_entity
    results = get_call_history_records(
        interviewer_name,
        start_date_as_string,
        end_date_as_string,
        survey_tla="LMS",
        questionnaires=["LMS2202_TST", "LMS2101_AA1"],
    )

    assert len(results) == 1
    assert results == mock_datastore_entity
    mock_get_datastore_records.assert_called_with(
        interviewer_name,
        datetime.datetime(2021, 9, 22, 0, 0),
        datetime.datetime(2021, 9, 22, 23, 59, 59),
        "LMS",
        ["LMS2202_TST", "LMS2101_AA1"],
    )
