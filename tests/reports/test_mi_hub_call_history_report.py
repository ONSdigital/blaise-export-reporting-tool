import pytest

from reports.mi_hub_call_history_report import check_if_questionnaire_id_is_in_questionnaire_list


@pytest.mark.parametrize(
    "instrument_id, questionnaire_list, expected",
    [
        ("12345-67890", [{"id": "12345-67890"}], True),
        ("12345-67890", [{"id": "12345-67890"}, {"id": "00000-00000"}], True),
        ("12345-67890", [], False),
        ("12345-67890", [{"id": "00000-00000"}], False),
    ],
)
def test_check_if_questionnaire_id_is_in_questionnaire_list(instrument_id, questionnaire_list, expected):
    check_if_questionnaire_id_is_in_questionnaire_list(instrument_id, questionnaire_list) == expected
