from models.mi_hub_respondent_data import MiHubRespondentData


def test_mi_hub_respondent_data():
    mi_hub_respondent_data = MiHubRespondentData(
        serial_number="",
        outcome_code="",
        date_completed="",
        interviewer="",
        mode="",
        postcode="",
        gender="",
        date_of_birth="",
        age="",
    )
    assert mi_hub_respondent_data is not None
