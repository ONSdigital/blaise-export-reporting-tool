import pytest
from functions.request_handlers import date_handler, survey_tla_handler
from models.error_capture import BertException


def test_date_handler_returns_error_when_start_date_parameter_is_not_provided(client):
    request = client.get("/api/reports/call-history/matpal?end-date=2021-01-01").request
    with pytest.raises(BertException) as err:
        date_handler(request)
    assert err.value.message == "Invalid request, missing required date parameters"
    assert err.value.code == 400


def test_date_handler_returns_error_when_start_date_value_is_not_provided(client):
    request = client.get("/api/reports/call-history/matpal?start-date=&end-date=2021-01-01").request
    with pytest.raises(BertException) as err:
        date_handler(request)
    assert err.value.message == "Invalid request, date is not valid"
    assert err.value.code == 400


def test_date_handler_returns_error_when_start_date_value_is_not_a_valid_date(client):
    request = client.get("/api/reports/call-history/matpal?start-date=blah&end-date=2021-01-01").request
    with pytest.raises(BertException) as err:
        date_handler(request)
    assert err.value.message == "Invalid request, date is not valid"
    assert err.value.code == 400


def test_date_handler_returns_error_when_end_date_parameter_is_not_provided(client):
    request = client.get("/api/reports/call-history/matpal?start-date=2021-01-01").request
    with pytest.raises(BertException) as err:
        date_handler(request)
    assert err.value.message == "Invalid request, missing required date parameters"
    assert err.value.code == 400


def test_date_handler_returns_error_when_end_date_value_is_not_provided(client):
    request = client.get("/api/reports/call-history/matpal?start-date=2021-01-01&end-date=").request
    with pytest.raises(BertException) as err:
        date_handler(request)
    assert err.value.message == "Invalid request, date is not valid"
    assert err.value.code == 400


def test_date_handler_returns_error_when_end_date_value_is_not_a_valid_date(client):
    request = client.get("/api/reports/call-history/matpal?start-date=2021-01-01&end-date=blah").request
    with pytest.raises(BertException) as err:
        date_handler(request)
    assert err.value.message == "Invalid request, date is not valid"
    assert err.value.code == 400


def test_survey_tla_handler_returns_none_when_survey_tla_parameter_is_not_provided(client):
    request = client.post(f"/api/reports/call-history/matpal?start-date=2021-01-01&end-date=2021-01-01").request
    assert survey_tla_handler(request) is None


def test_survey_tla_handler_returns_none_when_survey_tla_value_is_not_provided(client):
    request = client.post(f"/api/reports/call-history/matpal?start-date=2021-01-01&end-date=2021-01-01&survey-tla=").request
    assert survey_tla_handler(request) is None


@pytest.mark.parametrize(
    "survey_tla",
    [
        "123",
        "LMFAO",
        "!@$%",
        "O.N"
    ],
)
def test_survey_tla_handler_returns_error_when_survey_tla_is_not_a_valid_survey_tla(client, survey_tla):
    request = client.post(f"/api/reports/call-pattern/matpal?start-date=2021-01-01&end-date=2021-01-01&survey-tla={survey_tla}").request
    with pytest.raises(BertException) as err:
        survey_tla_handler(request)
    assert err.value.message == f"Invalid request, {survey_tla} is not a valid survey tla"
    assert err.value.code == 400


@pytest.mark.parametrize(
    "survey_tla, expected",
    [
        ("opn", "OPN"),
        ("LMS", "LMS"),
        ("iDk", "IDK"),
        ("LoL", "LOL"),
    ],
)
def test_survey_tla_handler_returns_upper_case_tla(client, survey_tla, expected):
    request = client.post(f"/api/reports/call-history/matpal?start-date=2021-01-01&end-date=2021-01-01&survey-tla={survey_tla}").request
    assert survey_tla_handler(request) == expected

