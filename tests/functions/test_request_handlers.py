import pytest
from functions.request_handlers import date_handler
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
