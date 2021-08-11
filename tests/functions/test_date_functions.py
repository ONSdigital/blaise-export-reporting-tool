from datetime import datetime

import pytest

from functions.date_functions import date_handler, parse_date_string_to_datetime
from models.error_capture import BertException


@pytest.mark.parametrize(
    "date_string",
    [
        "bacon-----",
        "34-2021-12-11-sfds",
        "2021-12-11-sdfsdf",
        "12-21-23",
        "1222-330-45",
        "",
    ],
)
def test_parse_date_string_to_datetime_returns_none_if_date_is_invalid(date_string):
    date_time = parse_date_string_to_datetime(date_string)
    assert date_time is None


@pytest.mark.parametrize(
    "date_string, expected",
    [
        ("2021-05-01", [2021, 5, 1]),
        ("2021-12-11", [2021, 12, 11]),
        ("1997-12-24", [1997, 12, 24]),
    ],
)
def test_parse_date_string_to_datetime_returns_valid_date(date_string, expected):
    expected = datetime(*expected)
    date_time = parse_date_string_to_datetime(date_string)
    assert date_time == expected


@pytest.mark.parametrize(
    "date_string, expected",
    [
        ("2021-05-01", [2021, 5, 1]),
        ("2021-12-11", [2021, 12, 11]),
        ("1997-12-24", [1997, 12, 24]),
    ],
)
def test_parse_date_string_to_datetime_returns_valid_date_with_end_of_day_true(date_string, expected):
    expected = datetime(*expected, 23, 59, 59)
    date_time = parse_date_string_to_datetime(date_string, True)
    assert date_time == expected


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
