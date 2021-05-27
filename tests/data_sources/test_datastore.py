import datetime
import pytest

from data_sources.datastore import date_string_to_datetime


@pytest.mark.parametrize(
    "date_string, expected_datetime",
    [
        ("2021-05-01", [2021, 5, 1]),
        ("2021-12-11", [2021, 12, 11]),
        ("1997-12-24", [1997, 12, 24]),
    ],
)
def test_date_string_to_datetime_returns_valid_date(date_string, expected_datetime):
    expected_datetime = datetime.datetime(*expected_datetime)

    date_time = date_string_to_datetime(date_string)

    assert date_time == expected_datetime


@pytest.mark.parametrize(
    "date_string, expected_datetime",
    [
        ("2021-05-01", [2021, 5, 1]),
        ("2021-12-11", [2021, 12, 11]),
        ("1997-12-24", [1997, 12, 24]),
    ],
)
def test_date_string_to_datetime_returns_valid_date_with_end_time(date_string, expected_datetime):
    expected_datetime = datetime.datetime(*expected_datetime, 23, 59, 59)

    date_time = date_string_to_datetime(date_string, True)

    assert date_time == expected_datetime


@pytest.mark.parametrize(
    "date_string",
    [
        "bacon-----",
        "34-2021-12-11-sfds",
        "2021-12-11-sdfsdf",
        "12-21-23",
        "1222-330-45",
    ],
)
def test_date_string_to_datetime_returns_None_if_date_is_invalid(date_string):
    date_time = date_string_to_datetime(date_string)

    assert date_time == None
