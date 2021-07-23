from datetime import datetime

import pytest

from data_sources.datastore import parse_date_string_to_datetime


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
