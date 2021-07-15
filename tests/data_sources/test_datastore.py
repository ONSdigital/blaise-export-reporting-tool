import datetime

import pytest

from data_sources.datastore import parse_date_string_to_datetime, split_into_batches, \
    get_call_history_records_by_interviewer_and_date_range


@pytest.mark.parametrize(
    "date_string, expected",
    [
        ("2021-05-01", [2021, 5, 1]),
        ("2021-12-11", [2021, 12, 11]),
        ("1997-12-24", [1997, 12, 24]),
    ],
)
def test_parse_date_string_to_datetime_returns_valid_date(date_string, expected):
    expected = datetime.datetime(*expected)
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
def test_parse_date_string_to_datetime_returns_valid_date_with_end_time(date_string, expected):
    expected = datetime.datetime(*expected, 23, 59, 59)
    date_time = parse_date_string_to_datetime(date_string, True)
    assert date_time == expected


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
def test_date_string_to_datetime_returns_none_if_date_is_invalid(date_string):
    date_time = parse_date_string_to_datetime(date_string)
    assert date_time is None


def test_get_call_history_records_by_interviewer_and_date_range_with_invalid_dates(interviewer_name, invalid_date):
    assert get_call_history_records_by_interviewer_and_date_range(interviewer_name, invalid_date, invalid_date) == (
        ('Invalid date range parameters provided', 400), [])


@pytest.mark.parametrize(
    "list_to_split, number_to_split_by, expected",
    [
        (
                [
                    "item",
                    "item",
                    "item",
                    "item",
                    "item",
                    "item",
                    "item",
                    "item",
                    "item",
                    "item",
                ],
                2,
                [2, 2, 2, 2, 2],
        ),
        (
                [
                    "item",
                    "item",
                    "item",
                    "item",
                    "item",
                    "item",
                    "item",
                    "item",
                    "item",
                    "item",
                    "item",
                    "item",
                    "item",
                    "item",
                    "item",
                    "item",
                    "item",
                    "item",
                    "item",
                    "item",
                ],
                5,
                [5, 5, 5, 5],
        ),
        (["item", "item", "item"], 2, [2, 1]),
        (["item", "item", "item"], 5, [3]),
    ],
)
def test_split_into_batches(list_to_split, number_to_split_by, expected):
    split_list = split_into_batches(list_to_split, number_to_split_by)
    assert len(split_list) == len(expected)
    i = 0
    while i < len(split_list):
        assert len(split_list[i]) == expected[i]
        i += 1
