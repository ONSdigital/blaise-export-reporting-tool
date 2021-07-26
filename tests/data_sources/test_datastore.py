import pytest

from data_sources.datastore import split_into_batches, get_call_history_records_by_interviewer_and_date_range


def test_get_call_history_records_by_interviewer_and_date_range_with_invalid_dates(interviewer_name, invalid_date):
    assert get_call_history_records_by_interviewer_and_date_range(interviewer_name, invalid_date, invalid_date) == (
        ("Invalid date range parameters provided", 400), [])


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
