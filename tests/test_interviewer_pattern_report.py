import pytest

from interviewer_pattern import *


def test_richs_og_get_hours_worked_against_refactored_get_hours_worked():
    from google.api_core.datetime_helpers import DatetimeWithNanoseconds

    results = [
        {
            'call_start_time': DatetimeWithNanoseconds(2021, 1, 1, 10, 00, 00, tzinfo=datetime.timezone.utc),
            'call_end_time': DatetimeWithNanoseconds(2021, 1, 1, 10, 30, 00, tzinfo=datetime.timezone.utc)
        },
        {
            'call_start_time': DatetimeWithNanoseconds(2021, 1, 11, 00, 00, 00, tzinfo=datetime.timezone.utc),
            'call_end_time': DatetimeWithNanoseconds(2021, 1, 11, 11, 30, 00, tzinfo=datetime.timezone.utc)
        },
        {
            'call_start_time': DatetimeWithNanoseconds(2021, 1, 2, 10, 00, 00, tzinfo=datetime.timezone.utc),
            'call_end_time': DatetimeWithNanoseconds(2021, 1, 2, 11, 00, 00, tzinfo=datetime.timezone.utc)
        },
        {
            'call_start_time': DatetimeWithNanoseconds(2021, 1, 5, 14, 00, 00, tzinfo=datetime.timezone.utc),
            'call_end_time': DatetimeWithNanoseconds(2021, 1, 5, 16, 00, 00, tzinfo=datetime.timezone.utc)
        }
    ]
    df = pd.DataFrame(results)
    assert richs_og_get_hours_worked(results) == get_hours_worked(df)


def test_get_call_history_dataframe_by_interviewer():
    # TODO: How to mock datastore entities?
    pass


def test_get_hours_worked(mock_data):
    assert get_hours_worked(mock_data) == "15:00:00"


def test_get_call_time(mock_data):
    assert get_call_time(mock_data) == "64"


@pytest.mark.parametrize(
    "hours_worked, total_call_seconds, expected",
    [
        ("10:00:00", "18000", 50),
        ("30:00:00", "16200", 15),
        ("50:00:00", "135000", 75),
    ],
)
def test_get_percentage_of_time_on_calls(hours_worked, total_call_seconds, expected):
    assert get_percentage_of_time_on_calls(hours_worked, total_call_seconds) == expected


def test_get_average_calls_per_hour(mock_data):
    x = get_average_calls_per_hour(mock_data)
    assert True


def test_interviewer_pattern_data():
    expected_columns = [
        "Hours worked",
        "Call time",
        "% Hours on calls",
        "Ave calls per working hour",
        "Respondents interviewed",
        "Households completed successfully",
        "Ave respondents interviewed per working hour",
        "% Non-contacts for all calls",
        "% Appointments for contacts",
        "% Contacts with positive results (wave-1)",
        "% Contacts with positive results (wave-2+)",
        "% Contacts with refusals (wave-1)",
        "% Contacts with refusals (wave-2+)",
    ]

    actual_dictionary = interviewer_pattern_data("matpal", "2021-01-01", "2021-06-11")

    assert all(keys in actual_dictionary for keys in expected_columns)


