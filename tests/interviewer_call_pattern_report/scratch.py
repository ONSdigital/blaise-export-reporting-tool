import datetime
import pytest
import math
from interviewer_call_pattern_report.report import validate_dataframe, generate_report, get_invalid_fields
from models.interviewer_call_pattern import InterviewerCallPattern
from interviewer_call_pattern_report.derived_variables import get_average_calls_per_hour


def test_input_data_can_be_used_to_recreate_uat_output(dev_data):
    print("\nThis ensures the input data used for testing matches the data used for UAT")
    assert generate_report_for_testing(dev_data) == InterviewerCallPattern(
        hours_worked='0:06:57',
        call_time='0:06:01',
        hours_on_calls_percentage='86.57%',
        average_calls_per_hour=25.9,
        respondents_interviewed=3,
        households_completed_successfully=0,
        average_respondents_interviewed_per_hour=25.9,
        no_contacts_percentage='66.67%',
        appointments_for_contacts_percentage='0.0%',
        discounted_invalid_records='1/4',
        invalid_fields='call_end_time')


def test_average_calculations_with_different_total_of_calls(dev_data):
    expected_results_from_all_calls = 34.53
    expected_valid_results = 25.9
    uat_results = 29.95

    hours_worked = '0:06:57'
    all_calls = dev_data                                    # 4
    total_valid_calls = validate_dataframe(dev_data)[1]     # 3

    all_average_calls_per_hour = get_average_calls_per_hour(all_calls, hours_worked)

    valid_average_calls_per_hour = get_average_calls_per_hour(total_valid_calls, hours_worked)

    print("\nThis proves Mark's method matches BERT")
    assert all_average_calls_per_hour == expected_results_from_all_calls
    assert valid_average_calls_per_hour == expected_valid_results

    print("and demonstrates the discounted records weren't the cause of the differing UAT results")
    assert all_average_calls_per_hour != uat_results
    assert valid_average_calls_per_hour != uat_results


@pytest.mark.parametrize(
    "total_hours, calls, expected_seconds, expected_call_time, expected_average",
    [
        ('0:06:57', 4, 417, 104.25, 34.53),             # all the calls
        ('0:06:57', 3, 417, 139.0, 25.9),               # valid calls only
        ('0:06:01', 3, 361, 120.33333333333333, 29.92), # using call time instead of hours worked
    ],
)
def test_and_use_output_to_demonstrate_to_mark(total_hours, calls, expected_seconds, expected_call_time, expected_average):
    seconds = get_seconds_in_decimal_from(total_hours)
    assert seconds == expected_seconds
    print(f"\n{total_hours} = {seconds} seconds")

    call_time = seconds / calls
    assert call_time == expected_call_time
    print(f"{seconds} seconds / {calls} calls = {call_time}")

    calls_per_hour = 3600 / call_time
    rounded_calls_per_hour = float("{:.2f}".format(calls_per_hour))
    assert rounded_calls_per_hour == expected_average
    print(f"divided by 36000 = {rounded_calls_per_hour}\n")


@pytest.mark.parametrize(
    "calls, hours, average, time, dev_message",
    [
        (42, 7, 6, "7:00:00", "this is a sanity check"),
        (3, 0.11583333333333333, 25.89928057553957, "0:06:57", "this uses valid calls only"),
        (4, 0.11583333333333333, 34.53237410071942, "0:06:57", "this uses all calls"),
        (4, 0.1335559265442404, 29.95, "0:08:00", "this is a reverse engineered rabbit hole of doom"),
        (3, 0.1001669449081803, 29.95, "0:06:00", "when you punch these numbers into a calculator it returns a sad face"),
    ],
)
def test_reverse_engineer_uat_value(calls, hours, average, time, dev_message):
    print(f"\n{dev_message}")

    assert math.isclose(average, calls/hours)
    print(f"{average} average is equal to {calls} calls / {hours} hours")

    assert math.isclose(calls, average*hours)
    print(f"{calls} calls is equal to {average} average * {hours} hours")

    assert math.isclose(hours, calls/average)
    print(f"{hours} hours is equal to {calls} calls / {average} average")

    assert str(get_pretty_time_from_decimal_for(hours)) == time
    print(f"hours worked: {get_pretty_time_from_decimal_for(hours)}")


@pytest.mark.parametrize(
    "hours, expected",
    [
        (1, "1:00:00"),
        (60, "60:00:00"),
        (24, "24:00:00"),
        (0.1335559265442404, "0:08:00"),
        (0.1001669449081803, "0:06:00"),
    ],
)
def test_get_pretty_time_from_decimal_for(hours, expected):
    assert get_pretty_time_from_decimal_for(hours) == expected


@pytest.mark.parametrize(
    "hours, expected",
    [
        ("00:00:01", 1),
        ("00:01:00", 60),
        ("01:00:00", 3600),
    ],
)
def test_get_seconds_in_decimal_from(hours, expected):
    assert get_seconds_in_decimal_from(hours) == expected


@pytest.mark.parametrize(
    "hours, expected",
    [
        ("01:00:00", 1),
        ("07:30:00", 7.5),
    ],
)
def test_get_seconds_in_decimal_from(hours, expected):
    assert get_seconds_in_decimal_from(hours) / 3600 == expected


def generate_report_for_testing(dev_data):
    validate_dataframe_error, valid_dataframe, invalid_dataframe = validate_dataframe(dev_data)

    generate_report_error, report = generate_report(valid_dataframe)

    report.discounted_invalid_records = f'{len(invalid_dataframe.index)}/{len(dev_data.index)}'
    report.invalid_fields = f'{get_invalid_fields(dev_data)}'

    return report


def get_seconds_in_decimal_from(total_hours):
    # convert hours_worked to seconds
    [hours, minutes, seconds] = [int(x) for x in total_hours.split(':')]
    x = datetime.timedelta(hours=hours, minutes=minutes, seconds=seconds)
    return x.seconds


def get_pretty_time_from_decimal_for(time):
    hours = int(time)
    minutes = (time*60) % 60
    seconds = (time*3600) % 60

    return "%d:%02d:%02d" % (hours, minutes, seconds)


def limit_two_decimal_places(float_value):
    return float("{:.2f}".format(float_value))


@pytest.mark.parametrize(
    "total_calls, hours_worked, expected",
    [
        (11, "01:00:00", 11),
        (11, "07:30:00", 1.47),
        (11, "07:30:00", 1.47),
    ],
)
def test_demo(total_calls, hours_worked, expected):
    divide = u"÷"

    seconds = get_seconds_in_decimal_from(hours_worked)
    hours = seconds / 3600

    print(f"\n\nTo calculate average calls per working hour the calculation is:\n total_calls {divide} hours_worked")

    print(f"\nbut first lets sanity check the hours are converting to decimal correctly...")

    print(f"\n{hours_worked}: {hours} hours")

    print("\nWhich means...")

    print(f"\ntotal_calls: {total_calls}")
    print(f"hours_worked: {hours}")
    print(f"{total_calls} {divide} {hours} = {total_calls/hours}")

    print(f"\n\nlimiting that to 2 decimal places gives us: {limit_two_decimal_places(total_calls/hours)}")

    assert limit_two_decimal_places(total_calls/hours) == expected
