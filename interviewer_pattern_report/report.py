import json
from interviewer_pattern_report.derived_variables import *
from data_sources.datastore import get_call_history_records_by_interviewer_and_date_range
from models.interviewer_pattern import InterviewerPatternReport

COLUMNS_TO_VALIDATE = ['call_start_time', 'call_end_time', 'number_of_interviews']


def get_invalid_fields(data):
    data = data.filter(COLUMNS_TO_VALIDATE)
    return ''.join(data.columns[data.isna().any()].tolist())


def generate_report(valid_call_history_dataframe):
    try:
        hours_worked = get_hours_worked(valid_call_history_dataframe)
        total_call_seconds = get_call_time_in_seconds(valid_call_history_dataframe)

        report = InterviewerPatternReport(
            hours_worked=hours_worked,
            call_time=convert_call_time_seconds_to_datetime_format(total_call_seconds),
            hours_on_calls_percentage=get_percentage_of_hours_on_calls(hours_worked, total_call_seconds),
            average_calls_per_hour=get_average_calls_per_hour(valid_call_history_dataframe, hours_worked),
            respondents_interviewed=get_respondents_interviewed(valid_call_history_dataframe),
            households_completed_successfully=get_number_of_households_completed_successfully(
                'numberwang', valid_call_history_dataframe),
            average_respondents_interviewed_per_hour=get_average_respondents_interviewed_per_hour(
                valid_call_history_dataframe, hours_worked),
            no_contacts_percentage=get_percentage_of_call_for_status(
                'no contact', valid_call_history_dataframe),
            appointments_for_contacts_percentage=get_percentage_of_call_for_status(
                'appointment made', valid_call_history_dataframe),
        )
    except ZeroDivisionError as err:
        return f"generate_report() failed with a ZeroDivisionError: {err}", None
    except Exception as err:
        return f"generate_report() failed: {err}", None
    return None, report


def drop_and_return_invalidated_records(dataframe):
    valid_records = dataframe.dropna(subset=COLUMNS_TO_VALIDATE)
    invalid_records = dataframe[~dataframe.index.isin(valid_records.index)]

    valid_records.reset_index(drop=True, inplace=True)
    invalid_records.reset_index(drop=True, inplace=True)

    return valid_records, invalid_records


def validate_dataframe(data):
    invalid_data = pd.DataFrame()
    valid_data = data.copy()

    valid_data.columns = valid_data.columns.str.lower()
    missing_data_found = valid_data.filter(COLUMNS_TO_VALIDATE).isna().any().any()

    if missing_data_found:
        valid_data = drop_and_return_invalidated_records(data)[0]
        invalid_data = drop_and_return_invalidated_records(data)[1]

    try:
        valid_data = valid_data.astype({"number_of_interviews": 'int32', "dial_secs": 'float64'})
    except Exception as err:
        return f"validate_dataframe() failed: {err}", None, None
    return None, valid_data, invalid_data


def create_dataframe(call_history):
    try:
        result = pd.DataFrame(data=call_history)
    except Exception as err:
        return f"create_dataframe failed: {err}", None
    return None, result


def get_call_pattern_records_by_interviewer_and_date_range(interviewer_name, start_date_string, end_date_string):
    call_history_records_error, call_history = get_call_history_records_by_interviewer_and_date_range(
        interviewer_name, start_date_string, end_date_string
    )
    if call_history_records_error:
        print(call_history_records_error[0])
        return (call_history_records_error[0], 400), []

    create_dataframe_error, call_history_dataframe = create_dataframe(call_history)
    if create_dataframe_error:
        print(create_dataframe_error)
        return (create_dataframe_error, 400), []

    validate_dataframe_error, valid_dataframe, invalid_dataframe = validate_dataframe(call_history_dataframe)
    if validate_dataframe_error:
        print(validate_dataframe_error)
        return (validate_dataframe_error, 400), []

    generate_report_error, report = generate_report(valid_dataframe)
    if generate_report_error:
        print(generate_report_error)
        return (generate_report_error, 400), []

    if not invalid_dataframe.empty:
        setattr(report,
                'discounted_invalid_records',
                f'{len(invalid_dataframe.index)}/{len(call_history_dataframe.index)}')

        setattr(report,
                'invalid_fields',
                f'{get_invalid_fields(call_history_dataframe)}')

    return None, report.json()


if __name__ == "__main__":
    stuff, things = get_call_pattern_records_by_interviewer_and_date_range("matpal", "2021-01-01", "2021-06-11")

    call_history_dataframe = pd.read_csv('/Users/ThornE1/Documents/Blaise/TO Report/uber_is_na_call_history_dataframe.csv', engine='python')
    get_invalid_fields(call_history_dataframe)
    print("foo")

    # import pprint
    # pp = pprint.PrettyPrinter(indent=4)
    # pp.pprint(get_call_pattern_records_by_interviewer_and_date_range("matpal", "2021-01-01", "2021-06-11")[1])

