from interviewer_pattern_report.derived_variables import *
from data_sources.datastore import get_call_history_records_by_interviewer_and_date_range
from models.interviewer_pattern import InterviewerPatternReport


def generate_report(call_history_dataframe):
    try:    # to calculate
        hours_worked = get_hours_worked(call_history_dataframe)
        total_call_seconds = get_call_time_in_seconds(call_history_dataframe)
        hours_on_calls_percentage = get_percentage_of_hours_on_calls(hours_worked, total_call_seconds)
        average_calls_per_hour = get_average_calls_per_hour(call_history_dataframe, hours_worked)
        respondents_interviewed = get_respondents_interviewed(call_history_dataframe)
        households_completed_successfully = get_number_of_households_completed_successfully('numberwang', call_history_dataframe)
        average_respondents_interviewed_per_hour = get_average_respondents_interviewed_per_hour(call_history_dataframe, hours_worked)
        no_contacts_percentage = get_percentage_of_call_for_status('no contact', call_history_dataframe)
        appointments_for_contacts_percentage = get_percentage_of_call_for_status('Appointment made', call_history_dataframe)
        formatted_total_call_time = convert_call_time_seconds_to_datetime_format(total_call_seconds)
    except ZeroDivisionError as err:
        return f"generate_report() failed with a ZeroDivisionError: {err}", None
    except Exception as err:
        return f"generate_report() failed: {err}", None

    try:    # to populate
        report = InterviewerPatternReport(
            hours_worked=hours_worked,
            call_time=formatted_total_call_time,
            hours_on_calls_percentage=hours_on_calls_percentage,
            average_calls_per_hour=average_calls_per_hour,
            respondents_interviewed=respondents_interviewed,
            households_completed_successfully=households_completed_successfully,
            average_respondents_interviewed_per_hour=average_respondents_interviewed_per_hour,
            no_contacts_percentage=no_contacts_percentage,
            appointments_for_contacts_percentage=appointments_for_contacts_percentage,
        )
    except Exception as err:
        return f"generate_report() failed: {err}", None

    return None, report


def has_any_missing_data(series):
    if series.isnull().values.any():
        return True
    return False


def drop_invalidated_records(dataframe, column_name):
    return dataframe.dropna(subset=[column_name])


def validate_dataframe(call_history_dataframe):
    call_history_dataframe.columns = call_history_dataframe.columns.str.lower()

    if has_any_missing_data(call_history_dataframe['call_start_time']):
        return "validate_dataframe() failed: call_start_time has missing values", None

    if has_any_missing_data(call_history_dataframe['call_end_time']):
        call_history_dataframe = drop_invalidated_records(call_history_dataframe, 'call_end_time')

    if has_any_missing_data(call_history_dataframe['number_of_interviews']):
        return "validate_dataframe() failed: number_of_interviews has missing values", None

    if has_any_missing_data(call_history_dataframe['dial_secs']):
        call_history_dataframe = drop_invalidated_records(call_history_dataframe, 'dial_secs')

    try:
        call_history_dataframe = call_history_dataframe.astype({"number_of_interviews": 'int32', "dial_secs": 'float64'})
    except Exception as err:
        return f"validate_dataframe() failed: {err}", None
    return None, call_history_dataframe


def create_dataframe(call_history):
    try:
        call_history_dataframe = pd.DataFrame(data=call_history)
    except Exception as err:
        return f"create_dataframe failed: {err}", None
    return None, call_history_dataframe


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

    validate_dataframe_error, call_history_dataframe = validate_dataframe(call_history_dataframe)
    if validate_dataframe_error:
        print(validate_dataframe_error)
        return (validate_dataframe_error, 400), []

    generate_report_error, report = generate_report(call_history_dataframe)
    if generate_report_error:
        print(generate_report_error)
        return (generate_report_error, 400), []

    return None, report.json()


if __name__ == "__main__":
    import pprint
    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(get_call_pattern_records_by_interviewer_and_date_range("matpal", "2021-01-01", "2021-06-11")[1])

