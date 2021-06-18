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
        households_completed_successfully = get_percentage_of_call_for_status('numberwang', call_history_dataframe)
        average_respondents_interviewed_per_hour = get_average_respondents_interviewed_per_hour(call_history_dataframe, hours_worked)
        no_contacts_percentage = get_percentage_of_call_for_status('no contact', call_history_dataframe)
        appointments_for_contacts_percentage = get_percentage_of_call_for_status('Appointment made', call_history_dataframe)
        formatted_total_call_time = convert_call_time_seconds_to_datetime_format(total_call_seconds)
    except ZeroDivisionError as zero_div_err:
        print(f"""You've been Wangernumbed! 
        {call_history_dataframe['interviewer']} worked {hours_worked} hours yet completed {len(call_history_dataframe.index)} calls. 
        Please review the data and try again. Goodbye!""")
        return zero_div_err, None
    except Exception as err:
        return err, None

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
        print(f"Populating the data model failed: {err}")
        return err, None

    return None, report


# TODO: Finish and test
def validate_dataframe(call_history_dataframe):
    try:
        call_history_dataframe = call_history_dataframe.astype({'number_of_interviews': 'int32'})
        # TODO: et al
    except Exception as err:
        print(f"validate_dataframe() failed: {err}")
        return err, None
    return None, call_history_dataframe


# TODO: Finish and test
def create_dataframe(call_history):
    call_history_dataframe = pd.DataFrame()

    try:
        call_history_dataframe = pd.DataFrame(data=call_history)
    except Exception as err:
        print(f"create_dataframe failed: {err}")
        return err, call_history_dataframe
    return None, call_history_dataframe


def get_call_pattern_records_by_interviewer_and_date_range(interviewer_name, start_date_string, end_date_string):
    call_history_records_error, call_history = get_call_history_records_by_interviewer_and_date_range(
        interviewer_name, start_date_string, end_date_string
    )
    if call_history_records_error:
        return call_history_records_error, []

    create_dataframe_error, call_history_dataframe = create_dataframe(call_history)
    if create_dataframe_error:
        return create_dataframe_error, []

    validate_dataframe_error, call_history_dataframe = validate_dataframe(call_history_dataframe)
    if validate_dataframe_error:
        return validate_dataframe_error, []

    generate_report_error, report = generate_report(call_history_dataframe)
    if generate_report_error:
        return generate_report_error, []

    return None, report.json()


if __name__ == "__main__":
    import pprint
    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(get_call_pattern_records_by_interviewer_and_date_range("matpal", "2021-01-01", "2021-06-11")[1])

