from interviewer_pattern_report.derived_variables import *
from data_sources.datastore import get_call_history_records_by_interviewer_and_date_range
from models.interviewer_pattern import InterviewerPatternReport


# TODO: Test
def convert_to_json(report):
    try:
        json_report = json.dumps(dataclasses.asdict(report))
    except Exception as err:
        print(f"convert_to_json() failed: {err}")
        return err, None
    return None, json_report


# TODO: Test
def generate_report(call_history_dataframe):
    try:
        hours_worked = get_hours_worked(call_history_dataframe)
        total_call_seconds = get_call_time_in_seconds(call_history_dataframe)

        report = InterviewerPatternReport(
            hours_worked=hours_worked,
            call_time=total_call_seconds,
            hours_on_calls_percentage=f"{100 * float(total_call_seconds)/float(get_total_seconds_from_string(hours_worked))}%",
            average_calls_per_hour=get_average_calls_per_hour(call_history_dataframe, hours_worked),
            respondents_interviewed=get_respondents_interviewed(call_history_dataframe),
            households_completed_successfully=get_percentage_of_call_for_status('numberwang', call_history_dataframe),
            average_respondents_interviewed_per_hour=get_average_respondents_interviewed_per_hour(call_history_dataframe, hours_worked),
            no_contacts_percentage=get_percentage_of_call_for_status('no contact', call_history_dataframe),
            appointments_for_contacts_percentage=get_percentage_of_call_for_status('Appointment made', call_history_dataframe),
        )
    except Exception as err:
        print(f"generate_report() failed: {err}")
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

