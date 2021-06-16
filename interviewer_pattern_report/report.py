import pandas as pd

from data_sources.datastore import get_call_history_records_by_interviewer_and_date_range
from interviewer_pattern_report.derived_variables import get_hours_worked, get_call_time, \
    get_percentage_of_time_on_calls, get_average_calls_per_hour, get_respondents_interviewed


def get_call_pattern_records_by_interviewer_and_date_range(interviewer_name, start_date_string, end_date_string):
    error, call_history = get_call_history_records_by_interviewer_and_date_range(
        interviewer_name, start_date_string, end_date_string
    )

    if error:
        print(f"get_call_pattern_records_by_interviewer_and_date_range failed because {error}")
        return

    percentage_hours_on_calls = f"{get_percentage_of_time_on_calls(get_hours_worked(call_history), get_call_time(call_history))}%"
    interviewer_pattern_dict = {
        'Hours worked': get_hours_worked(call_history),
        'Call time': get_call_time(call_history),
        '% Hours on calls': percentage_hours_on_calls,
        'Ave calls per working hour': get_average_calls_per_hour(call_history),
        'Respondents interviewed': get_respondents_interviewed(call_history),
    }

    # TODO: check some errorrrrs before returning!
    return None, interviewer_pattern_dict
