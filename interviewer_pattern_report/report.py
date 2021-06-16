import pandas as pd

from data_sources.datastore import get_call_history_records_by_interviewer_and_date_range
from interviewer_pattern_report.derived_variables import get_hours_worked, get_call_time, \
    get_percentage_of_time_on_calls


def get_call_pattern_records_by_interviewer_and_date_range(interviewer_name, start_date_string, end_date_string):
    error, call_history = get_call_history_records_by_interviewer_and_date_range(
        interviewer_name, start_date_string, end_date_string
    )

    if error:
        print(f"get_call_pattern_records_by_interviewer_and_date_range failed because {error}")
        return

    interviewer_pattern_dict = {
        'Hours worked': get_hours_worked(call_history),
        'Call time': get_call_time(call_history),
        # '% Hours on calls': f"{get_percentage_of_time_on_calls(get_hours_worked(call_history_dataframe), get_call_time(call_history_dataframe))}%",
        # 'Ave calls per working hour': get_average_calls_per_hour,
    }

    # TODO: check some errorrrrs before returning!
    return None, interviewer_pattern_dict

    # df_call_history = pd.DataFrame(call_history)
    # hours_worked = get_hours_worked(df_call_history)
    # total_call_seconds = get_total_call_seconds(df_call_history)
    # percentage_of_time_on_calls = get_percentage_of_time_on_calls(hours_worked, total_call_seconds)
    # df_call_pattern = pd.DataFrame([hours_worked]).append([total_call_seconds]).append([percentage_of_time_on_calls])
    # return None, df_call_pattern

