import pandas as pd

from data_sources.datastore import get_call_history_records_by_interviewer_and_date_range
from interviewer_pattern_report.derived_variables import get_hours_worked, get_total_call_seconds


def get_call_pattern_records_by_interviewer_and_date_range(interviewer_name, start_date_string, end_date_string):
    error, call_history = get_call_history_records_by_interviewer_and_date_range(
        interviewer_name, start_date_string, end_date_string
    )
    df_call_history = pd.DataFrame(call_history)
    hours_worked = get_hours_worked(df_call_history)
    total_call_seconds = get_total_call_seconds(df_call_history)
    df_call_pattern = pd.DataFrame([hours_worked]).append([total_call_seconds])
    return None, df_call_pattern
