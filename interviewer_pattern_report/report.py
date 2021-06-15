import pandas as pd

from data_sources.datastore import get_call_history_records_by_interviewer
from interviewer_pattern_report.derived_variables import get_hours_worked


def get_call_pattern_records_by_interviewer(interviewer_name, start_date_string, end_date_string):
    thing, results = get_call_history_records_by_interviewer(
        interviewer_name, start_date_string, end_date_string
    )

    hours_worked = get_hours_worked(results)
    df_results = pd.DataFrame([hours_worked])

    return None, df_results
