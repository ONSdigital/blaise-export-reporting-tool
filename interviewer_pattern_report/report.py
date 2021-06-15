import pandas as pd
import os

from data_sources.datastore import get_call_history_records, get_call_history_records_by_interviewer
from interviewer_pattern_report.derived_variables import get_hours_worked


def get_call_pattern_records_by_interviewer(interviewer_name, start_date_string, end_date_string):
    thing, results = get_call_history_records_by_interviewer(
        interviewer_name, start_date_string, end_date_string
    )

    hours_worked = get_hours_worked(results)
    df_results = pd.DataFrame([hours_worked])

    return None, df_results


def foo(results):
    x = pd.DataFrame(results)
    y = x[["call_start_time"]]
    return y


def get_call_history_as_dataframe():
    entities = get_call_history_records()
    for e in entities:
        e['entity_key'] = e.key
        e['entity_key_name'] = e.key.name

    return pd.DataFrame(entities)



if __name__ == "__main__":
    os.environ["GCLOUD_PROJECT"] = "ons-blaise-v2-dev-matt02"

    # shit from datastore
    just_interviewers = get_call_history_records_by_interviewer("matpal", "2021-01-01", "2021-06-11")
    everything = get_call_history_records()

    # le dataframes
    everything_df = foo(everything)
    interviewers_df = foo(just_interviewers)


