from google.cloud import datastore
from functions.datastore_functions import get_call_history_records


def get_call_history_report(interviewer, start_date, end_date, survey_tla):
    records = get_call_history_records(interviewer, start_date, end_date, survey_tla)
    return list(set([record["questionnaire_name"] for record in records]))
