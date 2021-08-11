from google.cloud import datastore

from functions.date_functions import parse_date_string_to_datetime
from models.error_capture import BertException


def get_call_history_records_by_interviewer_and_date_range(interviewer_name, start_date_string, end_date_string, survey_tla=None):
    start_date = parse_date_string_to_datetime(start_date_string)
    end_date = parse_date_string_to_datetime(end_date_string, True)
    if start_date is None or end_date is None:
        raise BertException("Invalid date range parameters provided", 400)

    client = datastore.Client()
    query = client.query(kind="CallHistory")
    query.add_filter("interviewer", "=", interviewer_name)
    query.add_filter("call_start_time", ">=", start_date)
    query.add_filter("call_start_time", "<=", end_date)

    if survey_tla is not None:
        query.add_filter("survey", "=", survey_tla)

    query.order = ["call_start_time"]
    results = list(query.fetch())
    print(f"get_call_history_records_by_interviewer_and_date_range - {len(results)} records found")
    return results
