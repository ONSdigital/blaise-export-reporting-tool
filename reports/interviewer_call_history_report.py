from google.cloud import datastore

from functions.date_functions import parse_date_string_to_datetime
from models.error_capture import BertException


def get_call_history_records(interviewer_name, start_date_string, end_date_string, survey_tla=None):
    start_date, end_date = parse_dates(start_date_string, end_date_string)
    if is_invalid(start_date) or is_invalid(end_date):
        raise BertException("Invalid date range parameters provided", 400)
    records = get_datastore_records(interviewer_name, start_date, end_date, survey_tla)
    results = identify_webnudge_cases(records)
    return results


def parse_dates(start_date_string, end_date_string):
    start_date = parse_date_string_to_datetime(start_date_string)
    end_date = parse_date_string_to_datetime(end_date_string, True)
    return start_date, end_date


def is_invalid(date):
    if date is None:
        return True


def get_datastore_records(interviewer_name, start_date, end_date, survey_tla):
    print(f"Getting call history data for interviewer '{interviewer_name}' between '{start_date}' and '{end_date}'")
    client = datastore.Client()
    query = client.query(kind="CallHistory")
    query.add_filter("interviewer", "=", interviewer_name)
    query.add_filter("call_start_time", ">=", start_date)
    query.add_filter("call_start_time", "<=", end_date)

    if survey_tla is not None:
        print(f"Filtering call history data by survey '{survey_tla}'")
        query.add_filter("survey", "=", survey_tla)

    query.order = ["call_start_time"]
    records = list(query.fetch())
    print(f"get_call_history_records_by_interviewer_and_date_range - {len(records)} records found")
    return records


def identify_webnudge_cases(records):
    for record in records:
        if record["outcome_code"] == "120":
            record["status"] = "WebNudge"

    return records
