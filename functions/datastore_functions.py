from google.cloud import datastore
from functions.date_functions import parse_date_string_to_datetime
from models.error_capture import BertException


def get_call_history_records(interviewer_name, start_date_string, end_date_string, survey_tla=None,
                             questionnaires=None):
    start_date, end_date = parse_dates(start_date_string, end_date_string)
    if is_invalid(start_date) or is_invalid(end_date):
        raise BertException("Invalid date range parameters provided", 400)
    records = get_datastore_records(interviewer_name, start_date, end_date, survey_tla, questionnaires)
    results = identify_webnudge_cases(records)
    return results


def get_datastore_records(interviewer_name, start_date, end_date, survey_tla, questionnaires):
    client = datastore.Client()

    if questionnaires is None:
        return get_datastore_records_for_questionnaire(client, interviewer_name, start_date, survey_tla, end_date, None)

    records = []
    for questionnaire in questionnaires:
        records += get_datastore_records_for_questionnaire(client, interviewer_name, start_date, survey_tla, end_date,
                                                           questionnaire)
    return sorted(records, key=lambda record: record['call_start_time'])


def get_datastore_records_for_questionnaire(client, interviewer_name, start_date, survey_tla, end_date, questionnaire):
    print(f"Getting call history data for interviewer '{interviewer_name}' between '{start_date}' and '{end_date}'")
    query = client.query(kind="CallHistory")
    query.add_filter("interviewer", "=", interviewer_name)
    query.add_filter("call_start_time", ">=", start_date)
    query.add_filter("call_start_time", "<=", end_date)

    if survey_tla is not None:
        print(f"Filtering call history data by survey '{survey_tla}'")
        query.add_filter("survey", "=", survey_tla)

    if questionnaire is not None:
        print(f"Filtering call history data by instrument '{questionnaire}'")
        query.add_filter("questionnaire_name", "=", questionnaire)

    records = list(query.fetch())
    print(f"get_call_history_records_by_interviewer_and_date_range - {len(records)} records found")
    return records


def parse_dates(start_date_string, end_date_string):
    start_date = parse_date_string_to_datetime(start_date_string)
    end_date = parse_date_string_to_datetime(end_date_string, True)
    return start_date, end_date


def is_invalid(date):
    if date is None:
        return True


def identify_webnudge_cases(records):
    for record in records:
        if record["outcome_code"] == "120":
            record["call_result"] = "WebNudge"
            record["status"] = "WebNudge"
    return records
