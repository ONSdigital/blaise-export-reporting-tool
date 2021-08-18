import logging

from google.cloud import datastore

from functions.date_functions import parse_date_string_to_datetime
from models.error_capture import BertException

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)


def get_call_history_records_for_all_surveys(client, interviewer_name, start_date, end_date):
    query = client.query(kind="CallHistory")
    query.add_filter("interviewer", "=", interviewer_name)
    query.add_filter("call_start_time", ">=", start_date)
    query.add_filter("call_start_time", "<=", end_date)
    query.order = ["call_start_time"]

    logging.debug(f"Query: {query}")

    results = list(query.fetch())

    logging.info(f"get_call_history_records_by_interviewer_and_date_range - {len(results)} records found")
    return results


def get_call_history_records_by_survey(client, interviewer_name, start_date, end_date, survey_tla):
    logging.info(f"Filtering call history data by survey '{survey_tla}'")

    query = client.query(kind="CallHistoryBySurvey")
    query.add_filter("interviewer", "=", interviewer_name)
    query.add_filter("call_start_time", ">=", start_date)
    query.add_filter("call_start_time", "<=", end_date)
    query.add_filter("survey", "=", survey_tla)
    query.order = ["call_start_time"]

    logging.debug(f"Query: '{query}'")

    results = list(query.fetch())

    logging.info(f"get_call_history_records_by_interviewer_and_date_range - {len(results)} records found")
    return results


def get_call_history_records(interviewer_name, start_date_string, end_date_string, survey_tla=None):
    start_date = parse_date_string_to_datetime(start_date_string)
    end_date = parse_date_string_to_datetime(end_date_string, True)
    logging.info(f"Getting call history data for interviewer '{interviewer_name}' between '{start_date}' and '{end_date}'")

    if start_date is None or end_date is None:
        raise BertException("Invalid date range parameters provided", 400)

    client = datastore.Client()
    if survey_tla is not None:
        logging.debug(f"survey_tla is '{survey_tla}. Retrieving records by survey...'")
        return get_call_history_records_by_survey(client, interviewer_name, start_date, end_date, survey_tla)

    logging.debug(f"survey_tla is '{survey_tla}. Retrieving records for all surveys...'")
    return get_call_history_records_for_all_surveys(client, interviewer_name, start_date, end_date)
