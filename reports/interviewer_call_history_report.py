import pandas as pd
from google.cloud import datastore

from functions.date_functions import parse_date_string_to_datetime
from models.error_capture import BertException


def get_call_history_records(
        interviewer_name: str,
        start_date_string: str,
        end_date_string: str,
        survey_tla: str,
) -> pd.DataFrame:
    """
    Query datastore and return a pandas dataframe.

    Args:
        interviewer_name: Name of interviewer to report on.
        start_date_string: Report start date in YYYY-MM-DD format.
        end_date_string: Report end date in YYYY-MM-DD format.
        survey_tla: Survey to report on (i.e. OPN, LMS).

    Returns:
        A pd.DataFrame of an individual's call history. Each row represents a call made to a respondent.

    Raises:
        BertException: get_call_history_records failed
    """
    try:
        start_date = parse_date_string_to_datetime(start_date_string)
        end_date = parse_date_string_to_datetime(end_date_string, True)

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
        return pd.DataFrame(list(query.fetch()))

    except Exception as err:
        raise BertException(f"get_call_history_records failed: {err}", 400)
