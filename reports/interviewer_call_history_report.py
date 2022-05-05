from functions.datastore_functions import get_call_history_records


def get_call_history_report(interviewer, start_date, end_date, survey_tla, questionnaires):
    return get_call_history_records(interviewer, start_date, end_date, survey_tla, questionnaires)


