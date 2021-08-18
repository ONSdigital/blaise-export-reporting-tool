from models.error_capture import BertException
from functions.date_functions import validate_date

import logging
logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)


def date_handler(request):
    start_date = request.args.get("start-date", None)
    end_date = request.args.get("end-date", None)

    if start_date is None or end_date is None:
        raise(BertException("Invalid request, missing required date parameters", 400))

    if not validate_date(start_date) or not validate_date(end_date):
        raise(BertException("Invalid request, date is not valid", 400))

    return start_date, end_date


def survey_tla_handler(request):
    logging.debug(f"request: '{request}'")
    survey_tla = request.args.get("survey-tla", None)

    if survey_tla is None or survey_tla == "" or survey_tla == "undefined":
        logging.debug(f"survey-tla is '{survey_tla}'. Returning None...")
        return None

    if not survey_tla.isalpha() or len(survey_tla) != 3:
        logging.debug(f"survey-tla is '{survey_tla}'. Raising BertException...")
        raise(BertException(f"Invalid request, {survey_tla} is not a valid survey three letter acronym", 400))

    return survey_tla.upper()
