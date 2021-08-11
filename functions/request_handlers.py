from models.error_capture import BertException
from functions.date_functions import validate_date


def date_handler(request):
    start_date = request.args.get("start-date", None)
    end_date = request.args.get("end-date", None)

    if start_date is None or end_date is None:
        raise(BertException("Invalid request, missing required date parameters", 400))

    if not validate_date(start_date) or not validate_date(end_date):
        raise(BertException("Invalid request, date is not valid", 400))

    return start_date, end_date
