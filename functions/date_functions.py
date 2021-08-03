import re
from datetime import datetime


def validate_date(date_text):
    try:
        if date_text != datetime.strptime(date_text, "%Y-%m-%d").strftime('%Y-%m-%d'):
            raise ValueError
        return True
    except ValueError:
        return False


def parse_date_string_to_datetime(date_string, end_of_day=False):
    x = re.search("^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}$", date_string)
    if not x:
        return None
    date_split = date_string.split("-")
    if end_of_day:
        return datetime(
            int(date_split[0]), int(date_split[1]), int(date_split[2]), 23, 59, 59
        )
    return datetime(int(date_split[0]), int(date_split[1]), int(date_split[2]))


def date_handler(start_date, end_date):
    if start_date is None or end_date is None:
        print("Invalid request, missing required date parameters")
        return '{"error": "Invalid request, missing required date parameters"}', 400

    if not validate_date(start_date) or not validate_date(end_date):
        print("Invalid request, date is not valid")
        return '{"error": "Invalid request, date is not valid"}', 400
