from flask import Flask, jsonify, request

from data_sources.datastore_data import get_call_history_report_status
from functions.date_functions import validate_date
from models.config_model import Config
from reports.interviewer_call_history_report import get_call_history_records_by_interviewer_and_date_range
from reports.interviewer_call_pattern_report import get_call_pattern_records_by_interviewer_and_date_range

app = Flask(__name__)


def load_config(application):
    application.configuration = Config.from_env()
    application.configuration.log()


@app.route("/api/reports/call-history-status")
def call_history_report_status():
    return jsonify(get_call_history_report_status())


@app.route("/api/reports/call-history/<interviewer>")
def call_history(interviewer):
    start_date = request.args.get("start-date", None)
    end_date = request.args.get("end-date", None)
    print(f"Getting call history data for {interviewer} between {start_date} and {end_date}")
    if start_date is None or end_date is None:
        print("Invalid request, missing required date parameters")
        return '{"error": "Invalid request, missing required date parameters"}', 400
    if not validate_date(start_date) or not validate_date(end_date):
        print("Invalid request, date is not valid")
        return '{"error": "Invalid request, date is not valid"}', 400
    error, results = get_call_history_records_by_interviewer_and_date_range(interviewer, start_date, end_date)
    if error:
        error_message, error_code = error
        print(error_message)
        return error_message, error_code
    return jsonify(results)


@app.route("/api/reports/call-pattern/<interviewer>")
def call_pattern(interviewer):
    start_date = request.args.get("start-date", None)
    end_date = request.args.get("end-date", None)
    print(f"Getting call pattern data for {interviewer} between {start_date} and {end_date}")
    if start_date is None or end_date is None:
        print("Invalid request, missing required date parameters")
        return '{"error": "Invalid request, missing required date parameters"}', 400
    if not validate_date(start_date) or not validate_date(end_date):
        print("Invalid request, date is not valid")
        return '{"error": "Invalid request, date is not valid"}', 400
    error, results = get_call_pattern_records_by_interviewer_and_date_range(interviewer, start_date, end_date)
    if error:
        error_message, error_code = error
        print(error_message)
        return error_message, error_code
    if results == {}:
        return {}
    else:
        return results.json()
