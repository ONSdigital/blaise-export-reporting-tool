from flask import Flask, jsonify, request

from data_sources.datastore import get_call_history_records_by_interviewer_and_date_range, \
    get_call_history_report_status
from functions.date_functions import date_handler
from interviewer_call_pattern_report.report import get_call_pattern_records_by_interviewer_and_date_range
from models.config import Config

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
    error = date_handler(start_date, end_date)
    if error:
        error_message, error_code = error
        print(error_message)
        return error_message, error_code

    print(f"Getting call history data for {interviewer} between {start_date} and {end_date}")

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
    error = date_handler(start_date, end_date)
    if error:
        error_message, error_code = error
        print(error_message)
        return error_message, error_code

    print(f"Getting call pattern data for {interviewer} between {start_date} and {end_date}")
    error, results = get_call_pattern_records_by_interviewer_and_date_range(interviewer, start_date, end_date)
    if error:
        error_message, error_code = error
        print(error_message)
        return error_message, error_code
    if results == {}:
        return {}
    else:
        return results.json()


@app.route("/bert/<version>/health")
def health_check(version):
    response = {"healthy": True}
    return jsonify(response)
