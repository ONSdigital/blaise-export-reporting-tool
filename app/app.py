from flask import Flask, jsonify, request

from data_sources.datastore_data import get_call_history_report_status
from functions.date_functions import validate_date
from models.config_model import Config
from reports.appointment_resource_planning_report import get_appointment_resource_planning_by_date
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
    start_date, end_date = date_handler(request)

    error, results = get_call_history_records_by_interviewer_and_date_range(interviewer, start_date, end_date)
    if error:
        error_message, error_code = error
        print(error_message)
        return error_message, error_code
    return jsonify(results)


@app.route("/api/reports/call-pattern/<interviewer>")
def call_pattern(interviewer):
    start_date, end_date = date_handler(request)

    results = get_call_pattern_records_by_interviewer_and_date_range(interviewer, start_date, end_date)
    if results == {}:
        return {}
    else:
        return results.json()


@app.route("/bert/<version>/health")
def health_check(version):
    response = {"healthy": True}
    return jsonify(response)
