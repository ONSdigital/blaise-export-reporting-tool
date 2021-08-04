from flask import Flask, jsonify, request

from data_sources.datastore_data import get_call_history_report_status
from models.config_model import Config
from models.error_capture import BertException
from functions.date_functions import date_handler
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
    return jsonify(get_call_history_records_by_interviewer_and_date_range(interviewer, start_date, end_date))


@app.route("/api/reports/call-pattern/<interviewer>")
def call_pattern(interviewer):
    start_date, end_date = date_handler(request)
    results = get_call_pattern_records_by_interviewer_and_date_range(interviewer, start_date, end_date)
    if results == {}:
        return {}
    else:
        return results.json()


@app.route("/api/reports/appointment-resource-planning/<date>")
def appointment_resource_planning(date):
    return jsonify(get_appointment_resource_planning_by_date(date))


@app.route("/bert/<version>/health")
def health_check(version):
    response = {"healthy": True}
    return jsonify(response)


@app.errorhandler(BertException)
def bert_error(error):
    return error.json(), error.code
