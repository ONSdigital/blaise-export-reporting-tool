from flask import Flask, jsonify, request, current_app

from data_sources.database import get_events
from data_sources.datastore import get_call_history_records, get_call_history_records_by_interviewer_and_date_range, \
    get_call_history_report_status
from extract_call_history import get_call_history
from interviewer_pattern_report.report import get_call_pattern_records_by_interviewer_and_date_range
from models.error_information import ErrorInformation
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

    print(
        f"Call history for interviewer: {interviewer} between {start_date} and {end_date}"
    )

    if start_date is None or end_date is None:
        print("Invalid request missing required filter properties ")
        return '{"error": "Invalid request missing required filter properties"}', 400

    error, results = get_call_history_records_by_interviewer_and_date_range(
        interviewer, start_date, end_date
    )

    if error:
        message, error_code = error
        return message, error_code

    return jsonify(results)


@app.route("/api/reports/call-pattern/<interviewer>")
def call_pattern(interviewer):
    start_date = request.args.get("start-date", None)
    end_date = request.args.get("end-date", None)

    print(f"Call history for interviewer: {interviewer} between {start_date} and {end_date}")

    if start_date is None or end_date is None:
        print("Invalid request: missing required filter properties")
        return {}

    error, results = get_call_pattern_records_by_interviewer_and_date_range(interviewer, start_date, end_date)

    if error:
        message, code = error
        print(message)
        return {}

    return results.json()


@app.route("/call_history")
def get_cati_db():
    return jsonify(get_call_history(current_app.configuration))


@app.route("/events")
def get_events_cati_db():
    return jsonify(get_events(current_app.configuration))
