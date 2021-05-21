from flask import Flask, jsonify

from data_sources.database import get_events
from data_sources.datastore import get_call_history_records, get_call_history_records_by_interviewer
from upload_call_history import add_call_history_to_datastore
from extract_call_history import get_call_history
from import_call_history import import_call_history_data
from models.config import Config

app = Flask(__name__)

config = Config.from_env()
config.log()


@app.route("/upload_call_history")
def upload_call_history():
    merged_call_history = import_call_history_data(config)

    status = add_call_history_to_datastore(merged_call_history)
    print(status)

    return status


@app.route("/")
def get_all():
    records = get_call_history_records()

    return jsonify(records)


@app.route("/find")
def find():
    results = get_call_history_records_by_interviewer("matpal")

    return jsonify(results)


@app.route("/call_history")
def get_cati_db():
    return jsonify(get_call_history(config))


@app.route("/events")
def get_events_cati_db():
    return jsonify(get_events(config))


if __name__ == "__main__":
    app.run(port=5011)
