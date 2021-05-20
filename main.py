from dataclasses import asdict

from flask import Flask, jsonify
from google.cloud import datastore

from database import get_events
from datastore import add_call_history_to_datastore
from extract_call_history import get_call_history
from import_call_history import import_call_history_data
from models.config import Config

app = Flask(__name__)

config = Config.from_env()
config.log()


@app.route("/upload_call_history")
def upload_call_history():
    merged_call_history = import_call_history_data(config)
    task_batch = []

    status = add_call_history_to_datastore(merged_call_history, task_batch)
    print(status)

    return status


@app.route("/")
def get_all():
    client = datastore.Client()

    query = client.query(kind="CallHistory")

    results = list(query.fetch())

    return jsonify(results)


@app.route("/find")
def find():
    client = datastore.Client()

    query = client.query(kind="CallHistory")
    query.add_filter("interviewer", "=", "matpal")

    results = list(query.fetch())

    return jsonify(results)


@app.route("/call_history")
def get_cati_db():
    return jsonify(get_call_history(config))


@app.route("/events")
def get_events_cati_db():
    return jsonify(get_events(config))


if __name__ == "__main__":
    app.run(port=5011)
