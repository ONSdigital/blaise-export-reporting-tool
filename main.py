from dataclasses import asdict

from flask import Flask, jsonify
from google.cloud import datastore

from extract_call_history import get_call_history, get_events
from extract import import_call_history_data
from models.config import Config

app = Flask(__name__)

config = Config.from_env()
config.log()


@app.route("/upload_call_history")
def upload_call_history():
    client = datastore.Client()

    merged_call_history = import_call_history_data(config)
    task_batch = []

    call_history_batches = [
        merged_call_history[i : i + 400]
        for i in range(0, len(merged_call_history), 400)
    ]

    for call_history_batch in call_history_batches:
        for call_history in call_history_batch:
            task1 = datastore.Entity(
                client.key(
                    "CallHistory",
                    f"{call_history.serial_number}-{call_history.call_start_time}",
                )
            )

            task1.update(asdict(call_history))

            task_batch.append(task1)

        client.put_multi(task_batch)
        task_batch = []

    status = f"Uploaded {len(merged_call_history)} call history items"
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
    query.add_filter("interviewer", "=", "Edwin")

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
