import datetime
from dataclasses import asdict

from google.cloud import datastore


def get_call_history_records():
    client = datastore.Client()
    query = client.query(kind="CallHistory")
    results = list(query.fetch())
    return results


def get_call_history_records_by_interviewer(interviewer_name):
    start_date = datetime.datetime(2021, 5, 19)
    end_date = datetime.datetime(2021, 5, 19, 23, 59, 59)

    client = datastore.Client()
    query = client.query(kind="CallHistory")
    query.add_filter("interviewer", "=", interviewer_name)
    query.add_filter("call_start_time", ">=", start_date)
    query.add_filter("call_start_time", "<=", end_date)
    query.order = ["call_start_time"]

    results = list(query.fetch())
    print(f"Cases found {len(results)}")
    return results


def get_call_history_keys():
    client = datastore.Client()
    query = client.query(kind="CallHistory")
    query.keys_only()
    current_call_history = list([entity.key.id_or_name for entity in query.fetch()])
    return current_call_history


def bulk_upload_call_history(new_call_history_entries):
    client = datastore.Client()

    datastore_tasks = []

    for call_history_record in new_call_history_entries:
        task1 = datastore.Entity(
            client.key(
                "CallHistory",
                f"{call_history_record.serial_number}-{call_history_record.call_start_time}",
            )
        )

        task1.update(asdict(call_history_record))

        datastore_tasks.append(task1)

    datastore_batches = split_into_batches(datastore_tasks, 500)

    for batch in datastore_batches:
        client.put_multi(batch)


def split_into_batches(merged_call_history, length):
    return [
        merged_call_history[i: i + length]
        for i in range(0, len(merged_call_history), length)
    ]
