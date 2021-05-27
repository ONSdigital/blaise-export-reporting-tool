import datetime
import re
from dataclasses import asdict

from google.cloud import datastore


def get_call_history_records():
    client = datastore.Client()
    query = client.query(kind="CallHistory")
    results = list(query.fetch())
    return results


def date_string_to_datetime(date_string, end_of_day=False):
    x = re.search("^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}$", date_string)
    if not x:
        return None

    date_split = date_string.split('-')

    if end_of_day:
        return datetime.datetime(int(date_split[0]), int(date_split[1]), int(date_split[2]), 23, 59, 59)

    return datetime.datetime(int(date_split[0]), int(date_split[1]), int(date_split[2]))


def get_call_history_records_by_interviewer(interviewer_name, start_date_string, end_date_string):
    start_date = date_string_to_datetime(start_date_string)
    end_date = date_string_to_datetime(end_date_string, True)

    if start_date is None or end_date is None:
        return 400, []

    client = datastore.Client()
    query = client.query(kind="CallHistory")
    query.add_filter("interviewer", "=", interviewer_name)
    query.add_filter("call_start_time", ">=", start_date)
    query.add_filter("call_start_time", "<=", end_date)
    query.order = ["call_start_time"]

    results = list(query.fetch())
    print(f"Cases found {len(results)}")
    return None, results


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
