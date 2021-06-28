import datetime
import re
from dataclasses import asdict

from google.cloud import datastore


def get_call_history_records():
    client = datastore.Client()
    query = client.query(kind="CallHistory")
    results = list(query.fetch())
    return results


def parse_date_string_to_datetime(date_string, end_of_day=False):
    x = re.search("^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}$", date_string)
    if not x:
        return None

    date_split = date_string.split("-")

    if end_of_day:
        return datetime.datetime(
            int(date_split[0]), int(date_split[1]), int(date_split[2]), 23, 59, 59
        )

    return datetime.datetime(int(date_split[0]), int(date_split[1]), int(date_split[2]))


def get_call_history_records_by_interviewer_and_date_range(
        interviewer_name, start_date_string, end_date_string
):
    start_date = parse_date_string_to_datetime(start_date_string)
    end_date = parse_date_string_to_datetime(end_date_string, True)

    if start_date is None or end_date is None:
        return ("Invalid format for date properties provided", 400), []

    client = datastore.Client()
    query = client.query(kind="CallHistory")
    query.add_filter("interviewer", "=", interviewer_name)
    query.add_filter("call_start_time", ">=", start_date)
    query.add_filter("call_start_time", "<=", end_date)
    query.order = ["call_start_time"]

    results = list(query.fetch())
    print(f"get_call_history_records_by_interviewer_and_date_range - {len(results)} records found")
    return None, results


def update_call_history_report_status():
    client = datastore.Client()

    complete_key = client.key("Status", "call_history")
    task = datastore.Entity(key=complete_key)

    task.update(
        {
            "last_updated": datetime.datetime.utcnow(),
        }
    )

    client.put(task)

    return


def get_call_history_report_status():
    client = datastore.Client()
    key = client.key("Status", "call_history")
    status = client.get(key)
    return status


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
