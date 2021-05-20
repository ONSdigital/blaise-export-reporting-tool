from dataclasses import asdict

from google.cloud import datastore


def add_call_history_to_datastore(merged_call_history, task_batch):
    client = datastore.Client()

    new_call_history_entries = filter_out_existing_records(client, merged_call_history)

    print(
        f"{len(merged_call_history)} call history items found of which {len(new_call_history_entries)} are new entries"
    )

    if len(new_call_history_entries) == 0:
        print("No new records to add, Existing.")
        return

    call_history_batches = split_into_batches(new_call_history_entries, 500)
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

    return f"Uploaded {len(new_call_history_entries)} call history items"


def split_into_batches(merged_call_history, length):
    return [
        merged_call_history[i : i + length]
        for i in range(0, len(merged_call_history), length)
    ]


def does_record_already_exist(call_history, case_list_to_query):
    case_data = [
        case
        for case in case_list_to_query
        if f"{call_history.serial_number}-{call_history.call_start_time}" == case
    ]
    print(case_data)
    if len(case_data) != 1:
        return False
    return True


def filter_out_existing_records(client, merged_call_history):
    query = client.query(kind="CallHistory")
    query.keys_only()
    current_call_history = list([entity.key.id_or_name for entity in query.fetch()])

    return [
        call_history
        for call_history in merged_call_history
        if (
            does_record_already_exist(
                call_history,
                current_call_history,
            )
            is False
        )
    ]
