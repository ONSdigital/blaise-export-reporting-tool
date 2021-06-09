from data_sources.datastore import (
    get_call_history_keys,
    bulk_upload_call_history,
    update_call_history_report_status,
)


def add_call_history_to_datastore(merged_call_history):
    new_call_history_entries = filter_out_existing_records(merged_call_history)

    print(
        f"{len(merged_call_history)} call history items found of which {len(new_call_history_entries)} are new entries"
    )

    if len(new_call_history_entries) == 0:
        print("No new records to add, Existing.")
        return "No new records to add, Existing."

    bulk_upload_call_history(new_call_history_entries)
    update_call_history_report_status()

    return f"Uploaded {len(new_call_history_entries)} call history records"


def record_already_exists(call_history, case_list_to_query):
    case_data = [
        case
        for case in case_list_to_query
        if f"{call_history.serial_number}-{call_history.call_start_time}" == case
    ]
    if len(case_data) != 1:
        return False
    return True


def filter_out_existing_records(merged_call_history):
    current_call_history = get_call_history_keys()

    return [
        call_history_record
        for call_history_record in merged_call_history
        if (
            record_already_exists(
                call_history_record,
                current_call_history,
            )
            is False
        )
    ]
