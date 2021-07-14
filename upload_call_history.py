from data_sources.datastore import (
    get_call_history_keys,
    bulk_upload_call_history,
    update_call_history_report_status,
)


def upload_call_history_to_datastore(call_history_data):
    print("Checking for new call history records to upload to datastore")
    new_call_history_records = filter_out_existing_call_history_records(call_history_data)
    if len(new_call_history_records) == 0:
        print("No new call history records to upload to datastore")
    else:
        bulk_upload_call_history(new_call_history_records)
        print(f"Uploaded {len(new_call_history_records)} new call history records to datastore")
    update_call_history_report_status()


def filter_out_existing_call_history_records(call_history_data):
    current_call_history_in_datastore = get_call_history_keys()
    return [
        call_history_record
        for call_history_record in call_history_data
        if check_if_call_history_record_already_exists(call_history_record, current_call_history_in_datastore) is False
    ]


def check_if_call_history_record_already_exists(call_history_record, current_call_history_in_datastore):
    existing_record = [
        record
        for record in current_call_history_in_datastore
        if f"{call_history_record.serial_number}-{call_history_record.call_start_time}" == record
    ]
    if len(existing_record) > 0:
        return True
    else:
        return False
