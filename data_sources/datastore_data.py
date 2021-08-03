from dataclasses import asdict
from datetime import datetime

from google.cloud import datastore

from data_sources.cati_data import get_cati_call_history_from_database
from data_sources.questionnaire_data import get_list_of_installed_questionnaires, get_questionnaire_data, \
    get_questionnaire_name_from_id
from models.call_history_model import CallHistory


def get_call_history(config):
    print("Getting call history data")
    installed_questionnaire_list = get_list_of_installed_questionnaires(config)
    cati_call_history = get_cati_call_history(config, installed_questionnaire_list)
    questionnaire_fields_to_get = [
        "QID.Serial_Number",
        "QHAdmin.HOut",
        "QHousehold.QHHold.HHSize",
    ]
    questionnaire_data = []
    for questionnaire in installed_questionnaire_list:
        questionnaire_data.extend(
            get_questionnaire_data(questionnaire.get("name"), config, questionnaire_fields_to_get))
    print(f"Found {len(questionnaire_data)} questionnaire records")
    cati_call_history_and_questionnaire_data_merged = merge_cati_call_history_and_questionnaire_data(
        cati_call_history, questionnaire_data)
    print("Merged cati call history and questionnaire data")
    return cati_call_history_and_questionnaire_data_merged


def get_cati_call_history(config, questionnaire_list):
    results = get_cati_call_history_from_database(config)
    cati_call_history_list = []
    for item in results:
        cati_call_history = CallHistory(
            questionnaire_id=item.get("InstrumentId"),
            serial_number=item.get("PrimaryKeyValue"),
            call_number=item.get("CallNumber"),
            dial_number=item.get("DialNumber"),
            busy_dials=item.get("BusyDials"),
            call_start_time=item.get("StartTime"),
            call_end_time=item.get("EndTime"),
            dial_secs=item.get("dial_secs"),
            status=item.get("Status"),
            interviewer=item.get("Interviewer"),
            call_result=item.get("DialResult"),
            update_info=item.get("UpdateInfo"),
            appointment_info=item.get("AppointmentInfo"),
        )
        questionnaire_name = get_questionnaire_name_from_id(cati_call_history.questionnaire_id, questionnaire_list)
        if questionnaire_name != "":
            cati_call_history.generate_questionnaire_details(questionnaire_name)
        cati_call_history_list.append(cati_call_history)
    print(f"Found {len(results)} call history records in the CATI database")
    return cati_call_history_list


def merge_cati_call_history_and_questionnaire_data(cati_call_history, questionnaire_data):
    for record in cati_call_history:
        matched_record = match_cati_call_history_and_questionnaire_data(
            record.serial_number, record.questionnaire_name, questionnaire_data)
        if matched_record is not None:
            record.number_of_interviews = matched_record.get("qHousehold.QHHold.HHSize", "")
            record.outcome_code = matched_record.get("qhAdmin.HOut", "")
    return cati_call_history


def match_cati_call_history_and_questionnaire_data(serial_number, questionnaire_name, questionnaire_data):
    matched_record = [
        record
        for record in questionnaire_data
        if record.get("qiD.Serial_Number") == serial_number and record["questionnaire_name"] == questionnaire_name
    ]
    if len(matched_record) > 0:
        return matched_record[0]
    else:
        return None


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


def get_call_history_keys():
    client = datastore.Client()
    query = client.query(kind="CallHistory")
    query.keys_only()
    current_call_history = list([entity.key.id_or_name for entity in query.fetch()])
    return current_call_history


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


def update_call_history_report_status():
    client = datastore.Client()
    complete_key = client.key("Status", "call_history")
    task = datastore.Entity(key=complete_key)
    task.update(
        {
            "last_updated": datetime.utcnow(),
        }
    )
    client.put(task)
    return


def get_call_history_report_status():
    client = datastore.Client()
    key = client.key("Status", "call_history")
    status = client.get(key)
    return status
