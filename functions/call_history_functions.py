from data_sources.blaise_api import get_list_of_installed_instruments, get_instrument_data
from data_sources.cati_database import get_cati_call_history_from_database, get_cati_mi_hub_call_history_from_database
from data_sources.datastore import (
    get_call_history_keys,
    bulk_upload_call_history,
    update_call_history_report_status,
)
from models.call_history import CallHistory
from models.mi_hub_call_history import MiHubCallHistory


def get_call_history(config):
    print("Getting call history data")
    installed_instrument_list = get_list_of_installed_instruments(config)
    cati_call_history = get_cati_call_history(config, installed_instrument_list)
    instrument_fields_to_get = [
        "QID.Serial_Number",
        "QHAdmin.HOut",
        "QHousehold.QHHold.HHSize",
    ]
    instrument_data = []
    for instrument in installed_instrument_list:
        instrument_data.extend(get_instrument_data(instrument.get("name"), config, instrument_fields_to_get))
    print(f"Found {len(instrument_data)} instrument records")
    cati_call_history_and_instrument_data_merged = merge_cati_call_history_and_instrument_data(
        cati_call_history, instrument_data)
    print(f"Merged cati call history and instrument data")
    return cati_call_history_and_instrument_data_merged


def get_cati_call_history(config, instrument_list):
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
        instrument_name = get_instrument_name_from_id(cati_call_history.questionnaire_id, instrument_list)
        if instrument_name != "":
            cati_call_history.generate_instrument_details(instrument_name)
        cati_call_history_list.append(cati_call_history)
    print(f"Found {len(results)} call history records in the CATI database")
    return cati_call_history_list


def get_cati_mi_hub_call_history(config, instrument_list):
    results = get_cati_mi_hub_call_history_from_database(config)
    cati_mi_hub_call_history_list = []
    for item in results:
        cati_mi_hub_call_history = MiHubCallHistory(
            questionnaire_id=item.get("InstrumentId"),
            serial_number=item.get("PrimaryKeyValue"),
            call_number=item.get("CallNumber"),
            dial_number=item.get("DialNumber"),
            interviewer=item.get("Interviewer"),
            dial_result=item.get("DialResult"),
            dial_line_number=item.get("DialedNumber"),
            seconds_interview=item.get("dial_secs")
        )
        cati_mi_hub_call_history.generate_dial_date_and_time_fields(item.get("StartTime"), item.get("EndTime"))
        instrument_name = get_instrument_name_from_id(cati_mi_hub_call_history.questionnaire_id, instrument_list)
        if instrument_name != "":
            cati_mi_hub_call_history.questionnaire_name = instrument_name
        cati_mi_hub_call_history_list.append(cati_mi_hub_call_history)
    print(f"Found {len(results)} mi hub call history records in the CATI database")
    return cati_mi_hub_call_history_list


def get_instrument_name_from_id(instrument_id, instrument_list):
    return next(
        (item for item in instrument_list if item.get("id") == instrument_id),
        {"name": ""},
    ).get("name")


def merge_cati_call_history_and_instrument_data(cati_call_history, instrument_data):
    for record in cati_call_history:
        matched_record = match_cati_call_history_and_instrument_data(
            record.serial_number, record.questionnaire_name, instrument_data)
        if matched_record is not None:
            record.number_of_interviews = matched_record.get("qHousehold.QHHold.HHSize", "")
            record.outcome_code = matched_record.get("qhAdmin.HOut", "")
    return cati_call_history


def match_cati_call_history_and_instrument_data(serial_number, instrument_name, instrument_data):
    matched_record = [
        record
        for record in instrument_data
        if record.get("qiD.Serial_Number") == serial_number and record["questionnaire_name"] == instrument_name
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
