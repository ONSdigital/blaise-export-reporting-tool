from data_sources.database import get_cati_call_history_from_database, get_mi_hub_call_history
from models.call_history import CallHistory
from models.mi_hub_call_history import MiHubCallHistory


def get_instrument_name_from_id(instrument_id, instrument_list):
    return next(
        (item for item in instrument_list if item.get("id") == instrument_id),
        {"name": ""},
    ).get("name")


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


def load_mi_hub_cati_dial_history(config, questionnaire_list):
    results = get_mi_hub_call_history(config)

    call_history_list = []

    for item in results:
        call_history = MiHubCallHistory(
            questionnaire_id=item.get("InstrumentId"),
            serial_number=item.get("PrimaryKeyValue"),
            call_number=item.get("CallNumber"),
            dial_number=item.get("DialNumber"),
            interviewer=item.get("Interviewer"),
            dial_result=item.get("DialResult"),
            dial_line_number=item.get("DialedNumber"),
            seconds_interview=item.get("dial_secs")
        )
        call_history.generate_dial_date_and_time_fields(item.get("StartTime"), item.get("EndTime"))
        questionnaire_name = get_instrument_name_from_id(
            call_history.questionnaire_id, questionnaire_list
        )
        if questionnaire_name != "":
            call_history.questionnaire_name = questionnaire_name
        call_history_list.append(call_history)

    print(
        f"{len(results)} calls in CATI DB - {len(call_history_list)} calls appended to list"
    )

    return call_history_list
