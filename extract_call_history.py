from data_sources.database import get_call_history, get_mi_call_history
from models.call_history import CallHistory
from models.mi_hub_call_history import MiHubCallHistory


def get_questionnaire_name_from_id(questionnaire_id, questionnaire_list):
    return next(
        (item for item in questionnaire_list if item.get("id") == questionnaire_id),
        {"name": ""},
    ).get("name")


def load_cati_dial_history(config, questionnaire_list):
    results = get_call_history(config)

    call_history_list = []

    for item in results:
        call_history = CallHistory(
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
        questionnaire_name = get_questionnaire_name_from_id(
            call_history.questionnaire_id, questionnaire_list
        )
        if questionnaire_name != "":
            call_history.generate_questionnaire_details(questionnaire_name)
        call_history_list.append(call_history)

    print(
        f"{len(results)} calls in CATI DB - {len(call_history_list)} calls appended to list"
    )

    return call_history_list


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
            seconds_dial=item.get("dial_secs"),
        )
        call_history.generate_dial_date_and_time_fields(item.get("StartTime"), item.get("EndTime"))

        questionnaire_name = get_questionnaire_name_from_id(
            call_history.questionnaire_id, questionnaire_list
        )
        if questionnaire_name != "":
            call_history.questionnaire_name = questionnaire_name
        call_history_list.append(call_history)

    print(
        f"{len(results)} calls in CATI DB - {len(call_history_list)} calls appended to list"
    )

    return call_history_list
