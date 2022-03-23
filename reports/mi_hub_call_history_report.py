from data_sources.cati_data import get_cati_mi_hub_call_history_from_database
from models.mi_hub_call_history_model import MiHubCallHistory


def get_mi_hub_call_history(config, questionnaire_name, questionnaire_id):
    print("Getting data for the MI hub call history report")
    cati_data = get_cati_mi_hub_call_history_from_database(config)
    cati_mi_hub_call_history_list = []
    for item in cati_data:
        if item.get("InstrumentId") == questionnaire_id:
            cati_mi_hub_call_history = MiHubCallHistory(
            questionnaire_id=item.get("InstrumentId"),
            serial_number=item.get("PrimaryKeyValue"),
            call_number=item.get("CallNumber"),
            dial_number=item.get("DialNumber"),
            interviewer=item.get("Interviewer"),
            dial_result=item.get("DialResult"),
            dial_line_number=item.get("DialedNumber"),
            seconds_interview=item.get("dial_secs"),
            outcome_code=item.get("OutcomeCode"),
        )
        cati_mi_hub_call_history.generate_dial_date_and_time_fields(
            item.get("StartTime"), item.get("EndTime")
        )
        cati_mi_hub_call_history.questionnaire_name = questionnaire_name
        cati_mi_hub_call_history_list.append(cati_mi_hub_call_history)
    return cati_mi_hub_call_history_list
