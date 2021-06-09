from data_sources.database import get_call_history, get_mi_call_history
from models.call_history import CallHistory
from models.mi_call_history import MICallHistory


def get_questionnaire_name_from_id(questionnaire_id, questionnaire_list):
    return next(
        (item for item in questionnaire_list if item.get("id") == questionnaire_id),
        {"name": ""},
    ).get("name")


def load_cati_dial_history(config, questionnaire_list):
    results = get_call_history(config)

    call_history_list = []

    for item in results:
        call_history = CallHistory(*item)
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


def load_mi_cati_dial_history(config, questionnaire_list):
    results = get_mi_call_history(config)

    call_history_list = []

    for item in results:
        call_history = MICallHistory(
            questionnaire_id=item[0],
            serial_number=item[1],
            internal_key=item[2],
            call_number=item[4],
            dial_number=item[5],
            interviewer=item[6],
            dial_result=item[7],
            dial_line_number=item[8],
            seconds_dial=item[11],
        )
        call_history.generate_dial_date_and_time_fields(item[3], item[10])

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
