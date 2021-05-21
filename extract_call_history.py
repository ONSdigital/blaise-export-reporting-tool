from data_sources.database import get_call_history
from models.call_history import CallHistory


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
        if questionnaire_name is not "":
            call_history.generate_questionnaire_details(questionnaire_name)
        call_history_list.append(call_history)

    print(
        f"{len(results)} calls in CATI DB - {len(call_history_list)} calls appended to list"
    )

    return call_history_list
