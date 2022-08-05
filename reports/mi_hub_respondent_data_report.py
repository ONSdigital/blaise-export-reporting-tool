from data_sources.questionnaire_data import get_questionnaire_data
from models.mi_hub_respondent_data_model import MiHubRespondentData


questionnaire_fields_to_get = [
    {
        "QID.Serial_Number",
        "QHAdmin.HOut",
        "QHAdmin.Interviewer[1]",
        "Mode",
        "QDataBag.PostCode",
        "QHousehold.QHHold.Person[1].Sex",
        "QHousehold.QHHold.Person[1].tmpDoB",
        "QHousehold.QHHold.Person[1].DVAge",
        "DateTimeStamp",
    }
]


def get_mi_hub_respondent_data(config, questionnaire_name):
    print(f"Getting MI hub respondent data report data for {questionnaire_name}")
    mi_hub_respondent_data_list = []
    questionnaire_data = get_questionnaire_data(
        questionnaire_name, config, questionnaire_fields_to_get
    )
    for item in questionnaire_data:
        mi_hub_respondent_data_record = MiHubRespondentData(
            serial_number=item.get("qiD.Serial_Number"),
            outcome_code=item.get("qhAdmin.HOut"),
            date_completed=item.get("dateTimeStamp"),
            interviewer=item.get("qhAdmin.Interviewer[1]"),
            mode=item.get("mode"),
            postcode=item.get("qDataBag.PostCode"),
            gender=item.get("qHousehold.QHHold.Person[1].Sex"),
            date_of_birth=item.get("qHousehold.QHHold.Person[1].tmpDoB"),
            age=item.get("qHousehold.QHHold.Person[1].DVAge"),
        )
        mi_hub_respondent_data_list.append(mi_hub_respondent_data_record)
    return mi_hub_respondent_data_list
