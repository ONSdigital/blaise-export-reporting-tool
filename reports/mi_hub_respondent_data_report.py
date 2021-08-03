from data_sources.questionnaire_data import get_questionnaire_data, get_list_of_installed_questionnaires
from functions.csv_functions import write_list_of_dicts_to_csv
from functions.folder_functions import (
    get_tmp_directory_path,
    create_questionnaire_name_folder_in_tmp_directory,
)
from models.mi_hub_respondent_data_model import MiHubRespondentData


def get_mi_hub_respondent_data(config):
    print("Getting data for the MI hub respondent data report")
    installed_questionnaire_list = get_list_of_installed_questionnaires(config)
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
    for questionnaire in installed_questionnaire_list:
        questionnaire_name = questionnaire.get("name")
        mi_hub_respondent_data = get_mi_hub_respondent_data_for_questionnaire(
            questionnaire_fields_to_get, config, questionnaire_name
        )
        create_questionnaire_name_folder_in_tmp_directory(questionnaire_name)
        tmp_folder = get_tmp_directory_path()
        csv_file = f"{tmp_folder}/{questionnaire_name}/respondent_data.csv"
        write_list_of_dicts_to_csv(csv_file, mi_hub_respondent_data, MiHubRespondentData.fields())


def get_mi_hub_respondent_data_for_questionnaire(blaise_fields_to_get, config, questionnaire_name):
    records = []
    records.extend(get_questionnaire_data(questionnaire_name, config, blaise_fields_to_get))
    mi_hub_respondent_data = []
    for record in records:
        mi_hub_respondent_data_record = MiHubRespondentData(
            serial_number=record.get("qiD.Serial_Number"),
            outcome_code=record.get("qhAdmin.HOut"),
            date_completed=record.get("dateTimeStamp"),
            interviewer=record.get("qhAdmin.Interviewer[1]"),
            mode=record.get("mode"),
            postcode=record.get("qDataBag.PostCode"),
            gender=record.get("qHousehold.QHHold.Person[1].Sex"),
            date_of_birth=record.get("qHousehold.QHHold.Person[1].tmpDoB"),
            age=record.get("qHousehold.QHHold.Person[1].DVAge"),
        )
        mi_hub_respondent_data.append(mi_hub_respondent_data_record)
    return mi_hub_respondent_data
