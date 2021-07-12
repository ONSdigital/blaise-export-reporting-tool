import csv
import os
from dataclasses import asdict

from data_sources.blaise_api import load_case_data, get_questionnaire_list
from models.mi_hub_respondent_data import MiHubRespondentData
from storage_and_files.folder_management import (
    clear_tmp_directory,
    get_tmp_directory_path,
    create_folder_in_tmp_directory,
)
from storage_and_files.write_csv import write_list_of_dict_to_csv

csv_columns = [
    "dateTimeStamp",
    "qHousehold.QHHold.Person[1].DVAge",
    "qiD.Serial_Number",
    "qhAdmin.Interviewer[1]",
    "qHousehold.QHHold.Person[1].tmpDoB",
    "questionnaire_name",
    "qDataBag.PostCode",
    "mode",
    "qhAdmin.HOut",
    "qHousehold.QHHold.Person[1].Sex",
]


def extract_mi_hub_respondent_data(config):
    questionnaires = get_questionnaire_list(config)
    tmp_folder = get_tmp_directory_path()

    blaise_fields_to_get = [
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

    for questionnaire in questionnaires:
        questionnaire_name = questionnaire.get("name")

        respondent_data = get_respondent_data_for_questionnaire(
            blaise_fields_to_get, config, questionnaire_name
        )

        create_folder_in_tmp_directory(questionnaire_name)

        csv_file = f"{tmp_folder}/{questionnaire_name}/respondent_data.csv"
        write_list_of_dict_to_csv(csv_file, respondent_data, MiHubRespondentData.fields())


def get_respondent_data_for_questionnaire(
        blaise_fields_to_get, config, questionnaire_name
):
    cases = []
    cases.extend(load_case_data(questionnaire_name, config, blaise_fields_to_get))

    respondent_data = []
    for case in cases:
        respondent = MiHubRespondentData(
            SER_NO=case.get("qiD.Serial_Number"),
            OUTCOME=case.get("qhAdmin.HOut"),
            DATE_COMPLETED=case.get("dateTimeStamp"),
            INT_NAME=case.get("qhAdmin.Interviewer[1]"),
            MODE=case.get("mode"),
            POSTCODE=case.get("qDataBag.PostCode"),
            GENDER=case.get("qHousehold.QHHold.Person[1].Sex"),
            DATE_OF_BIRTH=case.get("qHousehold.QHHold.Person[1].tmpDoB"),
            AGE=case.get("qHousehold.QHHold.Person[1].DVAge"),
        )
        respondent_data.append(respondent)
    return respondent_data
