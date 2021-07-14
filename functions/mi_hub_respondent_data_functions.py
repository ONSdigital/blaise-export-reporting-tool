from data_sources.blaise_api import get_instrument_data, get_list_of_installed_instruments
from functions.csv_functions import write_list_of_dicts_to_csv
from functions.folder_functions import (
    get_tmp_directory_path,
    create_instrument_name_folder_in_tmp_directory,
)
from models.mi_hub_respondent_data import MiHubRespondentData


def get_mi_hub_respondent_data(config):
    print("Getting data for the MI hub respondent data report")
    installed_instrument_list = get_list_of_installed_instruments(config)
    instrument_fields_to_get = [
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
    for instrument in installed_instrument_list:
        instrument_name = instrument.get("name")
        mi_hub_respondent_data = get_mi_hub_respondent_data_for_instrument(
            instrument_fields_to_get, config, instrument_name
        )
        create_instrument_name_folder_in_tmp_directory(instrument_name)
        tmp_folder = get_tmp_directory_path()
        csv_file = f"{tmp_folder}/{instrument_name}/respondent_data.csv"
        write_list_of_dicts_to_csv(csv_file, mi_hub_respondent_data, MiHubRespondentData.fields())


def get_mi_hub_respondent_data_for_instrument(blaise_fields_to_get, config, questionnaire_name):
    records = []
    records.extend(get_instrument_data(questionnaire_name, config, blaise_fields_to_get))
    mi_hub_respondent_data = []
    for record in records:
        mi_hub_respondent_data_record = MiHubRespondentData(
            SER_NO=record.get("qiD.Serial_Number"),
            OUTCOME=record.get("qhAdmin.HOut"),
            DATE_COMPLETED=record.get("dateTimeStamp"),
            INT_NAME=record.get("qhAdmin.Interviewer[1]"),
            MODE=record.get("mode"),
            POSTCODE=record.get("qDataBag.PostCode"),
            GENDER=record.get("qHousehold.QHHold.Person[1].Sex"),
            DATE_OF_BIRTH=record.get("qHousehold.QHHold.Person[1].tmpDoB"),
            AGE=record.get("qHousehold.QHHold.Person[1].DVAge"),
        )
        mi_hub_respondent_data.append(mi_hub_respondent_data_record)
    return mi_hub_respondent_data
