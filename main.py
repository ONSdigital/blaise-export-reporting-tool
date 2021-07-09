import os
import datetime

from app.app import app, load_config
from extract_mi_hub_respondent_data import extract_mi_hub_respondent_data
from extract_mi_hub_call_history import extract_mi_hub_call_history
from import_call_history import import_call_history_data
from models.config import Config
from upload_call_history import add_call_history_to_datastore
from storage_and_files.zip_management import prepare_zip
from storage_and_files.folder_management import (
    clear_tmp_directory,
    get_tmp_directory_path,
    create_folder_in_tmp_directory,
    create_tmp_directory
)

# os.environ["GCLOUD_PROJECT"] = "ons-blaise-v2-dev-<blah>"


def upload_call_history(_event, _context):
    config = Config.from_env()
    config.log()

    merged_call_history = import_call_history_data(config)

    status = add_call_history_to_datastore(merged_call_history)
    print(status)

    return status


def mi_hub_call_history():
    config = Config.from_env()
    config.log()

    merged_call_history = extract_mi_hub_call_history(config)

    print(merged_call_history)

    return merged_call_history


def mi_hub_respondent_data():
    config = Config.from_env()
    config.log()

    extract_mi_hub_respondent_data(config)


def deliver_mi_hub_reports(_event, _context):
    clear_tmp_directory()
    create_tmp_directory()

    mi_hub_call_history()
    mi_hub_respondent_data()

    dirname = os.path.dirname(__file__)
    tmp_folder = os.path.join(dirname, 'tmp')
    questionnaires = [x for x in os.listdir(tmp_folder) if x.startswith("OPN")]
    print("")
    dt_string = datetime.datetime.now().strftime("%d%m%Y_%H%M%S")
    # questionnaire = "LMS2101_AA1"
    # dd_LMS2101_AA1_04052021_105226.zip
    mi_zip_files = []
    for questionnaire in questionnaires:
        mi_filename = f"mi_{questionnaire}_{dt_string}"
        prepare_zip(os.path.join(tmp_folder, questionnaire), f"tmp/{mi_filename}")
        mi_zip_files.append(f"{mi_filename}.zip")


load_config(app)

if __name__ == "__main__":

    # deliver_mi_hub_reports(None, None)

    app.run(host="0.0.0.0", port=5011)
