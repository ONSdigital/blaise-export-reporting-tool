import datetime
import os

from dotenv import load_dotenv

from app.app import app, load_config
from functions.call_history_functions import get_call_history, upload_call_history_to_datastore
from functions.folder_functions import (
    get_tmp_directory_path,
    clear_tmp_directory,
    create_tmp_directory
)
from functions.google_storage_functions import init_google_storage, GoogleStorage
from functions.mi_hub_call_history_functions import get_mi_hub_call_history
from functions.mi_hub_respondent_data_functions import get_mi_hub_respondent_data
from functions.zip_functions import create_zip
from models.config import Config


def upload_call_history(_event, _context):
    config = Config.from_env()
    config.log()
    upload_call_history_to_datastore(get_call_history(config))


def mi_hub_call_history():
    config = Config.from_env()
    config.log()
    get_mi_hub_call_history(config)


def mi_hub_respondent_data():
    config = Config.from_env()
    config.log()
    get_mi_hub_respondent_data(config)


def deliver_mi_hub_reports(_event, _context):
    config = Config.from_env()
    config.log()
    google_storage = init_google_storage(config)
    if google_storage.bucket is None:
        return "Connection to storage bucket failed", 500
    create_tmp_directory()
    clear_tmp_directory()
    mi_hub_call_history()
    mi_hub_respondent_data()
    tmp_folder = get_tmp_directory_path()
    questionnaires = [x for x in os.listdir(tmp_folder)]
    dt_string = datetime.datetime.now().strftime("%d%m%Y_%H%M%S")
    mi_zip_files = []
    for questionnaire in questionnaires:
        mi_filename = f"mi_{questionnaire}_{dt_string}"
        create_zip(os.path.join(tmp_folder, questionnaire), f"{tmp_folder}/{mi_filename}")
        mi_zip_files.append(f"{mi_filename}.zip")
    for mi_zip_file in mi_zip_files:
        GoogleStorage.upload_file(google_storage, source=os.path.join(tmp_folder, mi_zip_file), dest=mi_zip_file)


if os.path.isfile("./.env"):
    print("Loading environment variables from dotenv file")
    load_dotenv()

load_config(app)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5011)
