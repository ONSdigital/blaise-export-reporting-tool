import datetime
from functions.csv_functions import write_csv
import os

from dotenv import load_dotenv

from app.app import app, load_config
from data_sources.datastore_data import (
    get_call_history,
    upload_call_history_to_datastore,
)
from functions.google_storage_functions import init_google_storage
from functions.zip_functions import create_zip, zip_group
from models.config_model import Config
from reports.mi_hub_call_history_report import get_mi_hub_call_history
from reports.mi_hub_respondent_data_report import get_mi_hub_respondent_data


def upload_call_history(_event, _context):
    print("Running Cloud Function - upload_call_history")
    config = Config.from_env()
    config.log()
    call_history = get_call_history(config)
    upload_call_history_to_datastore(call_history)


def deliver_mi_hub_reports(_event, _context):
    print("Running Cloud Function - deliver_mi_hub_reports")
    config = Config.from_env()
    config.log()
    google_storage = init_google_storage(config)
    if google_storage.bucket is None:
        return "Connection to storage bucket failed", 500
    grouped_call_history_reports = get_mi_hub_call_history(config)
    grouped_respondent_data_reports = get_mi_hub_respondent_data(config)

    zip_groups = {}

    for questionnaire, call_history_report in grouped_call_history_reports.items():
        call_history_csv = write_csv(call_history_report)
        zip_files_for_questionnaire = zip_group(zip_groups, questionnaire)
        zip_files_for_questionnaire["call_history.csv"] = call_history_csv
        zip_groups[questionnaire] = zip_files_for_questionnaire

    for (
        questionnaire,
        respondent_data_report,
    ) in grouped_respondent_data_reports.items():
        respondent_data_csv = write_csv(respondent_data_report)
        zip_files_for_questionnaire = zip_group(zip_groups, questionnaire)
        zip_files_for_questionnaire["respondent_data.csv"] = respondent_data_csv
        zip_groups[questionnaire] = zip_files_for_questionnaire

    dt_string = datetime.datetime.now().strftime("%d%m%Y_%H%M%S")
    for questionnaire, zip_files in zip_groups.items():
        zip_file_data = []
        for filename, content in zip_files.items():
            zip_file_data.append({"filename": filename, "content": content})
        zipped = create_zip(zip_file_data)

        mi_filename = f"mi_{questionnaire}_{dt_string}"
        google_storage.upload_zip(f"{mi_filename}.zip", zipped)


if os.path.isfile("./.env"):
    print("Loading environment variables from dotenv file")
    load_dotenv()

load_config(app)

if __name__ == "__main__":
    print("Running Flask application")
    app.run(host="0.0.0.0", port=5011)
