import datetime
import os
import json

from dotenv import load_dotenv
from google.cloud import datastore
from google.cloud import tasks_v2

from app.app import app, load_config, setup_app
from data_sources.call_history_data import (CallHistoryClient)
from functions.csv_functions import write_csv
from functions.google_storage_functions import init_google_storage
from functions.zip_functions import create_zip
from models.config_model import Config
from reports.mi_hub_call_history_report import get_mi_hub_call_history
from reports.mi_hub_respondent_data_report import get_mi_hub_respondent_data
from data_sources.questionnaire_data import get_list_of_installed_questionnaires


def delete_old_call_history(_event, _context):
    print("Running Cloud Function - delete_call_history")
    datastore_client = datastore.Client()
    call_history_client = CallHistoryClient(datastore_client)
    print("Running Cloud Function - delete_old_call_history")
    call_history_client.delete_historical_call_history()


def upload_call_history(_event, _context):
    print("Running Cloud Function - upload_call_history")
    config = Config.from_env()
    config.log()
    datastore_client = datastore.Client()
    call_history_client = CallHistoryClient(datastore_client, config)
    call_history_client.call_history_extraction_process()


    # Loop through installed questionnnnaires
    # For each questionnnnaire
    # Get mi hub call history
    # Get mi hub respondent data
    # Create zip
    # Upload to bucket


def deliver_mi_hub_reports_trigger(_event, _context):
    print("Running Cloud Function - deliver_mi_hub_reports_trigger")
    config = Config.from_env()
    config.log()

    project = "ons-blaise-v2-dev-rr3"  # tf !?
    region = "europe-west2"  # tf !?

    installed_questionnaire_list = get_list_of_installed_questionnaires(config)

    for questionnaire in installed_questionnaire_list:
        print(f"Sending request to deliver_mi_hub_reports for {questionnaire.get('name')} {questionnaire.get('id')}")
        task_client = tasks_v2.CloudTasksClient()
        request = tasks_v2.CreateTaskRequest(
            parent=f"projects/{project}/locations/{region}/queues/bert-deliver-mi-hub-reports",
            task=tasks_v2.Task(
                name=f"projects/{project}/locations/{region}/queues/bert-deliver-mi-hub-reports/tasks/{questionnaire.get('name')}",
                http_request=tasks_v2.HttpRequest(
                    http_method="POST",
                    url=f"https://{region}-{project}.cloudfunctions.net/deliver_mi_hub_reports",
                    body=json.dumps(questionnaire.get('name'), questionnaire.get('id')).encode(),
                    headers={
                        "Content-Type": "application/json",
                        },
                ),
                dispatch_deadline=10000,  # !?
            ),
        )
        task_client.create_task(request)


def deliver_mi_hub_reports(_event, _context, questionnaire_name, questionnaire_id):
    print("Running Cloud Function - deliver_mi_hub_reports")
    config = Config.from_env()
    config.log()

    mi_hub_call_history = get_mi_hub_call_history(config, questionnaire_name, questionnaire_id)
    print(mi_hub_call_history)


"""
def deliver_mi_hub_reports(_event, _context):
    print("Running Cloud Function - deliver_mi_hub_reports")
    config = Config.from_env()
    config.log()
    google_storage = init_google_storage(config)
    if google_storage.bucket is None:
        return "Connection to storage bucket failed", 500
    grouped_call_history_reports = get_mi_hub_call_history(config)
    print(grouped_call_history_reports)
    grouped_respondent_data_reports = get_mi_hub_respondent_data(config)

    zip_data_grouped_by_questionnaire = {}

    for questionnaire, call_history_report in grouped_call_history_reports.items():
        call_history_csv = write_csv(call_history_report)
        files_for_questionnaire_zip = zip_data_grouped_by_questionnaire.get(
            questionnaire, {}
        )
        files_for_questionnaire_zip["call_history.csv"] = call_history_csv
        zip_data_grouped_by_questionnaire[questionnaire] = files_for_questionnaire_zip

    for (
            questionnaire,
            respondent_data_report,
    ) in grouped_respondent_data_reports.items():
        respondent_data_csv = write_csv(respondent_data_report)
        files_for_questionnaire_zip = zip_data_grouped_by_questionnaire.get(
            questionnaire, {}
        )
        files_for_questionnaire_zip["respondent_data.csv"] = respondent_data_csv
        zip_data_grouped_by_questionnaire[questionnaire] = files_for_questionnaire_zip

    datetime_string = datetime.datetime.now().strftime("%d%m%Y_%H%M%S")
    for questionnaire, files in zip_data_grouped_by_questionnaire.items():
        zip_file_data = []
        for filename, content in files.items():
            zip_file_data.append({"filename": filename, "content": content})
        zipped = create_zip(zip_file_data)

        mi_filename = f"mi_{questionnaire}_{datetime_string}"
        google_storage.upload_zip(f"{mi_filename}.zip", zipped)
"""


if os.path.isfile("./.env"):
    print("Loading environment variables from dotenv file")
    load_dotenv()

load_config(app)
setup_app(app)

if __name__ == "__main__":
    print("Running Flask application")
    app.run(host="0.0.0.0", port=5011)
