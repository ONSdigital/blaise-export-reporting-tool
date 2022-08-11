import datetime
import json
import os
import uuid

from dotenv import load_dotenv
from google.cloud import datastore, tasks_v2

from app.app import app, load_config, setup_app
from data_sources.call_history_data import CallHistoryClient
from data_sources.questionnaire_data import get_list_of_installed_questionnaires
from functions.csv_functions import write_csv
from functions.google_storage_functions import init_google_storage
from functions.zip_functions import create_zip
from models.config_model import Config
from reports.mi_hub_call_history_report import get_mi_hub_call_history
from reports.mi_hub_respondent_data_report import get_mi_hub_respondent_data


def delete_old_call_history(_event, _context):
    print("Running Cloud Function - delete_old_call_history")
    datastore_client = datastore.Client()
    call_history_client = CallHistoryClient(datastore_client)
    call_history_client.delete_historical_call_history()


def upload_call_history(_event, _context):
    print("Running Cloud Function - upload_call_history")
    config = Config.from_env()
    config.log()
    datastore_client = datastore.Client()
    call_history_client = CallHistoryClient(datastore_client, config)
    call_history_client.call_history_extraction_process()


def deliver_mi_hub_reports_trigger(_event, _context):
    print("Running Cloud Function - deliver_mi_hub_reports_trigger")
    config = Config.from_env()
    config.log()
    installed_questionnaire_list = get_list_of_installed_questionnaires(config)
    task_client = tasks_v2.CloudTasksClient()
    for questionnaire in installed_questionnaire_list:
        print(
            f"Sending request to deliver_mi_hub_reports_processor for {questionnaire.get('name')} {questionnaire.get('id')}"
        )
        request = tasks_v2.CreateTaskRequest(
            parent=config.deliver_mi_hub_reports_task_queue_id,
            task=tasks_v2.Task(
                name=f"{config.deliver_mi_hub_reports_task_queue_id}/tasks/{questionnaire.get('name')}-{uuid.uuid4()}",
                http_request=tasks_v2.HttpRequest(
                    http_method="POST",
                    url=f"https://{config.region}-{config.gcloud_project}.cloudfunctions.net/bert-deliver-mi-hub-reports-processor",
                    body=json.dumps(questionnaire).encode(),
                    headers={
                        "Content-Type": "application/json",
                    },
                    oidc_token={
                        "service_account_email": config.cloud_function_sa,
                    },
                ),
            ),
        )
        task_client.create_task(request)


def deliver_mi_hub_reports_processor(request):
    print("Running Cloud Function - deliver_mi_hub_reports_processor")
    config = Config.from_env()
    config.log()
    request_json = request.get_json()
    if request_json is None:
        raise Exception("Function was not triggered due to an valid request")
    questionnaire_name = request_json["name"]
    questionnaire_id = request_json["id"]
    zip_data = []
    mi_hub_call_history = get_mi_hub_call_history(
        config, questionnaire_name, questionnaire_id
    )
    if mi_hub_call_history:
        call_history_csv = write_csv(mi_hub_call_history)
        zip_data.append({"filename": "call_history.csv", "content": call_history_csv})
    else:
        print(f"No call history for {questionnaire_name}")
    mi_hub_respondent_data = get_mi_hub_respondent_data(config, questionnaire_name)
    if mi_hub_respondent_data:
        respondent_data_csv = write_csv(mi_hub_respondent_data)
        zip_data.append(
            {"filename": "respondent_data.csv", "content": respondent_data_csv}
        )
    else:
        print(f"No respondent data for {questionnaire_name}")
    if zip_data:
        zipped_data = create_zip(zip_data)
        datetime_string = datetime.datetime.now().strftime("%d%m%Y_%H%M%S")
        mi_filename = f"mi_{questionnaire_name}_{datetime_string}"
        google_storage = init_google_storage(config)
        if google_storage.bucket is None:
            return "Connection to storage bucket failed", 500
        google_storage.upload_zip(f"{mi_filename}.zip", zipped_data)
    else:
        print(f"No data for {questionnaire_name}")
    return f"Done - {questionnaire_name}"


if os.path.isfile("./.env"):
    print("Loading environment variables from dotenv file")
    load_dotenv()

load_config(app)
setup_app(app)

if __name__ == "__main__":
    print("Running Flask application")
    app.run(host="0.0.0.0", port=5011)
