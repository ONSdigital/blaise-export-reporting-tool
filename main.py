import json
import os
import uuid

import flask
from dotenv import load_dotenv
from google.cloud import datastore, tasks_v2

from app.app import app, load_config, setup_app
from cloud_functions.deliver_mi_hub_reports import (
    deliver_mi_hub_reports_cloud_function_processor,
)
from data_sources.call_history_data import CallHistoryClient
from data_sources.questionnaire_data import get_list_of_installed_questionnaires
from models.config_model import Config


def delete_old_call_history(_request: flask.Request):
    print("Running Cloud Function - delete_old_call_history")
    datastore_client = datastore.Client()
    call_history_client = CallHistoryClient(datastore_client)
    call_history_client.delete_historical_call_history()
    return "Done"


def upload_call_history(_request: flask.Request):
    print("Running Cloud Function - upload_call_history")
    config = Config.from_env()
    config.log()
    datastore_client = datastore.Client()
    call_history_client = CallHistoryClient(datastore_client, config)
    call_history_client.call_history_extraction_process()
    return "Done"


def deliver_mi_hub_reports_trigger(_request: flask.Request):
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
    return "Done"


def deliver_mi_hub_reports_processor(request: flask.Request) -> str:
    print("Running Cloud Function - deliver_mi_hub_reports_processor")
    config = Config.from_env()
    config.log()
    return deliver_mi_hub_reports_cloud_function_processor(request, config)


if os.path.isfile("./.env"):
    print("Loading environment variables from dotenv file")
    load_dotenv()

load_config(app)
setup_app(app)

if __name__ == "__main__":
    print("Running Flask application")
    app.run(host="0.0.0.0", port=5011)
