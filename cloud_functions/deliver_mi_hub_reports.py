import logging
import re

import flask

from data_sources.questionnaire_data import BlaiseAPIException
from functions.google_storage_functions import init_google_storage
from models.config_model import Config
from reports.mi_hub_call_history_report import get_mi_hub_call_history
from reports.mi_hub_respondent_data_report import get_mi_hub_respondent_data
from services.deliver_mi_hub_reports_service import DeliverMiHubReportsService


def deliver_mi_hub_reports_cloud_function_processor(
    request: flask.Request, config: Config
) -> str:
    request_json = request.get_json()
    if request_json is None:
        logging.error(
            "deliver_mi_hub_reports_cloud_function_processor was not triggered due to an invalid request"
        )
        raise Exception(
            "deliver_mi_hub_reports_cloud_function_processor was not triggered due to an invalid request"
        )

    google_storage = init_google_storage(config)
    if google_storage.bucket is None:
        logging.error(
            f"Connection to storage bucket {config.nifi_staging_bucket} failed"
        )
        raise Exception(
            f"Connection to storage bucket {config.nifi_staging_bucket} failed", 500
        )

    questionnaire_name = request_json["name"]
    questionnaire_id = request_json["id"]

    pattern = re.compile(r"^(DIT.*|DIA.*B|.*_ContactInfo)$", re.IGNORECASE)

    if pattern.match(questionnaire_name):
        return f"Skipping '{questionnaire_name}' as do not process DIT, DIA B or ContactInfo questionnaires"

    try:
        try:
            mi_hub_call_history = get_mi_hub_call_history(
                config, questionnaire_name, questionnaire_id
            )
        except BlaiseAPIException as err:
            logging.error(
                "Failed to fetch call history for %s (%s): %s",
                questionnaire_name,
                questionnaire_id,
                err,
            )
            mi_hub_call_history = []

        try:
            mi_hub_respondent_data = get_mi_hub_respondent_data(
                config, questionnaire_name
            )
        except BlaiseAPIException as err:
            logging.error(
                "Failed to fetch respondent data for %s (%s): %s",
                questionnaire_name,
                questionnaire_id,
                err,
            )
            mi_hub_respondent_data = []

        return DeliverMiHubReportsService.upload_mi_hub_reports_to_gcp(
            questionnaire_name,
            mi_hub_call_history,
            mi_hub_respondent_data,
            google_storage,
        )
    except Exception as err:
        logging.error(
            "deliver_mi_hub_reports_cloud_function_processor failed for %s (%s): %s",
            questionnaire_name,
            questionnaire_id,
            err,
        )
        return f"Error delivering reports for {questionnaire_name}: {err}"
