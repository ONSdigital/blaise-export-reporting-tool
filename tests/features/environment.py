import logging
from typing import List
from unittest.mock import create_autospec

from app.app import setup_app
from functions.google_storage_functions import GoogleStorage
from models.mi_hub_call_history_model import MiHubCallHistory


class LogCapture(logging.Handler):
    def __init__(self):
        super().__init__()
        self.buffer = []

    def emit(self, record):
        self.buffer.append(record)


def mock_mi_hub_call_history() -> List[MiHubCallHistory]:
    return [
        MiHubCallHistory(
            questionnaire_name="LMS2222Z",
            questionnaire_id="s0me-r7nd0m-gu1d",
            serial_number="900001",
            dial_date="20221017",
            dial_time="10:31:36",
            end_time="10:32:06",
            seconds_interview=30,
            call_number=1,
            dial_number=1,
            interviewer="thorne1",
            dial_line_number=None,
            outcome_code="461",
        ),
        MiHubCallHistory(
            questionnaire_name="LMS2222Z",
            questionnaire_id="s0me-r7nd0m-gu1d",
            serial_number="900002",
            dial_date="20221017",
            dial_time="10:33:36",
            end_time="10:34:36",
            seconds_interview=60,
            call_number=1,
            dial_number=1,
            interviewer="thorne1",
            dial_line_number=None,
            outcome_code="461",
        ),
        MiHubCallHistory(
            questionnaire_name="LMS2222Z",
            questionnaire_id="s0me-r7nd0m-gu1d",
            serial_number="900003",
            dial_date="20221017",
            dial_time="10:35:36",
            end_time="10:35:46",
            seconds_interview=10,
            call_number=1,
            dial_number=1,
            interviewer="thorne1",
            dial_line_number=None,
            outcome_code="461",
        ),
    ]


def before_scenario(context, _scenario):
    context.questionnaire_name = "LMS2222Z"
    context.mi_hub_respondent_data = []
    context.mi_hub_call_history = mock_mi_hub_call_history()
    context.mock_google_storage = create_autospec(GoogleStorage)
    # Add log capture for each scenario
    context.log_capture = LogCapture()
    logging.getLogger().addHandler(context.log_capture)


def after_scenario(context, _scenario):
    # Remove log capture after each scenario
    if hasattr(context, "log_capture"):
        logging.getLogger().removeHandler(context.log_capture)
