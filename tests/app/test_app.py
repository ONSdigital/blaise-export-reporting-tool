import json
from unittest.mock import patch

from models.config_model import Config
from models.error_capture import BertException


def test_load_config():
    application_config = Config.from_env()
    assert application_config is not None


def test_call_history_report_status(client):
    client.application.call_history_client.get_call_history_report_status.return_value = "blah"
    response = client.get("/api/reports/call-history-status")
    assert response.status_code == 200
    assert response.get_data() is not None


@patch("app.app.get_call_history_records")
def test_call_history_report(mock_get_call_history_records, client):
    mock_get_call_history_records.return_value = []
    response = client.get("/api/reports/call-history/matpal?start-date=2021-01-01&end-date=2021-01-01&survey-tla=opn")
    assert response.status_code == 200
    assert json.loads(response.get_data(as_text=True)) == []


@patch("app.app.get_call_history_records")
def test_call_history_report_returns_error(mock_get_call_history_records, client):
    mock_get_call_history_records.side_effect = BertException("Invalid date range parameters provided", 400)
    response = client.get("/api/reports/call-history/matpal?start-date=2021-01-01&end-date=2021-01-01")
    assert response.status_code == 400
    assert response.get_data(as_text=True) == '{"error": "Invalid date range parameters provided"}'


@patch("app.app.get_call_pattern_report")
def test_call_pattern_report(mock_get_call_pattern_records, client,
                             interviewer_call_pattern_report):
    mock_get_call_pattern_records.return_value = interviewer_call_pattern_report
    response = client.get("/api/reports/call-pattern/matpal?start-date=2021-01-01&end-date=2021-01-01&survey-tla=opn")
    assert response.status_code == 200
    assert response.get_data(as_text=True) == interviewer_call_pattern_report.json()


@patch("app.app.get_call_pattern_report")
def test_call_pattern_report_returns_error(mock_get_call_pattern_records, client):
    mock_get_call_pattern_records.side_effect = BertException("Invalid date range parameters provided", 400)
    response = client.get("/api/reports/call-pattern/matpal?start-date=2021-01-01&end-date=2021-01-01")
    assert response.status_code == 400
    assert response.get_data(as_text=True) == '{"error": "Invalid date range parameters provided"}'


@patch("app.app.get_call_pattern_report")
def test_call_pattern_report_returns_empty_dict(mock_get_call_pattern_records, client):
    mock_get_call_pattern_records.return_value = {}
    response = client.get("/api/reports/call-pattern/matpal?start-date=2021-01-01&end-date=2021-01-01")
    assert response.status_code == 200
    assert json.loads(response.get_data(as_text=True)) == {}


@patch("app.app.get_appointment_resource_planning_by_date")
def test_appointment_resource_planning(mock_get_appointment_resource_planning_by_date, client):
    mock_get_appointment_resource_planning_by_date.return_value = None, []
    response = client.get("/api/reports/appointment-resource-planning/2021-01-01")
    assert response.status_code == 200
    assert response.get_data() is not None
