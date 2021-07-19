import json
from unittest.mock import patch

from models.config import Config


def test_load_config():
    application_config = Config.from_env()
    assert application_config is not None


@patch("app.app.get_call_history_report_status")
def test_call_history_report_status(mock_get_call_history_report_status, client):
    mock_get_call_history_report_status.return_value = "blah"
    response = client.get("/api/reports/call-history-status")
    assert response.status_code == 200
    assert response.get_data() is not None


def test_call_history_report_returns_error_when_start_date_parameter_is_not_provided(client):
    response = client.get("/api/reports/call-history/matpal?end-date=2021-01-01")
    assert response.status_code == 400
    assert response.get_data(as_text=True) == '{"error": "Invalid request, missing required date parameters"}'


def test_call_history_report_returns_error_when_start_date_value_is_not_provided(client):
    response = client.get("/api/reports/call-history/matpal?start-date=&end-date=2021-01-01")
    assert response.status_code == 400
    assert response.get_data(as_text=True) == '{"error": "Invalid request, date is not valid"}'


def test_call_history_report_returns_error_when_start_date_value_is_not_a_valid_date(client):
    response = client.get("/api/reports/call-history/matpal?start-date=blah&end-date=2021-01-01")
    assert response.status_code == 400
    assert response.get_data(as_text=True) == '{"error": "Invalid request, date is not valid"}'


def test_call_history_report_returns_error_when_end_date_parameter_is_not_provided(client):
    response = client.get("/api/reports/call-history/matpal?start-date=2021-01-01")
    assert response.status_code == 400
    assert response.get_data(as_text=True) == '{"error": "Invalid request, missing required date parameters"}'


def test_call_history_report_returns_error_when_end_date_value_is_not_provided(client):
    response = client.get("/api/reports/call-history/matpal?start-date=2021-01-01&end-date=")
    assert response.status_code == 400
    assert response.get_data(as_text=True) == '{"error": "Invalid request, date is not valid"}'


def test_call_history_report_returns_error_when_end_date_value_is_not_a_valid_date(client):
    response = client.get("/api/reports/call-history/matpal?start-date=2021-01-01&end-date=blah")
    assert response.status_code == 400
    assert response.get_data(as_text=True) == '{"error": "Invalid request, date is not valid"}'


@patch("app.app.get_call_history_records_by_interviewer_and_date_range")
def test_call_history_report(mock_get_call_history_records_by_interviewer_and_date_range, client):
    mock_get_call_history_records_by_interviewer_and_date_range.return_value = None, []
    response = client.get("/api/reports/call-history/matpal?start-date=2021-01-01&end-date=2021-01-01")
    assert response.status_code == 200
    assert json.loads(response.get_data(as_text=True)) == []


@patch("app.app.get_call_history_records_by_interviewer_and_date_range")
def test_call_history_report_returns_error(mock_get_call_history_records_by_interviewer_and_date_range, client):
    mock_get_call_history_records_by_interviewer_and_date_range.return_value = (
                                                                                   "Invalid date range parameters provided",
                                                                                   400), []
    response = client.get("/api/reports/call-history/matpal?start-date=2021-01-01&end-date=2021-01-01")
    assert response.status_code == 400
    assert response.get_data(as_text=True) == "Invalid date range parameters provided"


def test_call_pattern_report_returns_error_when_start_date_parameter_is_not_provided(client):
    response = client.get("/api/reports/call-pattern/matpal?end-date=2021-01-01")
    assert response.status_code == 400
    assert response.get_data(as_text=True) == '{"error": "Invalid request, missing required date parameters"}'


def test_call_pattern_report_returns_error_when_start_date_value_is_not_provided(client):
    response = client.get("/api/reports/call-pattern/matpal?start-date=&end-date=2021-01-01")
    assert response.status_code == 400
    assert response.get_data(as_text=True) == '{"error": "Invalid request, date is not valid"}'


def test_call_pattern_report_returns_error_when_start_date_value_is_not_a_valid_date(client):
    response = client.get("/api/reports/call-pattern/matpal?start-date=blah&end-date=2021-01-01")
    assert response.status_code == 400
    assert response.get_data(as_text=True) == '{"error": "Invalid request, date is not valid"}'


def test_call_pattern_report_returns_error_when_end_date_parameter_is_not_provided(client):
    response = client.get("/api/reports/call-pattern/matpal?start-date=2021-01-01")
    assert response.status_code == 400
    assert response.get_data(as_text=True) == '{"error": "Invalid request, missing required date parameters"}'


def test_call_pattern_report_returns_error_when_end_date_value_is_not_provided(client):
    response = client.get("/api/reports/call-pattern/matpal?start-date=2021-01-01&end-date=")
    assert response.status_code == 400
    assert response.get_data(as_text=True) == '{"error": "Invalid request, date is not valid"}'


def test_call_pattern_report_returns_error_when_end_date_value_is_not_a_valid_date(client):
    response = client.get("/api/reports/call-pattern/matpal?start-date=2021-01-01&end-date=blah")
    assert response.status_code == 400
    assert response.get_data(as_text=True) == '{"error": "Invalid request, date is not valid"}'


@patch("app.app.get_call_pattern_records_by_interviewer_and_date_range")
def test_call_pattern_report(mock_get_call_pattern_records_by_interviewer_and_date_range, client, mock_report):
    mock_get_call_pattern_records_by_interviewer_and_date_range.return_value = (None, 200), mock_report
    response = client.get("/api/reports/call-pattern/matpal?start-date=2021-01-01&end-date=2021-01-01")
    assert response.status_code == 200
    assert response.get_data(as_text=True) == mock_report.json()


@patch("app.app.get_call_pattern_records_by_interviewer_and_date_range")
def test_call_pattern_report_returns_error(mock_get_call_pattern_records_by_interviewer_and_date_range, client):
    mock_get_call_pattern_records_by_interviewer_and_date_range.return_value = (
                                                                                   "Invalid date range parameters provided",
                                                                                   400), []
    response = client.get("/api/reports/call-pattern/matpal?start-date=2021-01-01&end-date=2021-01-01")
    assert response.status_code == 400
    assert response.get_data(as_text=True) == "Invalid date range parameters provided"


@patch("app.app.get_call_pattern_records_by_interviewer_and_date_range")
def test_call_pattern_report(mock_get_call_pattern_records_by_interviewer_and_date_range, client,
                             interviewer_call_pattern_report):
    mock_get_call_pattern_records_by_interviewer_and_date_range.return_value = None, interviewer_call_pattern_report
    response = client.get("/api/reports/call-pattern/matpal?start-date=2021-01-01&end-date=2021-01-01")
    assert response.status_code == 200
    assert response.get_data(as_text=True) == interviewer_call_pattern_report.json()
