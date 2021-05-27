import json
from unittest.mock import patch


@patch("app.app.get_call_history_records_by_interviewer")
def test_call_history_report_handles_missing_end_date(mock_get_call_history_records_by_interviewer, client):
    mock_get_call_history_records_by_interviewer.return_value = None, []

    response = client.get('/api/reports/call-history/matpal?start-date=now')
    assert response.status_code == 400
    assert json.loads(response.get_data(as_text=True)) == {"error": "Invalid request missing required filter properties"}


@patch("app.app.get_call_history_records_by_interviewer")
def test_call_history_report_handles_missing_start_date(mock_get_call_history_records_by_interviewer, client):
    mock_get_call_history_records_by_interviewer.return_value = None, []

    response = client.get('/api/reports/call-history/matpal?end-date=later')
    assert response.status_code == 400
    assert json.loads(response.get_data(as_text=True)) == {"error": "Invalid request missing required filter properties"}


@patch("app.app.get_call_history_records_by_interviewer")
def test_call_history_report_returns_data_on_successful(mock_get_call_history_records_by_interviewer, client):
    mock_get_call_history_records_by_interviewer.return_value = None, []

    response = client.get('/api/reports/call-history/matpal?start-date=now&end-date=later')
    assert response.status_code == 200
    assert json.loads(response.get_data(as_text=True)) == []
