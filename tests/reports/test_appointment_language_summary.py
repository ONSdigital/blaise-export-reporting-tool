from unittest.mock import patch

from models.appointment_language_summary_model import AppointmentLanguageSummary
from reports.appointment_language_summary import (
    get_appointment_language_summary_by_date,
)


@patch("data_sources.cati_data.get_cati_appointment_language_summary_from_database")
def test_get_appointment_language_summary_by_date_returns_empty_list(
    mock_get_cati_appointment_language_summary,
):
    mock_get_cati_appointment_language_summary.return_value = []
    result = get_appointment_language_summary_by_date("2021-12-31", "", "")
    assert result == []


@patch("data_sources.cati_data.get_cati_appointment_language_summary_from_database")
def test_get_appointment_language_summary_by_date_returns_english_total_only(
    mock_get_cati_appointment_language_summary,
):
    mock_get_cati_appointment_language_summary.return_value = [
        {"AppointmentLanguage": "English", "Total": 1}
    ]
    result = get_appointment_language_summary_by_date("2021-12-31", "", "")
    assert result == [AppointmentLanguageSummary(language="English", total=1)]


@patch("data_sources.cati_data.get_cati_appointment_language_summary_from_database")
def test_get_appointment_language_summary_by_date_returns_english_and_welsh_totals(
    mock_get_cati_appointment_language_summary,
):
    mock_get_cati_appointment_language_summary.return_value = [
        {"AppointmentLanguage": "English", "Total": 1},
        {"AppointmentLanguage": "Welsh", "Total": 2},
    ]
    result = get_appointment_language_summary_by_date("2021-12-31", "", "")
    assert result == [
        AppointmentLanguageSummary(language="English", total=1),
        AppointmentLanguageSummary(language="Welsh", total=2),
    ]


@patch("data_sources.cati_data.get_cati_appointment_language_summary_from_database")
def test_get_appointment_language_summary_by_date_returns_totals_for_english_welsh_and_other(
    mock_get_cati_appointment_language_summary,
):
    mock_get_cati_appointment_language_summary.return_value = [
        {"AppointmentLanguage": "English", "Total": 1},
        {"AppointmentLanguage": "Welsh", "Total": 2},
        {"AppointmentLanguage": "Other", "Total": 3},
    ]
    result = get_appointment_language_summary_by_date("2021-12-31", "", "")
    assert result == [
        AppointmentLanguageSummary(language="English", total=1),
        AppointmentLanguageSummary(language="Welsh", total=2),
        AppointmentLanguageSummary(language="Other", total=3),
    ]


@patch("data_sources.cati_data.get_cati_appointment_language_summary_from_database")
def test_get_appointment_language_summary_by_date_returns_totals_for_english_welsh_other_and_new_languages(
    mock_get_cati_appointment_language_summary,
):
    mock_get_cati_appointment_language_summary.return_value = [
        {"AppointmentLanguage": "English", "Total": 1},
        {"AppointmentLanguage": "Welsh", "Total": 2},
        {"AppointmentLanguage": "Other", "Total": 3},
        {"AppointmentLanguage": "Sarcasm", "Total": 4},
        {"AppointmentLanguage": "Profanity", "Total": 5},
    ]
    result = get_appointment_language_summary_by_date("2021-12-31", "", "")
    assert result == [
        AppointmentLanguageSummary(language="English", total=1),
        AppointmentLanguageSummary(language="Welsh", total=2),
        AppointmentLanguageSummary(language="Other", total=3),
        AppointmentLanguageSummary(language="Sarcasm", total=4),
        AppointmentLanguageSummary(language="Profanity", total=5),
    ]
