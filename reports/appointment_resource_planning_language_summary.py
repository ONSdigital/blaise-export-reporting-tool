from data_sources.cati_data import get_cati_appointment_resource_planning_from_database_for_language_summary
from models.appointment_resource_planning_language_summary_model import AppointmentResourcePlanningLanguageSummary

from models.config_model import Config


def get_appointment_resource_planning_language_summary_by_date(date):
    print(f"Getting data for the appointment resource planning language summary for {date}")
    config = Config.from_env()
    config.log()
    results = get_cati_appointment_resource_planning_from_database_for_language_summary(config, date)

    return AppointmentResourcePlanningLanguageSummary(
        english=[key["Total"] for key in results if key["AppointmentLanguage"] == 'English'][0],
        welsh=[key["Total"] for key in results if key["AppointmentLanguage"] == 'Welsh'][0],
        other=[key["Total"] for key in results if key["AppointmentLanguage"] == 'Other'][0]
    )
