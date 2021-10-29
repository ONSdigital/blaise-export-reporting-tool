from data_sources.cati_data import get_cati_appointment_resource_planning_from_database_for_language_summary
from models.appointment_resource_planning_language_summary_model import AppointmentResourcePlanningLanguageSummary

from models.config_model import Config


def get_appointment_resource_planning_language_summary_by_date(date):
    print(f"Getting data for the appointment resource planning language summary for {date}")
    config = Config.from_env()
    config.log()
    results = get_cati_appointment_resource_planning_from_database_for_language_summary(config, date)

    if results == []:
        return []

    return AppointmentResourcePlanningLanguageSummary(
        english=[value["Total"] for value in results if value["AppointmentLanguage"] == 'English'][0],
        welsh=[value["Total"] for value in results if value["AppointmentLanguage"] == 'Welsh'][0],
        other=[value["Total"] for value in results if value["AppointmentLanguage"] == 'Other'][0]
    )
