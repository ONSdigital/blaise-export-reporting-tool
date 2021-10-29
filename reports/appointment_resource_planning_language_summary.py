from data_sources.cati_data import get_cati_appointment_resource_planning_from_database_for_language_summary
from models.appointment_resource_planning_language_summary_model import AppointmentResourcePlanningLanguageSummary

from models.config_model import Config


def get_appointment_resource_planning_language_summary_by_date(date):
    print(f"Getting data for the appointment resource planning language summary for {date}")
    config = Config.from_env()
    config.log()
    results = get_cati_appointment_resource_planning_from_database_for_language_summary(config, date)
    cati_appointment_resource_planning_language_summary_list = []
    for item in results:
        cati_appointment_resource_planning = AppointmentResourcePlanningLanguageSummary(
            english=item.get("English"),
            welsh=item.get("Welsh"),
            other=item.get("Other")
        )
        cati_appointment_resource_planning_language_summary_list.append(cati_appointment_resource_planning)
    return cati_appointment_resource_planning_language_summary_list
