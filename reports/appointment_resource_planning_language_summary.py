from data_sources import cati_data
from models.appointment_resource_planning_language_summary_model import (
    AppointmentResourcePlanningLanguageSummary,
)

from models.config_model import Config


def get_appointment_resource_planning_language_summary_by_date(date):
    print(
        f"Getting data for the appointment resource planning language summary for {date}"
    )
    config = Config.from_env()
    config.log()
    results = cati_data.get_cati_appointment_resource_planning_from_database_for_language_summary(
        config, date
    )

    if results == []:
        return []

    summary = []
    for item in results:
        value = AppointmentResourcePlanningLanguageSummary(
            language=item.get("AppointmentLanguage"), total=item.get("Total")
        )
        summary.append(value)
    return summary
