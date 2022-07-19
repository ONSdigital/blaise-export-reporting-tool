from data_sources import cati_data
from models.appointment_language_summary_model import (
    AppointmentLanguageSummary,
)

from models.config_model import Config


def get_appointment_language_summary_by_date(date, survey_tla, questionnaires):
    print(
        f"Getting data for the appointment resource planning language summary for {date} and TLA {survey_tla}"
    )
    config = Config.from_env()
    config.log()
    results = cati_data.get_cati_appointment_language_summary_from_database(
        config, date, survey_tla, questionnaires
    )

    if results == []:
        return []

    summary = []
    for item in results:
        value = AppointmentLanguageSummary(
            language=item.get("AppointmentLanguage"), total=item.get("Total")
        )
        summary.append(value)
    return summary
