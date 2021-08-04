from data_sources.cati_data import get_cati_appointment_resource_planning_from_database
from data_sources.questionnaire_data import get_list_of_installed_questionnaires
from data_sources.questionnaire_data import get_questionnaire_name_from_id
from models.appointment_resource_planning_model import AppointmentResourcePlanning
from models.config_model import Config


def get_appointment_resource_planning_by_date(date):
    print(f"Getting data for the appointment resource planning report for {date}")
    config = Config.from_env()
    config.log()
    installed_questionnaire_list = get_list_of_installed_questionnaires(config)
    results = get_cati_appointment_resource_planning_from_database(config, date)
    cati_appointment_resource_planning_list = []
    for item in results:
        cati_appointment_resource_planning = AppointmentResourcePlanning(
            appointment_time=item.get("AppointmentTime"),
            appointment_language=item.get("AppointmentLanguage"),
            total=item.get("Total")
        )
        questionnaire_name = get_questionnaire_name_from_id(item.get("InstrumentId"), installed_questionnaire_list)
        if questionnaire_name != "":
            cati_appointment_resource_planning.questionnaire_name = questionnaire_name
        cati_appointment_resource_planning_list.append(cati_appointment_resource_planning)
    return cati_appointment_resource_planning_list
