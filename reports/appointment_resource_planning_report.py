from data_sources.cati_data import get_cati_appointment_resource_planning_from_database
from data_sources.questionnaire_data import get_questionnaire_name
from models.appointment_resource_planning_model import AppointmentResourcePlanning
from models.config_model import Config


def get_appointment_resource_planning_by_date(date, survey_tla, questionnaires):
    print(
        f"Getting data for the appointment resource planning report for {date} and TLA {survey_tla}"
    )
    config = Config.from_env()
    config.log()
    results = get_cati_appointment_resource_planning_from_database(
        config, date, survey_tla, questionnaires
    )
    cati_appointment_resource_planning_list = []
    for item in results:
        cati_appointment_resource_planning = AppointmentResourcePlanning(
            appointment_time=item.get("AppointmentTime"),
            appointment_language=item.get("AppointmentLanguage"),
            case_reference=item.get("CaseReference"),
            respondent_name=item.get("RespondentName"),
            telephone_number=item.get("TelephoneNumber"),
        )
        questionnaire_name = get_questionnaire_name(config, item.get("InstrumentId"))
        if questionnaire_name == "":
            instrument_id = str(item.get("InstrumentId", "Unknown"))
            # Keep the first 4 characters, hide the rest
            masked_id = f"{instrument_id[:4]}..."
            print(
                f"Appointment with unknown questionnaire_name for InstrumentId: {masked_id}"
            )
        else:
            cati_appointment_resource_planning.questionnaire_name = questionnaire_name
        cati_appointment_resource_planning_list.append(
            cati_appointment_resource_planning
        )
    return cati_appointment_resource_planning_list


def get_appointment_questionnaires(date, survey_tla):
    appointment_resource_planning_list = get_appointment_resource_planning_by_date(
        date, survey_tla, None
    )
    questionnaire_list = []
    for item in appointment_resource_planning_list:
        if item.questionnaire_name not in questionnaire_list:
            questionnaire_list.append(item.questionnaire_name)
    return questionnaire_list
