import datetime
from dataclasses import dataclass


@dataclass
class CallHistory:
    questionnaire_id: str
    serial_number: int
    call_number: str
    dial_number: str
    busy_dials: int
    call_start_time: datetime
    call_end_time: datetime
    dial_secs: int
    status: str
    interviewer: str
    call_result: str
    update_info: str
    appointment_info: str
    questionnaire_name: str = ""
    survey: str = ""
    wave: int = None
    cohort: str = None
    number_of_interviews: int = None
    outcome_code: str = None

    def generate_questionnaire_details(self, questionnaire_name):
        self.questionnaire_name = questionnaire_name
        self.survey = questionnaire_name[0:3]
        if self.survey == "LMS":
            self.wave: int = int(
                self.questionnaire_name[len(self.questionnaire_name) - 1 :]
            )
            self.cohort: str = self.questionnaire_name[
                len(self.questionnaire_name) - 3 : -1
            ]
