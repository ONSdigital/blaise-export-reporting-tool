import json
from dataclasses import asdict, dataclass


@dataclass
class InterviewerCallPattern:
    hours_worked: str
    call_time: str
    hours_on_calls_percentage: str
    average_calls_per_hour: float
    respondents_interviewed: int
    average_respondents_interviewed_per_hour: float
    refusals: str
    no_contacts: str
    completed_successfully: str
    appointments_for_contacts: str
    no_contact_answer_service: str = "n/a"
    no_contact_busy: str = "n/a"
    no_contact_disconnect: str = "n/a"
    no_contact_no_answer: str = "n/a"
    no_contact_other: str = "n/a"
    discounted_invalid_cases: str = "0"
    invalid_fields: str = "n/a"

    def json(self):
        return json.dumps(asdict(self))

@dataclass
class InterviewerCallPatternRefactored:
    hours_worked: str
    call_time: str
    hours_on_calls_percentage: str
    average_calls_per_hour: float
    refusals: str
    no_contacts: str
    completed_successfully: str
    appointments_for_contacts: str
    no_contact_answer_service: str = "n/a"
    no_contact_busy: str = "n/a"
    no_contact_disconnect: str = "n/a"
    no_contact_no_answer: str = "n/a"
    no_contact_other: str = "n/a"
    discounted_invalid_cases: str = "0"
    invalid_fields: str = "n/a"

    def json(self):
        return json.dumps(asdict(self))


@dataclass
class InterviewerCallPatternWithNoValidData:
    discounted_invalid_cases: str = "0"
    invalid_fields: str = "n/a"

    def json(self):
        return json.dumps(asdict(self))
