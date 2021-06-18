import json
from dataclasses import asdict, dataclass


@dataclass
class InterviewerPatternReport:
    hours_worked: str
    call_time: int
    hours_on_calls_percentage: str
    average_calls_per_hour: float
    respondents_interviewed: int
    households_completed_successfully: str
    average_respondents_interviewed_per_hour: int
    no_contacts_percentage: str
    appointments_for_contacts_percentage: str
