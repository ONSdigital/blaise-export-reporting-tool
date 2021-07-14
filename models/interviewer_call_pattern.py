import json
from dataclasses import asdict, dataclass


@dataclass
class InterviewerCallPatternReport:
    hours_worked: str
    call_time: str
    hours_on_calls_percentage: str
    average_calls_per_hour: float
    respondents_interviewed: int
    households_completed_successfully: int
    average_respondents_interviewed_per_hour: float
    no_contacts_percentage: str
    appointments_for_contacts_percentage: str
    discounted_invalid_records: str = "0"
    invalid_fields: str = "n/a"

    def json(self):
        return json.dumps(asdict(self))
