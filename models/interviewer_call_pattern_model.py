import json
import math
from dataclasses import asdict, dataclass


@dataclass
class InterviewerCallPattern:
    total_records: int
    total_valid_records: int
    hours_worked: str
    call_time: str
    hours_on_calls_percentage: float
    average_calls_per_hour: float
    refusals: int
    no_contacts: int
    completed_successfully: int
    appointments_for_contacts: int
    no_contact_answer_service: int = math.nan
    no_contact_busy: int = math.nan
    no_contact_disconnect: int = math.nan
    no_contact_no_answer: int = math.nan
    no_contact_other: int = math.nan
    discounted_invalid_cases: int = math.nan
    invalid_fields: str = "n/a"

    def json(self):
        return json.dumps(asdict(self))


@dataclass
class InterviewerCallPatternWithNoValidData:
    discounted_invalid_cases: int
    invalid_fields: str
    total_valid_records: int = 0

    def json(self):
        return json.dumps(asdict(self))
