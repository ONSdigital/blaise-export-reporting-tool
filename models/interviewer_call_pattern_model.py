import json
import math
from dataclasses import asdict, dataclass
from typing import Union


@dataclass
class InterviewerCallPattern:
    total_valid_cases: int
    hours_worked: str
    call_time: str
    hours_on_calls_percentage: float
    average_calls_per_hour: float
    refusals: int
    no_contacts: int
    completed_successfully: int
    appointments_for_contacts: int
    web_nudge: int
    no_contact_answer_service: Union[int, float] = math.nan
    no_contact_busy: Union[int, float] = math.nan
    no_contact_disconnect: Union[int, float] = math.nan
    no_contact_no_answer: Union[int, float] = math.nan
    no_contact_invalid_telephone_number: Union[int, float] = math.nan
    no_contact_other: Union[int, float] = math.nan
    discounted_invalid_cases: Union[int, float] = math.nan
    invalid_fields: str = "n/a"

    def json(self):
        return json.dumps(asdict(self))


@dataclass
class InterviewerCallPatternWithNoValidData:
    discounted_invalid_cases: int
    invalid_fields: str

    def json(self):
        return json.dumps(asdict(self))
