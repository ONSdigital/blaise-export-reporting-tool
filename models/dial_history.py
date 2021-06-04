from dataclasses import dataclass
from datetime import datetime

@dataclass
class DialHistory:
    primary_key: str = ""
    internal_key: int = ""
    sample_month: str = ""
    dial_date: datetime = ""
    dial_time: datetime = ""
    call_number: int = ""
    dial_number: int = ""
    interviewer: str = ""
    entry_priority: int = ""
    exit_priority: int = ""
    dial_result: int = ""
    dial_line_number: int = ""
    appointment_type: str = ""
    end_time: datetime = ""
    seconds_dial: datetime = ""
    seconds_interview: datetime = ""
    outcome_code: int = ""
