from dataclasses import dataclass, fields


@dataclass
class MiHubRespondentData:
    serial_number: str
    outcome_code: str
    date_completed: str
    interviewer: str
    mode: str
    postcode: str
    gender: str
    date_of_birth: str
    age: str

    @classmethod
    def fields(cls):
        return [field.name for field in fields(cls)]
