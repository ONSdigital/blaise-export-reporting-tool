from dataclasses import dataclass, fields


@dataclass
class MiHubRespondentData:
    SER_NO: str
    OUTCOME: str
    DATE_COMPLETED: str
    INT_NAME: str
    MODE: str
    POSTCODE: str
    GENDER: str
    DATE_OF_BIRTH: str
    AGE: str

    @classmethod
    def fields(cls):
        return [field.name for field in fields(cls)]
