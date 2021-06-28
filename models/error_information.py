import json
from dataclasses import asdict, dataclass


@dataclass
class ErrorInformation:
    error_message: str
    status_code: int

    def json(self):
        return json.dumps(asdict(self))
