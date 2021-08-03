from dataclasses import dataclass


@dataclass
class Errors:
    message: str
    code: int
