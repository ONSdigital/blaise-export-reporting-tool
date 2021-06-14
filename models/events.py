from dataclasses import dataclass
from models.db_base import DBBase


@dataclass
class CatiEventsTable(DBBase):
    @classmethod
    def table_name(cls):
        return "cati.Events"

    @classmethod
    def fields(cls):
        return "*"
