import csv
from dataclasses import dataclass, fields

from functions.csv_functions import get_fieldnames, write_csv


@dataclass
class SimpleClass:
    name: str
    email: str

    @classmethod
    def fields(cls):
        return [field.name for field in fields(cls)]


def test_write_csv():
    data = [SimpleClass("matthew", "mail@email.com")]
    csv_data = write_csv(data)
    assert csv_data == """name,email\r\nmatthew,mail@email.com\r\n"""


def test_get_fieldnames_dict():
    data = [{"field1": "value1", "field2": "value2"}]
    assert get_fieldnames(data) == ["field1", "field2"]


def test_get_fieldnames_dataclass():
    data = [SimpleClass("foo", "foo@bar.com")]
    assert get_fieldnames(data) == ["name", "email"]
