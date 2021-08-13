import csv
from dataclasses import dataclass, fields

from functions.csv_functions import (
    get_fieldnames,
    write_csv_mem,
    write_list_of_dicts_to_csv,
)
from functions.folder_functions import get_tmp_directory_path, create_tmp_directory


@dataclass
class SimpleClass:
    name: str
    email: str

    @classmethod
    def fields(cls):
        return [field.name for field in fields(cls)]


def test_write_list_of_dicts_to_csv():
    headers = ["name", "email"]
    simple_list = [SimpleClass("matthew", "mail@email.com")]
    create_tmp_directory()
    tmp_folder = get_tmp_directory_path()
    csv_file_name = f"{tmp_folder}/test_file.csv"
    write_list_of_dicts_to_csv(csv_file_name, simple_list, headers)
    with open(csv_file_name, mode="r") as file:
        csv_file = csv.reader(file)
        header = next(csv_file)
        first_row = next(csv_file)
        assert header == headers
        assert ["matthew", "mail@email.com"] == first_row


def test_write_csv_mem():
    data = [SimpleClass("matthew", "mail@email.com")]
    csv_data = write_csv_mem(data)
    assert csv_data == """name,email\r\nmatthew,mail@email.com\r\n"""


def test_get_fieldnames_dict():
    data = [{"field1": "value1", "field2": "value2"}]
    assert get_fieldnames(data) == ["field1", "field2"]


def test_get_fieldnames_dataclass():
    data = [SimpleClass("foo", "foo@bar.com")]
    assert get_fieldnames(data) == ["name", "email"]
