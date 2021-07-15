import csv
from dataclasses import dataclass

from functions.csv_functions import write_list_of_dicts_to_csv
from functions.folder_functions import get_tmp_directory_path, create_tmp_directory


@dataclass
class SimpleClass:
    name: str
    email: str


def test_write_list_of_dicts_to_csv():
    headers = ["name", "email"]
    simple_list = [SimpleClass("matthew", "mail@email.com")]
    create_tmp_directory()
    tmp_folder = get_tmp_directory_path()
    csv_file_name = f"{tmp_folder}/test_file.csv"
    write_list_of_dicts_to_csv(csv_file_name, simple_list, headers)
    with open(csv_file_name, mode="r")as file:
        csv_file = csv.reader(file)
        header = next(csv_file)
        first_row = next(csv_file)
        assert header == headers
        assert ["matthew", "mail@email.com"] == first_row
