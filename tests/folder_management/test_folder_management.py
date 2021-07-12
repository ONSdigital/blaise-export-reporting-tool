import csv
from dataclasses import dataclass

from storage_and_files.folder_management import get_tmp_directory_path, create_tmp_directory
from storage_and_files.write_csv import write_list_of_dict_to_csv


@dataclass
class SimpleClass:
    name: str
    email: str


def test_write_list_of_dict_to_csv():
    headers = ["name", "email"]
    list = [SimpleClass("matthew", "mail@email.com")]

    create_tmp_directory()
    tmp_folder = get_tmp_directory_path()

    print(f"tmp_folder {tmp_folder}")
    csv_file_name = f"{tmp_folder}/test_file.csv"
    print(f" csv_file_name {csv_file_name}")

    write_list_of_dict_to_csv(csv_file_name, list, headers)

    with open(csv_file_name, mode='r')as file:
        # reading the CSV file
        csv_file = csv.reader(file)
        header = next(csv_file)
        first_row = next(csv_file)

        # displaying the contents of the CSV file
        assert header == headers
        assert ['matthew', 'mail@email.com'] == first_row
