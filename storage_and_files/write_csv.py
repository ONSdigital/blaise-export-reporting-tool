import csv
from dataclasses import asdict, is_dataclass


def write_list_of_dict_to_csv(csv_file, list_of_dicts, fieldnames):
    try:
        with open(csv_file, "w") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            for data in list_of_dicts:
                if is_dataclass(data):
                    writer.writerow(asdict(data))
                else:
                    writer.writerow(data)
    except IOError:
        print("I/O error")
