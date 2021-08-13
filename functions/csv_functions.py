import csv
from dataclasses import asdict, is_dataclass
from typing import List, Dict
import io


def write_list_of_dicts_to_csv(csv_file, list_of_dicts, fieldnames):
    try:
        with open(csv_file, "w", newline="") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            for data in list_of_dicts:
                if is_dataclass(data):
                    writer.writerow(asdict(data))
                else:
                    writer.writerow(data)
    except IOError:
        print("I/O error")


def write_csv_mem(data):
    buffer = io.StringIO()
    csv_writer = csv.DictWriter(buffer, fieldnames=get_fieldnames(data))
    csv_writer.writeheader()
    for row in data:
        if is_dataclass(row):
            csv_writer.writerow(asdict(row))
        else:
            csv_writer.writerow(row)
    csv_data = buffer.getvalue()
    buffer.close()
    return csv_data


def get_fieldnames(data):
    if len(data) == 0:
        return []

    fieldname_row = data[0]
    if is_dataclass(fieldname_row):
        return fieldname_row.fields()
    return list(fieldname_row.keys())
