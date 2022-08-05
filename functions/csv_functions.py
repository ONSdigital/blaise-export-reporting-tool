import csv
import io
from dataclasses import asdict, is_dataclass


def write_csv(data):
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
