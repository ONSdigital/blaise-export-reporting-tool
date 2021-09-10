from google.cloud import datastore
import datetime
import pandas as pd


def get_call_pattern_report():
    records = get_call_history_records()
    if records.empty:
        return {}

    hours_worked = calculate_hours_worked(records)
    discounted_invalid_cases = calculate_number_of_invalid_cases

    return {"hours_worked": hours_worked}

def get_call_history_records():
    client = datastore.Client()
    query = client.query(kind="CallHistory")
    return pd.DataFrame(list(query.fetch()))

def calculate_hours_worked(records):
    records = records[records.call_end_time != '']
    records = records.dropna(subset=["call_end_time"])

    # group by date
    daily_call_history_by_date = records.groupby(
        [records['call_start_time'].dt.date]).agg({'call_start_time': min, 'call_end_time': max})

    # subtract first call time from last call time
    daily_call_history_by_date['hours_worked'] = daily_call_history_by_date['call_end_time'] - \
                                                 daily_call_history_by_date['call_start_time']

    # sum total hours
    total_hours = daily_call_history_by_date['hours_worked'].sum()

    # return sum total in fancy format
    return str(datetime.timedelta(seconds=total_hours.total_seconds()))
