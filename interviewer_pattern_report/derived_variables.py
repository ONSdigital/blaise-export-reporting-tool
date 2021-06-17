import pandas as pd
import datetime


def get_hours_worked(call_history_dataframe):
    # group by date
    daily_call_history_by_date = call_history_dataframe.groupby(
        [call_history_dataframe['call_start_time'].dt.date]).agg({'call_start_time': min, 'call_end_time': max})

    # subtract first call time from last call time
    daily_call_history_by_date['hours_worked'] = daily_call_history_by_date['call_end_time']-daily_call_history_by_date['call_start_time']

    # sum total hours
    total_hours = daily_call_history_by_date['hours_worked'].sum()

    # return sum total in fancy format
    return str(datetime.timedelta(seconds=total_hours.total_seconds()))


def get_call_time_in_seconds(call_history_dataframe):
    return round(call_history_dataframe['dial_secs'].sum())


def get_total_seconds_from_string(hours_worked):
    h, m, s = hours_worked.split(':')
    return int(h) * 3600 + int(m) * 60 + int(s)


def get_percentage_of_hours_on_calls(hours_worked, total_call_seconds):
    return f"{100 * float(total_call_seconds)/float(get_total_seconds_from_string(hours_worked))}%"


def get_average_calls_per_hour(call_history_dataframe, string_hours_worked):
    integer_hours_worked = get_total_seconds_from_string(string_hours_worked)/3600
    total_calls = len(call_history_dataframe.index)

    return total_calls/integer_hours_worked


def get_respondents_interviewed(call_history_dataframe):
    return round(call_history_dataframe['number_of_interviews'].sum())


def get_average_respondents_interviewed_per_hour(call_history_dataframe, string_hours_worked):
    integer_hours_worked = get_total_seconds_from_string(string_hours_worked)/3600
    respondents_interviewed = call_history_dataframe['number_of_interviews'].sum()

    return respondents_interviewed/integer_hours_worked


def get_percentage_of_call_for_status(status, call_history_dataframe):
    numerator = call_history_dataframe.loc[call_history_dataframe['status'].str.contains(status, case=False)]
    return f"{len(numerator.index)/len(call_history_dataframe.index)*100}%"


if __name__ == "__main__":
    from data_sources.datastore import get_call_history_records_by_interviewer_and_date_range
    entities = get_call_history_records_by_interviewer_and_date_range("matpal", "2021-01-01", "2021-06-11")[1]
    df = pd.DataFrame(entities)
