import pandas as pd
import datetime


def get_hours_worked(call_history_dataframe):
    # group by date, subtract first call time from last call time, and return sum total in fancy format
    daily_call_history_by_date = call_history_dataframe.groupby(
        [call_history_dataframe['call_start_time'].dt.date]).agg({'call_start_time': min, 'call_end_time': max})

    daily_call_history_by_date['hours_worked'] = daily_call_history_by_date['call_end_time']-daily_call_history_by_date['call_start_time']

    total_hours = daily_call_history_by_date['hours_worked'].sum()

    return str(datetime.timedelta(seconds=total_hours.total_seconds()))


def get_call_time(call_history_dataframe):
    return str(round(call_history_dataframe['dial_secs'].sum()))


def get_percentage_of_time_on_calls(hours_worked, total_call_seconds):
    total_worked_seconds = sum(time_transformer * int(time_block) for time_transformer, time_block in zip([3600, 60, 1], hours_worked.split(":")))
    return 100 * float(total_call_seconds)/float(total_worked_seconds)


def get_average_calls_per_hour(call_history_dataframe):
    group_calls_by_hour = call_history_dataframe.groupby([call_history_dataframe['call_start_time'].dt.hour]).size().reset_index(name='count_by_hour')

    return str(group_calls_by_hour['count_by_hour'].sum()/len(group_calls_by_hour.index))


def get_respondents_interviewed(call_history_dataframe):
    return round(call_history_dataframe['number_of_interviews'].sum())


def get_successfully_completed_households(call_history_dataframe):
    pass


def get_average_respondents_interviewed_per_hour(call_history_dataframe):
    # group by hour
    group_respondents_by_hour = call_history_dataframe.groupby([call_history_dataframe['call_start_time'].dt.hour]).agg({'number_of_interviews': 'sum'})
    return group_respondents_by_hour['number_of_interviews'].sum()/len(group_respondents_by_hour.index)


if __name__ == "__main__":
    from data_sources.datastore import get_call_history_records_by_interviewer_and_date_range
    entities = get_call_history_records_by_interviewer_and_date_range("matpal", "2021-01-01", "2021-06-11")[1]
    df = pd.DataFrame(entities)
    get_average_respondents_interviewed_per_hour(df)