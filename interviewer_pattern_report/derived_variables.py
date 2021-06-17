import pandas as pd
import datetime


def get_hours_worked(call_history_dataframe):
    # group by date, subtract first call time from last call time, and return sum total in fancy format
    daily_call_history_by_date = call_history_dataframe.groupby(
        [call_history_dataframe['call_start_time'].dt.date]).agg({'call_start_time': min, 'call_end_time': max})

    daily_call_history_by_date['hours_worked'] = daily_call_history_by_date['call_end_time']-daily_call_history_by_date['call_start_time']

    total_hours = daily_call_history_by_date['hours_worked'].sum()

    return str(datetime.timedelta(seconds=total_hours.total_seconds()))


def get_call_time_in_seconds(call_history_dataframe):
    return str(round(call_history_dataframe['dial_secs'].sum()))


def get_total_seconds_from_string(hours_worked):
    h, m, s = hours_worked.split(':')
    return int(h) * 3600 + int(m) * 60 + int(s)


def get_percentage_of_hours_on_calls(hours_worked, total_call_seconds):
    return 100 * float(total_call_seconds)/float(get_total_seconds_from_string(hours_worked))


def get_average_calls_per_hour(call_history_dataframe, string_hours_worked):
    integer_hours_worked = get_total_seconds_from_string(string_hours_worked)/3600
    total_calls = len(call_history_dataframe.index)

    return total_calls/integer_hours_worked


def get_respondents_interviewed(call_history_dataframe):
    return round(call_history_dataframe['number_of_interviews'].sum())


def get_successfully_completed_households(call_history_dataframe):
    pass


def get_average_respondents_interviewed_per_hour(call_history_dataframe):
    group_respondents_by_hour = call_history_dataframe.groupby([call_history_dataframe['call_start_time'].dt.hour]).agg({'number_of_interviews': 'sum'})
    return group_respondents_by_hour['number_of_interviews'].sum()/len(group_respondents_by_hour.index)


def get_percentage_non_contacts_for_all_calls(call_history_dataframe):
    no_contact_calls = call_history_dataframe.loc[call_history_dataframe['status'].str.contains("NO CONTACT", case=False)]
    return len(no_contact_calls.index)/len(call_history_dataframe.index)*100



if __name__ == "__main__":
    from data_sources.datastore import get_call_history_records_by_interviewer_and_date_range
    entities = get_call_history_records_by_interviewer_and_date_range("matpal", "2021-01-01", "2021-06-11")[1]
    df = pd.DataFrame(entities)

    hours_worked = get_hours_worked(df)
    get_average_calls_per_hour(df, hours_worked)