import datetime


def get_hours_worked(call_history_dataframe):
    # group by date, subtract first call time from last call time, and return sum total in fancy format
    daily_call_history_by_date = call_history_dataframe.groupby(
        [call_history_dataframe['call_start_time'].dt.date]).agg({'call_start_time': min, 'call_end_time': max})

    daily_call_history_by_date['hours_worked'] = daily_call_history_by_date['call_end_time']-daily_call_history_by_date['call_start_time']

    total_hours = daily_call_history_by_date['hours_worked'].sum()

    return str(datetime.timedelta(seconds=total_hours.total_seconds()))


def get_total_call_seconds(df):
    df_call_seconds = df[["dial_secs"]]
    total_call_seconds = round(df_call_seconds["dial_secs"].sum())
    print("get_total_call_seconds - " + str(total_call_seconds))
    return total_call_seconds


def get_percentage_of_time_on_calls(hours_worked, total_call_seconds):
    print(hours_worked)
    sec_test = sum(x * int(t) for x, t in zip([3600, 60, 1], hours_worked.split(":")))
    print(sec_test)
    return 100 * float(total_call_seconds)/float(sec_test)
