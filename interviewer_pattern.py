import pandas as pd
import numpy as np
import datetime
from data_sources.datastore import get_call_history_records_by_interviewer


def richs_og_get_hours_worked(results):
    df_results = pd.DataFrame(results)
    df_call_start_times = df_results[["call_start_time"]]
    df_call_end_times = df_results[["call_end_time"]]
    df_first_calls = df_call_start_times.groupby([df_call_start_times['call_start_time'].dt.date], as_index=True).min()
    df_last_calls = df_call_end_times.groupby([df_call_end_times['call_end_time'].dt.date], as_index=True).max()
    df_first_calls.index.names = ["date"]
    df_first_calls.columns = ["first_call_time"]
    df_last_calls.index.names = ["date"]
    df_last_calls.columns = ["last_call_time"]
    df_first_and_last_calls = df_first_calls.merge(df_last_calls, on="date", how="inner")
    df_first_and_last_calls["time_worked"] = df_first_and_last_calls["last_call_time"] - df_first_and_last_calls[
        "first_call_time"]
    total_time_worked = df_first_and_last_calls["time_worked"].sum()
    print(df_first_and_last_calls)
    print(str(datetime.timedelta(seconds=total_time_worked.total_seconds())))
    return str(datetime.timedelta(seconds=total_time_worked.total_seconds()))


def get_call_history_dataframe_by_interviewer(interviewer, date_from, date_to):
    return pd.DataFrame(get_call_history_records_by_interviewer(interviewer, date_from, date_to)[1])


def get_hours_worked(call_history_dataframe):
    # group by date, subtract first call time from last call time, and return sum total in fancy format

    daily_call_history_by_date = call_history_dataframe.groupby(
        [call_history_dataframe['call_start_time'].dt.date]).agg({'call_start_time': min, 'call_end_time': max})

    daily_call_history_by_date['hours_worked'] = daily_call_history_by_date['call_end_time'] - \
                                                 daily_call_history_by_date['call_start_time']
    total_hours = daily_call_history_by_date['hours_worked'].sum()

    return str(datetime.timedelta(seconds=total_hours.total_seconds()))


def get_call_time(call_history_dataframe):
    return str(round(call_history_dataframe['dial_secs'].sum()))


def get_percentage_of_time_on_calls(hours_worked, total_call_seconds):
    seconds = sum(x * int(t) for x, t in zip([3600, 60, 1], hours_worked.split(":")))
    return round(100 * float(total_call_seconds) / float(seconds))


def get_average_calls_per_hour(call_history_dataframe):
    # groupby and count by hour
    group_by_hour = call_history_dataframe.groupby([call_history_dataframe['call_start_time'].dt.hour]).size().reset_index(name='count_by_hour')

    # total calls an hour
    total_calls_per_hour = len(group_by_hour.index)

    sum_calls = group_by_hour['count_by_hour'].sum()

    le_averahge = sum_calls/total_calls_per_hour

    # # average per hour
    # group_by_hour['average_per_hour'] = pd.Series(
    #     group_by_hour['count_by_hour'] / total_calls_per_hour
    # )
    #
    # # StackOverflow example2
    # foo = call_history_dataframe.groupby([call_history_dataframe['call_start_time'].dt.hour])['dial_secs'].describe()
    #
    #
    # # StackOverflow example
    # # gb = df.groupby(['col1', 'col2'])
    # # counts = gb.size().to_frame(name='counts')
    # # counts.join(gb.agg({'col3': 'mean'}).rename(columns={'col3': 'col3_mean'})).reset_index()
    #
    # # My attempt
    # grp_by_hr = call_history_dataframe.groupby([call_history_dataframe['call_start_time'].dt.hour])
    # counts = grp_by_hr.size().to_frame(name='count_by_hour')
    # grp_by_hr.join(
    #     counts.agg({'count_by_hour': 'mean'}).rename(
    #         columns={'count_by_hour': 'count_by_hour_mean'}
    #     )
    # ).reset_index()
    #
    #
    #
    #
    # # average of average
    #
    # print("foo")
    #
    #
    #
    #
    #
    #
    # try:
    #     total_calls/calls_her_pour
    # except ZeroDivisionError:
    #     raise
    # else:
    #     return total_calls/calls_her_pour


def interviewer_pattern_data(interviewer, date_from, date_to):
    call_history_dataframe = get_call_history_dataframe_by_interviewer(interviewer, date_from, date_to)

    interviewer_pattern_dict = {
        'Hours worked': get_hours_worked(call_history_dataframe),
        'Call time': get_call_time(call_history_dataframe),
        '% Hours on calls': f"{get_percentage_of_time_on_calls(get_hours_worked(call_history_dataframe), get_call_time(call_history_dataframe))}%",
        'Ave calls per working hour': get_average_calls_per_hour,
    }

    return interviewer_pattern_dict


if __name__ == "__main__":
    interviewer_pattern_data = interviewer_pattern_data("matpal", "2021-01-01", "2021-06-11")
    print("foo")
