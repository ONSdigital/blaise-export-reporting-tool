import pandas as pd
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

    daily_call_history = call_history_dataframe.groupby([call_history_dataframe['call_start_time'].dt.date]).agg({'call_start_time': min, 'call_end_time': max})

    daily_call_history['hours_worked'] = daily_call_history['call_end_time'] - daily_call_history['call_start_time']
    total_hours = daily_call_history['hours_worked'].sum()

    return str(datetime.timedelta(seconds=total_hours.total_seconds()))


def get_call_time(call_history_dataframe):
    return str(round(call_history_dataframe['dial_secs'].sum()))


def get_percentage_of_time_on_calls(hours_worked, total_call_seconds):
    seconds = sum(x * int(t) for x, t in zip([3600, 60, 1], hours_worked.split(":")))
    return round(100 * float(total_call_seconds)/float(seconds))


def interviewer_pattern_data(interviewer, date_from, date_to):
    call_history_dataframe = get_call_history_dataframe_by_interviewer(interviewer, date_from, date_to)

    interviewer_pattern_dict = {
        'Hours worked': get_hours_worked(call_history_dataframe),
        'Call time': get_call_time(call_history_dataframe),
        '% Hours on calls': f"{get_percentage_of_time_on_calls(get_hours_worked(call_history_dataframe), get_call_time(call_history_dataframe))}%",
    }

    return interviewer_pattern_dict


if __name__ == "__main__":
    interviewer_pattern_data = interviewer_pattern_data("matpal", "2021-01-01", "2021-06-11")

    print("foo")