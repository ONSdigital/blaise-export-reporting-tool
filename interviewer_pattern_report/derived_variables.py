import datetime


def get_hours_worked(df):
    df_call_start_times = df[["call_start_time"]]
    df_call_end_times = df[["call_end_time"]]
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
    print("get_hours_worked - " + str(datetime.timedelta(seconds=total_time_worked.total_seconds())))
    return str(datetime.timedelta(seconds=total_time_worked.total_seconds()))


def get_total_call_seconds(df):
    df_call_seconds = df[["dial_secs"]]
    total_call_seconds = round(df_call_seconds["dial_secs"].sum())
    print("get_total_call_seconds - " + str(total_call_seconds))
    return total_call_seconds
