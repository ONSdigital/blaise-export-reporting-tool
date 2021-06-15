import datetime

import pandas as pd


def get_hours_worked(results):
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


def get_total_call_seconds(results):
    # Call time is a calculation of all the durations of all calls
    # for an interviewer
    # in a day.

    # So using the example
    # 1st call 09:00 to 09:30
    # 2nd call 10:00 to 10:30
    # 3rd call 13:00 to 14:00
    # 4th call 15:00 to 15:30
    # 5th call 16:00 to 16:30
    # this is a Call time of 3 hours

    call_time = 0
    for call in results:
        call_time = call_time + call.get("dialsecs", 0)

    return 240
