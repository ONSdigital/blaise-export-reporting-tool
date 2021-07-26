import datetime


def get_hours_worked(call_history_dataframe):
    try:
        # group by date
        daily_call_history_by_date = call_history_dataframe.groupby(
            [call_history_dataframe['call_start_time'].dt.date]).agg({'call_start_time': min, 'call_end_time': max})

        # subtract first call time from last call time
        daily_call_history_by_date['hours_worked'] = daily_call_history_by_date['call_end_time'] - \
                                                     daily_call_history_by_date['call_start_time']

        # sum total hours
        total_hours = daily_call_history_by_date['hours_worked'].sum()
    except Exception as err:
        print(f"Could not calculate get_hours_worked(): {err}")
        raise

    # return sum total in fancy format
    return str(datetime.timedelta(seconds=total_hours.total_seconds()))


def get_call_time_in_seconds(call_history_dataframe):
    try:
        result = round(call_history_dataframe['dial_secs'].sum())
    except Exception as err:
        print(f"Could not calculate get_call_time_in_seconds(): {err}")
        raise

    return result


def convert_call_time_seconds_to_datetime_format(seconds):
    try:
        result = str(datetime.timedelta(seconds=seconds))
    except Exception as err:
        print(f"Could not convert_call_time_seconds_to_datetime_format(): {err}")
        raise

    return result


def get_total_seconds_from_string(hours_worked):
    try:
        h, m, s = hours_worked.split(':')
        result = int(h) * 3600 + int(m) * 60 + int(s)
    except Exception as err:
        print(f"Could not calculate get_total_seconds_from_string(): {err}")
        raise
    return result


def limit_two_decimal_places(float_value):
    return float("{:.2f}".format(float_value))


def get_percentage_of_hours_on_calls(hours_worked, total_call_seconds):
    try:
        value = 100 * float(total_call_seconds) / float(get_total_seconds_from_string(hours_worked))
        result = f"{limit_two_decimal_places(value)}%"
    except Exception as err:
        print(f"Could not calculate get_percentage_of_hours_on_calls(): {err}")
        raise
    return result


def get_average_calls_per_hour(call_history_dataframe, string_hours_worked):
    try:
        integer_hours_worked = get_total_seconds_from_string(string_hours_worked) / 3600
        total_calls = len(call_history_dataframe.index)

        result = limit_two_decimal_places(total_calls / integer_hours_worked)
    except Exception as err:
        print(f"Could not calculate get_average_calls_per_hour(): {err}")
        raise
    return result


def get_respondents_interviewed(call_history_dataframe):
    try:
        result = round(call_history_dataframe['number_of_interviews'].sum())
    except Exception as err:
        print(f"Could not calculate get_respondents_interviewed(): {err}")
        raise
    return result


def get_number_of_households_completed_successfully(status, call_history_dataframe):
    try:
        result = len(call_history_dataframe.loc[call_history_dataframe['status'].str.contains(status, case=False)])
    except Exception as err:
        print(f"Could not calculate get_percentage_of_call_for_status(): {err}")
        raise
    return result


def get_average_respondents_interviewed_per_hour(call_history_dataframe, string_hours_worked):
    try:
        integer_hours_worked = get_total_seconds_from_string(string_hours_worked) / 3600
        respondents_interviewed = call_history_dataframe['number_of_interviews'].sum()

        result = limit_two_decimal_places(respondents_interviewed / integer_hours_worked)
    except Exception as err:
        print(f"Could not calculate get_average_respondents_interviewed_per_hour(): {err}")
        raise
    return result


def get_percentage_of_call_for_status(status, call_history_dataframe):
    try:
        numerator = call_history_dataframe.loc[call_history_dataframe['status'].str.contains(status, case=False)]

        value = 100 * len(numerator.index) / len(call_history_dataframe.index)
        result = f"{limit_two_decimal_places(value)}%"
    except Exception as err:
        print(f"Could not calculate get_percentage_of_call_for_status(): {err}")
        raise
    return result
