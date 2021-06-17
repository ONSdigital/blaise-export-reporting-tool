from data_sources.datastore import get_call_history_records_by_interviewer_and_date_range
from interviewer_pattern_report.derived_variables import *


def get_call_pattern_records_by_interviewer_and_date_range(interviewer_name, start_date_string, end_date_string):
    error, call_history = get_call_history_records_by_interviewer_and_date_range(
        interviewer_name, start_date_string, end_date_string
    )

    if error:
        print(f"get_call_pattern_records_by_interviewer_and_date_range failed because {error}")
        return error, []

    # TODO: handle the crap out of this
    call_history_dataframe = pd.DataFrame(data=call_history)
    call_history_dataframe = call_history_dataframe.astype({'number_of_interviews': 'int32'})

    hours_worked = get_hours_worked(call_history_dataframe)

    interviewer_pattern_dict = {
        'Hours worked': hours_worked,
        'Call time': get_call_time_in_seconds(call_history_dataframe),
        '% Hours on calls': f"{get_percentage_of_hours_on_calls(get_hours_worked(call_history_dataframe), get_call_time_in_seconds(call_history_dataframe))}%",
        'Ave calls per working hour': get_average_calls_per_hour(call_history_dataframe, hours_worked),
        'Respondents interviewed': get_respondents_interviewed(call_history_dataframe),
        # 'Households completed successfully': get_successfully_completed_households(call_history),
        'Average respondents interviewed per working hour': get_average_respondents_interviewed_per_hour(call_history_dataframe),
        '% Non-contacts for all calls': f"{get_percentage_of_hours_on_calls(call_history_dataframe)}%",
    }

    # TODO: check some errorrrrs before returning!
    return None, interviewer_pattern_dict
