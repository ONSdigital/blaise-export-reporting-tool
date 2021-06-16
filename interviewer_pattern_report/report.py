from interviewer_pattern_report.derived_variables import *


def get_call_pattern_records_by_interviewer_and_date_range(interviewer_name, start_date_string, end_date_string):
    error, call_history = get_call_history_records_by_interviewer_and_date_range(
        interviewer_name, start_date_string, end_date_string
    )

    if error:
        print(f"get_call_pattern_records_by_interviewer_and_date_range failed because {error}")
        return

    # TODO: handle the crap out of this
    call_history['number_of_interviews'] = call_history['number_of_interviews'].astype('int')

    percentage_hours_on_calls = f"{get_percentage_of_time_on_calls(get_hours_worked(call_history), get_call_time(call_history))}%"
    interviewer_pattern_dict = {
        'Hours worked': get_hours_worked(call_history),
        'Call time': get_call_time(call_history),
        '% Hours on calls': percentage_hours_on_calls,
        'Ave calls per working hour': get_average_calls_per_hour(call_history),
        'Respondents interviewed': get_respondents_interviewed(call_history),
        # 'Households completed successfully': get_successfully_completed_households(call_history),
        'Average respondents interviewed per working hour': get_average_respondents_interviewed_per_hour(call_history),
    }

    # TODO: check some errorrrrs before returning!
    return None, interviewer_pattern_dict
