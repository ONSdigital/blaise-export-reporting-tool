from models.interviewer_call_pattern import InterviewerCallPattern


def test_interviewer_call_pattern():
    mi_hub_respondent_data = InterviewerCallPattern(
        hours_worked="",
        call_time="",
        hours_on_calls_percentage="",
        average_calls_per_hour="",
        respondents_interviewed="",
        households_completed_successfully="",
        average_respondents_interviewed_per_hour="",
        no_contacts_percentage="",
        appointments_for_contacts_percentage="",
        discounted_invalid_records="",
        invalid_fields="",
    )
    assert mi_hub_respondent_data is not None
