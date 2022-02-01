from models.interviewer_call_pattern_model import InterviewerCallPattern, InterviewerCallPatternWithNoValidData


def test_interviewer_call_pattern_mandatory_fields():
    interviewer_call_pattern = InterviewerCallPattern(
        total_valid_cases=0,
        hours_worked="",
        call_time="",
        hours_on_calls_percentage=0.0,
        average_calls_per_hour=0.0,
        refusals=0,
        no_contacts=0,
        completed_successfully=0,
        appointments_for_contacts=0,
        web_nudge=0,
    )
    assert interviewer_call_pattern is not None


def test_interviewer_call_pattern_all_fields():
    interviewer_call_pattern = InterviewerCallPattern(
        total_valid_cases=0,
        hours_worked="",
        call_time="",
        hours_on_calls_percentage=0,
        average_calls_per_hour=0.0,
        refusals=0,
        no_contacts=0,
        no_contact_answer_service=0,
        no_contact_busy=0,
        no_contact_disconnect=0,
        no_contact_no_answer=0,
        no_contact_other=0,
        completed_successfully=0,
        appointments_for_contacts=0,
        web_nudge=0,
        discounted_invalid_cases=0,
        invalid_fields="",
    )
    assert interviewer_call_pattern is not None


def test_interviewer_call_pattern_with_no_valid_data():
    interviewer_call_pattern_with_no_valid_data = InterviewerCallPatternWithNoValidData(
        discounted_invalid_cases=10,
        invalid_fields="",
    )
    assert interviewer_call_pattern_with_no_valid_data is not None
