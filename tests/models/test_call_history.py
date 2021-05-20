from models.call_history import CallHistory


def test_generate_questionnaire_details_lms():
    call_history = CallHistory(
        "896c4038-2b07-40bf-a853-ab07305ca3eb",
        "1001041",
        1,
        1,
        0,
        "2021-05-12 13:10:03.4471819",
        "2021-05-12 13:10:35.0991819",
        31,
        "Finished (No contact)",
        "Edwin",
        "Busy",
        None,
        None,
    )
    call_history.generate_questionnaire_details("LMS2101_AA1")

    assert call_history.questionnaire_name == "LMS2101_AA1"
    assert call_history.survey == "LMS"
    assert call_history.cohort == "AA"
    assert call_history.wave == 1


def test_generate_questionnaire_details_opn():
    call_history = CallHistory(
        "896c4038-2b07-40bf-a853-ab07305ca3eb",
        "1001041",
        1,
        1,
        0,
        "2021-05-12 13:10:03.4471819",
        "2021-05-12 13:10:35.0991819",
        31,
        "Finished (No contact)",
        "Edwin",
        "Busy",
        None,
        None,
    )
    call_history.generate_questionnaire_details("OPN2101A")

    assert call_history.questionnaire_name == "OPN2101A"
    assert call_history.survey == "OPN"
    assert call_history.cohort is None
    assert call_history.wave is None
