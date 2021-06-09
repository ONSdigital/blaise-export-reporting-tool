from models.mi_call_history import MICallHistory


def test_mi_call_history():
    mi_call_history = MICallHistory(
        questionnaire_id="",
        serial_number="",
        internal_key=0,
        call_number=0,
        dial_number=0,
        interviewer="",
        dial_result=0,
        dial_line_number=0,
        seconds_dial=0,
    )
    assert mi_call_history is not None
