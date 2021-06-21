from models.mi_call_history import MICallHistory, CatiMiCallHistoryTable


def test_mi_call_history():
    mi_call_history = MICallHistory(
        questionnaire_id="",
        serial_number="",
        call_number=0,
        dial_number=0,
        interviewer="",
        dial_result=0,
        dial_line_number=0,
        seconds_interview=0
    )
    assert mi_call_history is not None


def test_cati_mi_call_history_table_fields():
    fields = CatiMiCallHistoryTable.fields()
    assert fields == ", ".join(
        [
            "InstrumentId",
            "PrimaryKeyValue",
            "Id",
            "StartTime",
            "CallNumber",
            "DialNumber",
            "Interviewer",
            "DialResult",
            "DialedNumber",
            "AppointmentInfo",
            "EndTime",
            "ABS(TIME_TO_SEC(TIMEDIFF(EndTime, StartTime))) as dialsecs",
        ]
    )


def test_cati_mi_call_history_table_table_name():
    assert CatiMiCallHistoryTable.table_name() == "cati.DialHistory"
