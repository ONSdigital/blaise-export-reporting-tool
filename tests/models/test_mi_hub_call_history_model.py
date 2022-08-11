from models.mi_hub_call_history_model import CatiMiHubCallHistoryTable, MiHubCallHistory


def test_mi_hub_call_history():
    mi_hub_call_history = MiHubCallHistory(
        questionnaire_id="",
        serial_number="",
        call_number=0,
        dial_number=0,
        interviewer="",
        dial_result=0,
        dial_line_number=0,
        seconds_interview=0,
    )
    assert mi_hub_call_history is not None


def test_cati_mi_hub_call_history_table_fields():
    fields = CatiMiHubCallHistoryTable.fields()
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
            "ABS(TIME_TO_SEC(TIMEDIFF(EndTime, StartTime))) as dial_secs",
            "ExtractValue(`AdditionalData`, '/Fields/Field[@Name=\"QHAdmin.HOut\"]/@Value') AS OutcomeCode",
        ]
    )


def test_cati_mi_hub_call_history_table_table_name():
    assert CatiMiHubCallHistoryTable.table_name() == "cati.DialHistory"
