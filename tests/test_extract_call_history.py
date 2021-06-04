import os
from unittest import mock
from unittest.mock import patch
from extract_call_history import load_cati_dial_history
from models.call_history import CallHistory
from models.config import Config
from data_sources.database import connect_to_database, select_from


@mock.patch.dict(
    os.environ,
    {
        "MYSQL_HOST": "just-a-simple-host",
        "MYSQL_USER": "test",
        "MYSQL_PASSWORD": "unique-password",
        "MYSQL_DATABASE": "DB_NAME",
        "BLAISE_API_URL": "a-legit-url",
    },
)
@patch("extract_call_history.get_call_history")
def test_load_cati_dial_history(mock_get_call_history):
    # Setup
    questionnaire_list = [
        {"name": "OPN2101A", "id": "05cf69af-3a4e-47df-819a-928350fdda5a"}
    ]

    mock_get_call_history.return_value = [
        (
            "05cf69af-3a4e-47df-819a-928350fdda5a",
            "1001011",
            1,
            1,
            0,
            "2021/05/19 14:59:01",
            "2021/05/19 14:59:17",
            "00 hours 00 minutes 16 seconds",
            "Finished (Non response)",
            "matpal",
            "NonRespons",
            None,
            None,
        )
    ]
    config = Config.from_env()

    # Execution
    dial_history = load_cati_dial_history(config, questionnaire_list)

    # Assertion
    assert len(dial_history) == 1
    assert dial_history == [
        CallHistory(
            questionnaire_id="05cf69af-3a4e-47df-819a-928350fdda5a",
            serial_number="1001011",
            call_number=1,
            dial_number=1,
            busy_dials=0,
            call_start_time="2021/05/19 14:59:01",
            call_end_time="2021/05/19 14:59:17",
            dial_secs="00 hours 00 minutes 16 seconds",
            status="Finished (Non response)",
            interviewer="matpal",
            call_result="NonRespons",
            update_info=None,
            appointment_info=None,
            questionnaire_name="OPN2101A",
            survey="OPN",
            wave=None,
            cohort=None,
            number_of_interviews=None,
            outcome_code=None,
        )
    ]


def test_select_select_from():
    db = connect_to_database(Config.from_env())
    cursor = db.cursor()

    fields = ("InstrumentId , "
              "PrimaryKeyValue , "
              "CallNumber , "
              "DialNumber , "
              "BusyDials , "
              "StartTime , "
              "EndTime , "
              "ABS(TIME_TO_SEC(TIMEDIFF(EndTime, StartTime))) as dialsecs, "
              "Status , "
              "Interviewer , "
              "DialResult , "
              "UpdateInfo , "
              "AppointmentInfo ")
    results = select_from("cati.DialHistory", cursor, fields)
    print("foo")
