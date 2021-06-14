from models.events import CatiEventsTable


def test_cati_events_table_fields():
    fields = CatiEventsTable.fields()
    assert fields == "*"


def test_cati_events_table_table_name():
    assert CatiEventsTable.table_name() == "cati.Events"
