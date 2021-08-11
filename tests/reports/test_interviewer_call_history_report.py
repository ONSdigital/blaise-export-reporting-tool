import pytest
from reports.interviewer_call_history_report import get_call_history_records_by_interviewer_and_date_range
from models.error_capture import BertException


def test_get_call_history_records_by_interviewer_and_date_range_with_invalid_dates(interviewer_name, invalid_date):
    with pytest.raises(BertException) as err:
        get_call_history_records_by_interviewer_and_date_range(interviewer_name, invalid_date, invalid_date)

    assert err.value.message == "Invalid date range parameters provided"
    assert err.value.code == 400
