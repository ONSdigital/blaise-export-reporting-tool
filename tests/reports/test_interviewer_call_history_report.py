from reports.interviewer_call_history_report import get_call_history_records_by_interviewer_and_date_range


def test_get_call_history_records_by_interviewer_and_date_range_with_invalid_dates(interviewer_name, invalid_date):
    assert get_call_history_records_by_interviewer_and_date_range(interviewer_name, invalid_date, invalid_date) == (
        ("Invalid date range parameters provided", 400), [])
