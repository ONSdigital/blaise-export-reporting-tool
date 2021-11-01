import os
from unittest import mock
from unittest.mock import patch

from reports.appointment_resource_planning_language_summary import (
    get_appointment_resource_planning_language_summary_by_date,
)


@mock.patch.dict(
    os.environ,
    {
        "MYSQL_HOST": "foo",
        "MYSQL_USER": "foo",
        "MYSQL_PASSWORD": "foo",
        "MYSQL_DATABASE": "foo",
    },
)
@patch(
    "data_sources.cati_data.get_cati_appointment_resource_planning_from_database_for_language_summary"
)
def test_get_appointment_resource_planning_language_summary_by_date_returns_empty_string(
    mock_get_cati_appointment_resource_planning_from_database_for_language_summary,
):
    mock_get_cati_appointment_resource_planning_from_database_for_language_summary.return_value = (
        []
    )
    result = get_appointment_resource_planning_language_summary_by_date("2021-12-31")
    assert result == []
