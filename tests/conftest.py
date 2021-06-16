import datetime
import pytest
import pandas as pd
from app.app import app as flask_app
from google.api_core.datetime_helpers import DatetimeWithNanoseconds


@pytest.fixture
def app():
    yield flask_app


@pytest.fixture
def client(app):
    return app.test_client()


@pytest.fixture
def mock_data():
    results = [
        {
            'call_start_time': DatetimeWithNanoseconds(2021, 1, 1, 10, 00, 00, tzinfo=datetime.timezone.utc),
            'call_end_time': DatetimeWithNanoseconds(2021, 1, 1, 10, 30, 00, tzinfo=datetime.timezone.utc),
            'dial_secs': 16.0,
        },
        {
            'call_start_time': DatetimeWithNanoseconds(2021, 1, 11, 00, 00, 00, tzinfo=datetime.timezone.utc),
            'call_end_time': DatetimeWithNanoseconds(2021, 1, 11, 11, 30, 00, tzinfo=datetime.timezone.utc),
            'dial_secs': 16.0,
        },
        {
            'call_start_time': DatetimeWithNanoseconds(2021, 1, 2, 10, 00, 00, tzinfo=datetime.timezone.utc),
            'call_end_time': DatetimeWithNanoseconds(2021, 1, 2, 11, 00, 00, tzinfo=datetime.timezone.utc),
            'dial_secs': 16.0,
        },
        {
            'call_start_time': DatetimeWithNanoseconds(2021, 1, 5, 14, 00, 00, tzinfo=datetime.timezone.utc),
            'call_end_time': DatetimeWithNanoseconds(2021, 1, 5, 16, 00, 00, tzinfo=datetime.timezone.utc),
            'dial_secs': 16.0,
        }
    ]
    return pd.DataFrame(results)


