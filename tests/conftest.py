import pytest
from app.app import app as flask_app
from models.config import Config


@pytest.fixture
def app():
    yield flask_app


@pytest.fixture
def client(app):
    return app.test_client()


@pytest.fixture
def config():
    return Config(
        mysql_host="just-a-simple-host",
        mysql_user="test",
        mysql_password="unique-password",
        mysql_database="DB_NAME",
        blaise_api_url="a-legit-url",
    )

