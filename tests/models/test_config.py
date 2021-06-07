import os
from unittest import mock

from models.config import Config


def test_config(config):
    assert config.mysql_host == "just-a-simple-host"
    assert config.mysql_user == "test"
    assert config.mysql_password == "unique-password"
    assert config.mysql_database == "DB_NAME"
    assert config.blaise_api_url == "a-legit-url"


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
def test_config_from_env():
    config = Config.from_env()
    assert config.mysql_host == "just-a-simple-host"
    assert config.mysql_user == "test"
    assert config.mysql_password == "unique-password"
    assert config.mysql_database == "DB_NAME"
    assert config.blaise_api_url == "a-legit-url"
