import os
from unittest import mock

from models.config_model import Config


def test_config():
    config = Config(
        mysql_host="mysql_host_mock",
        mysql_user="mysql_user_mock",
        mysql_password="mysql_password_mock",
        mysql_database="mysql_database_mock",
        blaise_api_url="blaise_api_url_mock",
        nifi_staging_bucket="nifi_staging_bucket_mock"
    )
    assert config.mysql_host == "mysql_host_mock"
    assert config.mysql_user == "mysql_user_mock"
    assert config.mysql_password == "mysql_password_mock"
    assert config.mysql_database == "mysql_database_mock"
    assert config.blaise_api_url == "blaise_api_url_mock"
    assert config.nifi_staging_bucket == "nifi_staging_bucket_mock"


@mock.patch.dict(
    os.environ,
    {
        "MYSQL_HOST": "mysql_host_mock",
        "MYSQL_USER": "mysql_user_mock",
        "MYSQL_PASSWORD": "mysql_password_mock",
        "MYSQL_DATABASE": "mysql_database_mock",
        "BLAISE_API_URL": "blaise_api_url_mock",
        "NIFI_STAGING_BUCKET": "nifi_staging_bucket_mock"
    },
)
def test_config_from_env():
    config = Config.from_env()
    assert config.mysql_host == "mysql_host_mock"
    assert config.mysql_user == "mysql_user_mock"
    assert config.mysql_password == "mysql_password_mock"
    assert config.mysql_database == "mysql_database_mock"
    assert config.blaise_api_url == "blaise_api_url_mock"
    assert config.nifi_staging_bucket == "nifi_staging_bucket_mock"