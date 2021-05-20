import os
from dataclasses import dataclass


@dataclass
class Config:
    mysql_host: str
    mysql_user: str
    mysql_password: str
    mysql_database: str
    blaise_api_url: str

    @classmethod
    def from_env(cls):
        return cls(
            mysql_host=os.getenv("MYSQL_HOST"),
            mysql_user=os.getenv("MYSQL_USER"),
            mysql_password=os.getenv("MYSQL_PASSWORD"),
            mysql_database=os.getenv("MYSQL_DATABASE"),
            blaise_api_url=os.getenv("BLAISE_API_URL"),
        )

    def log(self):
        print(f"Configuration: mysql_host: {self.mysql_host}")
        print(f"Configuration: mysql_user: {self.mysql_user}")
        print(f"Configuration: mysql_password: {self.mysql_password}")
        print(f"Configuration: mysql_database: {self.mysql_database}")
