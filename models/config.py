import os

from dataclasses import dataclass


@dataclass
class Config:
    mysql_host: str
    mysql_user: str
    mysql_password: str
    mysql_database: str
    blaise_api_url: str
    bucket_name: str

    @classmethod
    def from_env(cls):
        return cls(
            mysql_host=os.getenv("MYSQL_HOST"),
            mysql_user=os.getenv("MYSQL_USER"),
            mysql_password=os.getenv("MYSQL_PASSWORD"),
            mysql_database=os.getenv("MYSQL_DATABASE"),
            blaise_api_url=os.getenv("BLAISE_API_URL"),
            bucket_name=os.getenv("BUCKET_NAME")
        )

    def log(self):
        print(f"Configuration: mysql_host: {self.mysql_host}")
        print(f"Configuration: mysql_user: {self.mysql_user}")
        if self.mysql_password is None:
            print(f"Configuration: mysql_password: None")
        else:
            print(f"Configuration: mysql_password: Provided")
        print(f"Configuration: mysql_database: {self.mysql_database}")
        print(f"Configuration: blaise_api_url: {self.blaise_api_url}")
        print(f"Configuration: bucket_name: {self.bucket_name}")
