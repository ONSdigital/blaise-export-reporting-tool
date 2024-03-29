import os
from dataclasses import dataclass


@dataclass
class Config:
    mysql_host: str
    mysql_user: str
    mysql_password: str
    mysql_database: str
    blaise_api_url: str
    nifi_staging_bucket: str
    deliver_mi_hub_reports_task_queue_id: str
    gcloud_project: str
    region: str
    cloud_function_sa: str

    @classmethod
    def from_env(cls):
        return cls(
            mysql_host=os.getenv("MYSQL_HOST"),
            mysql_user=os.getenv("MYSQL_USER"),
            mysql_password=os.getenv("MYSQL_PASSWORD"),
            mysql_database=os.getenv("MYSQL_DATABASE"),
            blaise_api_url=os.getenv("BLAISE_API_URL"),
            nifi_staging_bucket=os.getenv("NIFI_STAGING_BUCKET"),
            deliver_mi_hub_reports_task_queue_id=os.getenv(
                "DELIVER_MI_HUB_REPORTS_TASK_QUEUE_ID"
            ),
            gcloud_project=os.getenv("GCLOUD_PROJECT"),
            region=os.getenv("REGION"),
            cloud_function_sa=os.getenv("CLOUD_FUNCTION_SA"),
        )

    def log(self):
        print(f"Configuration - mysql_host: {self.mysql_host}")
        print(f"Configuration - mysql_user: {self.mysql_user}")
        if self.mysql_password is None:
            print("Configuration - mysql_password: None")
        else:
            print("Configuration - mysql_password: Provided")
        print(f"Configuration - mysql_database: {self.mysql_database}")
        print(f"Configuration - blaise_api_url: {self.blaise_api_url}")
        print(f"Configuration - nifi_staging_bucket: {self.nifi_staging_bucket}")
        print(
            f"Configuration - deliver_mi_hub_reports_task_queue_id: {self.deliver_mi_hub_reports_task_queue_id}"
        )
        print(f"Configuration - gcloud_project: {self.gcloud_project}")
        print(f"Configuration - region: {self.region}")
        print(f"Configuration - cloud_function_sa: {self.cloud_function_sa}")
