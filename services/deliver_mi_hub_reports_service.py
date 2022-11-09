import datetime

from functions.csv_functions import write_csv
from functions.zip_functions import create_zip


class DeliverMiHubReportsService:
    @staticmethod
    def upload_mi_hub_reports_to_gcp(questionnaire_name: str, mi_hub_call_history, mi_hub_respondent_data, google_storage) -> str:
        zip_data = []
        if mi_hub_call_history:
            call_history_csv = write_csv(mi_hub_call_history)
            zip_data.append({"filename": "call_history.csv", "content": call_history_csv})
        else:
            print(f"No call history for {questionnaire_name}")

        if mi_hub_respondent_data:
            respondent_data_csv = write_csv(mi_hub_respondent_data)
            zip_data.append(
                {"filename": "respondent_data.csv", "content": respondent_data_csv}
            )
        else:
            print(f"No respondent data for {questionnaire_name}")

        if zip_data:
            zipped_data = create_zip(zip_data)
            datetime_string = datetime.datetime.now().strftime("%d%m%Y_%H%M%S")
            mi_filename = f"mi_{questionnaire_name}_{datetime_string}"
            google_storage.upload_zip(f"{mi_filename}.zip", zipped_data)
        else:
            print(f"No data for {questionnaire_name}")

        return f"Done - {questionnaire_name}"
