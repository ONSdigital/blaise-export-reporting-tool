import asyncio
from dataclasses import asdict
from datetime import datetime
from dateutil.relativedelta import relativedelta

from google.cloud import datastore

from data_sources.cati_data import get_cati_call_history_from_database
from data_sources.questionnaire_data import get_questionnaire_name
from models.call_history_model import CallHistory


class CallHistoryClient:
    def __init__(self, datastore_client, config=None):
        self.datastore_client = datastore_client
        self.config = config

    def delete_historical_call_history(self):
        # Uncomment this to generate test data to confirm deletion is working
        # self.__generate_year_old_test_data()
        old_call_history_keys = self.__get_keys_for_historical_call_history_records()
        if len(old_call_history_keys) == 0:
            print("No call history records older than a year found")
            return
        self.__delete_datastore_records(old_call_history_keys)
        self.__get_keys_for_historical_call_history_records()

    def call_history_extraction_process(self):
        call_history = self.__extract_call_history()
        self.__upload_call_history_to_datastore(call_history)

    def get_call_history_report_status(self):
        key = self.datastore_client.key("Status", "call_history")
        status = self.datastore_client.get(key)
        return status

    def get_cati_call_history(self):
        print(f"Obtaining the call history records in the CATI database")
        results = get_cati_call_history_from_database(self.config)
        print(f"Obtained {len(results)} call history records in the CATI database")
        cati_call_history_list = []
        for item in results:
            cati_call_history = CallHistory(
                questionnaire_name=item.get("InstrumentName"),
                serial_number=item.get("PrimaryKeyValue"),
                call_number=item.get("CallNumber"),
                dial_number=item.get("DialNumber"),
                busy_dials=item.get("BusyDials"),
                call_start_time=item.get("StartTime"),
                call_end_time=item.get("EndTime"),
                dial_secs=item.get("DialSecs"),
                status=item.get("Status"),
                interviewer=item.get("Interviewer"),
                call_result=item.get("DialResult"),
                update_info=item.get("UpdateInfo"),
                appointment_info=item.get("AppointmentInfo"),
                outcome_code=item.get("OutcomeCode"),
            )
            if cati_call_history.questionnaire_name != "":
                cati_call_history.generate_questionnaire_details()
            cati_call_history_list.append(cati_call_history)
        print(
            f"Created {len(cati_call_history_list)} call history models for processing"
        )
        return cati_call_history_list

    @staticmethod
    def split_into_batches(list_to_split, batch_length):
        return [
            list_to_split[i : i + batch_length]
            for i in range(0, len(list_to_split), batch_length)
        ]

    def __generate_year_old_test_data(self):
        i = 1
        while i < 601:
            task = datastore.Entity(self.datastore_client.key("CallHistory"))
            task.update(
                {
                    "call_start_time": datetime.now() - relativedelta(years=1, days=1),
                    "number": i,
                }
            )
            self.datastore_client.put(task)
            i += 1

    def __delete_datastore_records(self, datastore_record_keys):
        batches = self.split_into_batches(datastore_record_keys, 500)
        try:
            for batch in batches:
                print(
                    f"Attempting to delete batch of {len(batch)} items (Total batches {len(batches)})"
                )
                self.datastore_client.delete_multi(batch)
        except Exception as err:
            print(f"Failed to delete records in datastore: {err}")

    def __get_keys_for_historical_call_history_records(self):
        a_year_ago = datetime.now() - relativedelta(years=1)
        query = self.datastore_client.query(
            kind="CallHistory", filters=[("call_start_time", "<=", a_year_ago)]
        )
        query.keys_only()
        old_call_history_keys = list([entity.key for entity in query.fetch()])
        print(
            f"Found {len(old_call_history_keys)} records with a call_start_time older than one year ({a_year_ago})"
        )
        return old_call_history_keys

    def __extract_call_history(self):
        print("Getting call history data")
        return self.get_cati_call_history()

    def __upload_call_history_to_datastore(self, call_history_data):
        print("Checking for new call history records to upload to datastore")
        new_call_history_records = self.filter_out_existing_call_history_records(
            call_history_data
        )
        if len(new_call_history_records) == 0:
            print("No new call history records to upload to datastore")
        else:
            self.__bulk_upload_call_history(new_call_history_records)
            print(
                f"Uploaded {len(new_call_history_records)} new call history records to datastore"
            )
        self.__update_call_history_report_status()

    def filter_out_existing_call_history_records(self, call_history_data):
        current_call_history_in_datastore = self.get_call_history_keys()

        return list(
            filter(
                lambda call_history_record: not self.__check_if_call_history_record_already_exists(
                    call_history_record, current_call_history_in_datastore
                ),
                call_history_data,
            )
        )

    def get_call_history_keys(self):
        query = self.datastore_client.query(kind="CallHistory")
        query.keys_only()
        current_call_history = {entity.key.id_or_name: None for entity in query.fetch()}
        return current_call_history

    def __bulk_upload_call_history(self, new_call_history_entries):
        client = datastore.Client()
        datastore_tasks = []
        for call_history_record in new_call_history_entries:
            task1 = datastore.Entity(
                client.key(
                    "CallHistory",
                    f"{call_history_record.call_number}-{call_history_record.dial_number}-{call_history_record.busy_dials}-{call_history_record.questionnaire_name}-{call_history_record.serial_number}-{call_history_record.call_start_time}",
                )
            )
            task1.update(asdict(call_history_record))
            datastore_tasks.append(task1)
        datastore_batches = self.split_into_batches(datastore_tasks, 500)

        for batch in datastore_batches:
            client.put_multi(batch)

    def __update_call_history_report_status(self):
        complete_key = self.datastore_client.key("Status", "call_history")
        task = datastore.Entity(key=complete_key)
        task.update(
            {
                "last_updated": datetime.utcnow(),
            }
        )
        self.datastore_client.put(task)
        return

    @staticmethod
    def __check_if_call_history_record_already_exists(
        call_history_record, current_call_history_in_datastore
    ):
        return (
            f"{call_history_record.call_number}-{call_history_record.dial_number}-{call_history_record.busy_dials}-{call_history_record.questionnaire_name}-{call_history_record.serial_number}-{call_history_record.call_start_time}"
            in current_call_history_in_datastore
        )
