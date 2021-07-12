from google.cloud import storage

# workaround to prevent file transfer timeouts
storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB


class GoogleStorage:
    def __init__(self, bucket_name, log):
        self.bucket_name = bucket_name
        self.log = log
        self.bucket = None
        self.storage_client = None

    def initialise_bucket_connection(self):
        try:
            print(f"Connecting to bucket - {self.bucket_name}")
            storage_client = storage.Client()
            self.storage_client = storage_client
            self.bucket = storage_client.get_bucket(self.bucket_name)
            print(f"Connected to bucket - {self.bucket_name}")
        except Exception as ex:
            print("Connection to bucket failed - %s", ex)

    def upload_file(self, source, dest):
        blob_destination = self.bucket.blob(dest)
        print(f"Uploading file - {source}")
        blob_destination.upload_from_filename(source)
        print(f"Uploaded file - {source}")


def init_google_storage(config):
    google_storage = GoogleStorage(config.bucket_name, None)
    google_storage.initialise_bucket_connection()
    return google_storage
