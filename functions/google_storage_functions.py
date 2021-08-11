from google.cloud import storage

# workaround to prevent file transfer timeouts
storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB


class GoogleStorage:
    def __init__(self, nifi_staging_bucket, log):
        self.nifi_staging_bucket = nifi_staging_bucket
        self.log = log
        self.bucket = None
        self.storage_client = None

    def initialise_bucket_connection(self):
        try:
            print(f"Connecting to bucket - {self.nifi_staging_bucket}")
            storage_client = storage.Client()
            self.storage_client = storage_client
            self.bucket = storage_client.get_bucket(self.nifi_staging_bucket)
            print(f"Connected to bucket - {self.nifi_staging_bucket}")
        except Exception as ex:
            print("Connection to bucket failed - %s", ex)

    def upload_file(self, source, dest):
        blob_destination = self.bucket.blob(dest)
        print(f"Uploading file to storage bucket - {source}")
        blob_destination.upload_from_filename(source)
        print(f"Uploaded file to storage bucket - {source}")

    def upload_zip_mem(self, dest, data):
        blob_destination = self.bucket.blob(dest)
        print(f"Uploading file to storage bucket - {dest}")
        blob_destination.upload_from_string(data, content_type="application/zip")
        print(f"Uploaded file to storage bucket - {dest}")


def init_google_storage(config):
    google_storage = GoogleStorage(config.nifi_staging_bucket, None)
    google_storage.initialise_bucket_connection()
    return google_storage
