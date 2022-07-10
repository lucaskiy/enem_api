from google.cloud import storage
import google.auth


class GcloudObject:
    def __init__(self):
        self.client = self._config()

    @staticmethod
    def _config():
        credentials, project = google.auth.default()
        client = storage.Client()
        return client

    def upload_to_bucket(self, blob_name: str, path_to_file: str, bucket_name: str) -> None:
        bucket = self.client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(path_to_file)

    