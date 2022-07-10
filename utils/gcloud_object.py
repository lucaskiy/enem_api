from google.cloud import storage
import os


class GcloudObject:
    def __init__(self):
        self.client = self._config()

    @staticmethod
    def _config():
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/lucas/Downloads/dataengproject-355818-a7335e30d3bb.json"
        client = storage.Client()
        return client

    def upload_to_bucket(self, blob_name: str, path_to_file: str, bucket_name: str) -> None:
        bucket = self.client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(path_to_file)

    def get_files_inside_bucket(self, bucket_name: str) -> list:
        bucket = self.client.get_bucket(bucket_name)
        blobs = bucket.list_blobs()
        results = list()

        for blob in blobs:
            results.append(blob.name)

        return results

if __name__ == '__main__':
    x = GcloudObject().get_files_inside_bucket(bucket_name="files-etl")
    