from google.cloud import bigquery
import os
from utils.logger_print import print_log

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/lucas/Downloads/dataengproject-355818-a7335e30d3bb.json"
# Construct a BigQuery client object.
client = bigquery.Client()
print(print_log(client))