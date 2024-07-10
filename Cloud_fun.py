import base64
import functions_framework
from google.cloud import bigquery
import json
# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def hello_pubsub(cloud_event):
   # Print out the data from Pub/Sub, to prove that it worked
   print(base64.b64decode(cloud_event.data["message"]["data"]))
 
# Initialize BigQuery client
client = bigquery.Client()
def insert_json_data(json_data, dataset_id, table_id):
      schema = [
       bigquery.SchemaField("name", "STRING"),
       bigquery.SchemaField("age", "INTEGER"),
       bigquery.SchemaField("gender", "STRING"),
       bigquery.SchemaField("bloodtype", "STRING")
       bigquery.SchemaField("doctor", "STRING")
       bigquery.SchemaField("dateadmission", "DATE")
       bigquery.SchemaField("disease", "STRING")
       bigquery.SchemaField("hospital", "STRING")
       bigquery.SchemaField("insurancename", "STRING")
       bigquery.SchemaField("discharge", "DATE")
       bigquery.SchemaField("medication", "STRING")
       bigquery.SchemaField("testresult", "STRING")
   ]
   job_config = bigquery.LoadJobConfig(
       schema=schema,
       source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
       write_disposition=bigquery.WriteDisposition.WRITE_APPEND
   )
   json_lines = '\n'.join([json.dumps(record) for record in json_data])
   table_ref = client.dataset(dataset_id).table(table_id)
   load_job = client.load_table_from_file(
       file_obj=json_lines,
       destination=table_ref,
       job_config=job_config
   )
   load_job.result()
   print(f'Data successfully inserted into {dataset_id}.{table_id}')
   # Dataset and table IDs
   dataset_id = 'gentle-analyst-428822-n1.Health_data'
   table_id = 'gentle-analyst-428822-n1.Health_data.Patient_data'
   insert_json_data(json_data, dataset_id, table_id)
