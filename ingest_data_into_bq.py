from google.cloud import bigquery
from google.oauth2 import service_account
import os
from google.cloud import storage
import pandas as pd


credentials = service_account.Credentials.from_service_account_file('C:\\Users\\Akanksha\\Downloads\\erp_pipeline_key.json')

project_id = 'learning-gcp-424417'
client = bigquery.Client(credentials= credentials,project = project_id)


query_job = client.query("""
   SELECT *
   FROM learning-gcp-424417.mayank_erp.categories
    """)

# results = query_job.result() # Wait for the job to complete.

##########################################################
##########################################################

def get_gcs_files(bucket_name, prefix, credentials):
    """Lists all the files in the GCS bucket with the given prefix."""
    storage_client = storage.Client(credentials=credentials)
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    return [blob.name for blob in blobs if not blob.name.endswith('/')]

def infer_schema_from_gcs_file(bucket_name, file_name, credentials):
    """Infers the schema of a CSV file stored in GCS."""
    storage_client = storage.Client(credentials=credentials)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    # Download the CSV file as a pandas DataFrame
    data = pd.read_csv(blob.open("rt"))
    # Infer the schema
    schema = []
    for column_name, dtype in zip(data.columns, data.dtypes):
        if pd.api.types.is_integer_dtype(dtype):
            field_type = "INTEGER"
        elif pd.api.types.is_float_dtype(dtype):
            field_type = "FLOAT"
        elif pd.api.types.is_bool_dtype(dtype):
            field_type = "BOOLEAN"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            field_type = "TIMESTAMP"
        else:
            field_type = "STRING"
        schema.append(bigquery.SchemaField(column_name, field_type))
    return schema

def create_table_with_schema(dataset_id, table_id, schema, credentials):
    """Creates a BigQuery table with the given schema."""
    bigquery_client = bigquery.Client(credentials=credentials)
    dataset_ref = bigquery_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    table = bigquery.Table(table_ref)
    table = bigquery_client.create_table(table)
    print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")

def load_data_from_gcs_to_bq(bucket_name, file_name, dataset_id, table_id, credentials):
    """Loads data from GCS file to BigQuery table."""
    bigquery_client = bigquery.Client(credentials=credentials)
    dataset_ref = bigquery_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1, autodetect=True
    )
    uri = f"gs://{bucket_name}/{file_name}"
    load_job = bigquery_client.load_table_from_uri(uri, table_ref, job_config=job_config)
    load_job.result()  # Wait for the job to complete
    print(f"Loaded data into {table_id} from {file_name}")

def main(bucket_name, dataset_id, prefix):
    # Get list of files from the GCS bucket
    files = get_gcs_files(bucket_name, prefix, credentials)
    for file_name in files:
        table_id = os.path.splitext(os.path.basename(file_name))[0]
        schema = infer_schema_from_gcs_file(bucket_name, file_name, credentials)
        create_table_with_schema(dataset_id, table_id, schema, credentials)
        load_data_from_gcs_to_bq(bucket_name, file_name, dataset_id, table_id, credentials)

if __name__ == "__main__":
    bucket_name = 'manu13891_erp'
    dataset_id = 'mayank_erp'
    prefix = 'erp/'
    main(bucket_name, dataset_id, prefix)



